defmodule Autoangel.Alert.PluginRunnerRetryPropertyTest do
  @moduledoc """
  Property 30 (Requirements 25.2, 25.3, 25.4).

  "For any sequence of attempt outcomes (success, retryable, timeout, permanent)
  and any max-attempts setting, the PluginRunner makes at most max attempts, each
  wait is strictly longer than the previous one (jitter included), no attempt
  follows a permanent error or a success, and a failure record naming the Plugin,
  the Anomaly and the final error class is produced exactly when delivery did not
  succeed."

  Strategy
  --------
  The real `Autoangel.Alert.PluginRunner` is driven with a SCRIPTED plugin whose
  `deliver/2` pops the NEXT outcome from a controller Agent and forwards
  `{:attempt, n, outcome, mono}` to the test process BEFORE returning that
  outcome. Because the observer fires on every delivery call, the test captures
  EXACTLY how many attempts the runner made and in what order — including each
  retry — independently of the outcome scripting.

  An independent reference model consumes the SAME script under the same
  `max_attempts` to predict the observable attempt sequence: attempts proceed in
  order and STOP at the first `:ok` or `:permanent`, or after `max_attempts`
  attempts, whichever comes first (a script shorter than `max_attempts` defaults
  to `:retryable`, matching the plugin). From that prediction the model derives
  whether delivery ultimately succeeded and therefore whether a failure record
  must exist, plus the success / retryable / permanent counter deltas.

  The strictly-increasing-wait guarantee (25.3) cannot be asserted precisely
  inside the property because the backoff carries jitter and the real delays
  (~1s, ~2s, ...) make large `max_runs` slow. It is covered by a SEPARATE
  deterministic example that captures monotonic timestamps per attempt for an
  all-retryable `max_attempts: 3` run and asserts the inter-attempt gaps fall in
  the non-overlapping bands [800, 1400] ms and [1600, 2800] ms — the plugin's
  `backoff/1` applies ±20% jitter around 1000 ms and 2000 ms, so gap2 > gap1
  always holds.
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Autoangel.Alert.PluginRunner

  # Each retryable attempt incurs a real backoff (~1s, ~2s, ...). Keeping
  # generated max_attempts in 1..3 bounds the wall-clock cost per run to ~3s,
  # so max_runs is kept modest and assert_receive/settle timeouts are generous.
  @settle_timeout 6_000

  # A scripted plugin: every `deliver/2` pops the next outcome from the
  # controller Agent, stamps a monotonic timestamp, and forwards
  # {:attempt, n, outcome, mono} to the parent BEFORE returning the outcome.
  # When the script is exhausted it defaults to {:retryable, :default}; the
  # runner's max_attempts bounds how many times that default can be hit.
  #
  # A `{:sleep, ms, outcome}` entry models a slow attempt: the observer message
  # is sent FIRST (so capture still works), then the call blocks for `ms`. When
  # `ms` exceeds the runner's configured `timeout:` the runner kills the task and
  # records a :timeout — the `outcome` the sleep would eventually return is
  # therefore never observed by the runner.
  defmodule ScriptedPlugin do
    @behaviour Autoangel.Plugin
    @impl true
    def validate_config(_), do: :ok

    @impl true
    def deliver(_alert, config) do
      parent = config[:parent]
      controller = config[:controller]

      {n, outcome} =
        Agent.get_and_update(controller, fn {count, script} ->
          next = count + 1

          {outcome, rest} =
            case script do
              [head | tail] -> {head, tail}
              [] -> {{:retryable, :default}, []}
            end

          {{next, outcome}, {next, rest}}
        end)

      case outcome do
        {:sleep, ms, eventual} ->
          send(parent, {:attempt, n, {:sleep, ms}, System.monotonic_time(:millisecond)})
          Process.sleep(ms)
          eventual

        _ ->
          send(parent, {:attempt, n, outcome, System.monotonic_time(:millisecond)})
          outcome
      end
    end
  end

  # ----------------------------------------------------------------------------
  # Reference model
  #
  # Walks `script` under `max_attempts` to predict the observable attempt
  # sequence and the delivery outcome. Attempts proceed in order and STOP at the
  # first terminal outcome (:ok or {:permanent, _}); otherwise retryable outcomes
  # continue until `max_attempts` attempts have been made. A script shorter than
  # `max_attempts` defaults to {:retryable, :default}, matching the plugin.
  #
  # Returns:
  #   * attempts        — the predicted list of per-attempt outcomes, in order
  #   * succeeded?       — true iff the final attempt returned :ok
  #   * final_class     — :success | :permanent | :retryable (class of the last
  #                       attempt; drives the expected failure record)
  #   * success_count   — number of :ok outcomes (0 or 1)
  #   * retryable_count — number of retryable outcomes observed
  #   * permanent_count — number of permanent outcomes (0 or 1)
  # ----------------------------------------------------------------------------
  defmodule Model do
    @moduledoc false

    def run(script, max_attempts) do
      outcomes = predicted_outcomes(script, max_attempts)
      last = List.last(outcomes)

      %{
        attempts: outcomes,
        succeeded?: last == :ok,
        final_class: class(last),
        success_count: Enum.count(outcomes, &(&1 == :ok)),
        retryable_count: Enum.count(outcomes, &retryable?/1),
        permanent_count: Enum.count(outcomes, &permanent?/1)
      }
    end

    # Produces the outcomes for attempts 1..max_attempts, stopping early at the
    # first terminal outcome. `script` is padded with the retryable default.
    defp predicted_outcomes(script, max_attempts) do
      script
      |> pad(max_attempts)
      |> Enum.take(max_attempts)
      |> take_until_terminal()
    end

    defp pad(script, max_attempts) do
      script ++ List.duplicate({:retryable, :default}, max_attempts)
    end

    defp take_until_terminal(outcomes), do: take_until_terminal(outcomes, [])

    defp take_until_terminal([], acc), do: Enum.reverse(acc)

    defp take_until_terminal([outcome | rest], acc) do
      if terminal?(outcome),
        do: Enum.reverse([outcome | acc]),
        else: take_until_terminal(rest, [outcome | acc])
    end

    defp terminal?(:ok), do: true
    defp terminal?({:permanent, _}), do: true
    defp terminal?(_), do: false

    defp retryable?({:retryable, _}), do: true
    defp retryable?(_), do: false

    defp permanent?({:permanent, _}), do: true
    defp permanent?(_), do: false

    defp class(:ok), do: :success
    defp class({:permanent, _}), do: :permanent
    defp class({:retryable, _}), do: :retryable
  end

  # ----------------------------------------------------------------------------
  # Generators
  # ----------------------------------------------------------------------------

  # A scripted non-timeout outcome. Retryable/permanent carry an opaque atom so
  # the runner's reason-threading paths are exercised.
  defp outcome_gen do
    one_of([
      constant(:ok),
      constant({:retryable, :temporary}),
      constant({:permanent, :rejected})
    ])
  end

  # ----------------------------------------------------------------------------
  # Property
  # ----------------------------------------------------------------------------

  # Worst case ~25 runs × up to 3 real retry backoffs (~3s each) can approach
  # ExUnit's 60s default timeout, so this property is given a generous bound.
  @tag timeout: 120_000
  property "runner makes at most max attempts, stops at the first terminal, and records failures exactly on non-success" do
    check all(
            max_attempts <- integer(1..3),
            script <- list_of(outcome_gen(), max_length: 3),
            max_runs: 25
          ) do
      model = Model.run(script, max_attempts)

      anomaly_id = "a-#{System.unique_integer([:positive])}"
      alert = %{anomaly_id: anomaly_id}

      {:ok, task_supervisor} = Task.Supervisor.start_link()
      {:ok, controller} = Agent.start_link(fn -> {0, script} end)

      {:ok, runner} =
        PluginRunner.start_link(
          plugin: ScriptedPlugin,
          config: %{parent: self(), controller: controller},
          task_supervisor: task_supervisor,
          # A large per-attempt timeout: the scripted plugin never blocks, so no
          # attempt times out; the only delays are the real retry backoffs.
          timeout: 1_000,
          max_attempts: max_attempts,
          # A high breaker threshold keeps the circuit closed for a single alert.
          breaker_threshold: 100
        )

      try do
        assert :ok = PluginRunner.enqueue(runner, alert)

        expected_attempts = length(model.attempts)

        # Capture exactly the attempts the model predicts, in order.
        observed = collect_attempts(expected_attempts)

        # Wait until the runner has settled (no attempt in flight, no timer).
        settle(runner)

        metrics = PluginRunner.metrics(runner)
        failures = for f <- PluginRunner.failures(runner), f.anomaly == anomaly_id, do: f

        # 1. AT MOST max_attempts attempts were made.
        assert length(observed) <= max_attempts

        # 2. The observed attempt sequence matches the model: it stops at the
        #    first :ok or permanent, so no attempt follows a success or a
        #    permanent, and otherwise runs the full max_attempts of retryables.
        assert Enum.map(observed, fn {_n, outcome} -> outcome end) == model.attempts
        assert observed_indices_are_sequential(observed)

        # 3. A failure record is produced EXACTLY when delivery did not succeed.
        if model.succeeded? do
          assert failures == []
          assert metrics.success == model.success_count
        else
          assert [record] = failures
          assert record.plugin == ScriptedPlugin
          assert record.anomaly == anomaly_id
          assert record.error_class == model.final_class
          assert metrics.success == 0
        end

        # Counters match the independent model.
        assert metrics.success == model.success_count
        assert metrics.retryable == model.retryable_count
        assert metrics.permanent == model.permanent_count
      after
        GenServer.stop(runner)
        Agent.stop(controller)
        Supervisor.stop(task_supervisor)
      end
    end
  end

  # ----------------------------------------------------------------------------
  # Deterministic example: strictly increasing jittered backoff (25.3)
  # ----------------------------------------------------------------------------

  test "each retry wait is strictly longer than the previous one (jitter included)" do
    # Three retryables force two real backoff waits: ~1000 ms then ~2000 ms.
    script = [{:retryable, :a}, {:retryable, :b}, {:retryable, :c}]
    anomaly_id = "backoff-#{System.unique_integer([:positive])}"

    {:ok, task_supervisor} = Task.Supervisor.start_link()
    {:ok, controller} = Agent.start_link(fn -> {0, script} end)

    {:ok, runner} =
      PluginRunner.start_link(
        plugin: ScriptedPlugin,
        config: %{parent: self(), controller: controller},
        task_supervisor: task_supervisor,
        timeout: 1_000,
        max_attempts: 3,
        breaker_threshold: 100
      )

    try do
      assert :ok = PluginRunner.enqueue(runner, %{anomaly_id: anomaly_id})

      assert_receive {:attempt, 1, {:retryable, :a}, t1}, @settle_timeout
      assert_receive {:attempt, 2, {:retryable, :b}, t2}, @settle_timeout
      assert_receive {:attempt, 3, {:retryable, :c}, t3}, @settle_timeout

      gap1 = t2 - t1
      gap2 = t3 - t2

      # backoff(1) ≈ 1000 ms ±20% → up to 1200 ms of real delay; backoff(2) ≈
      # 2000 ms ±20% → up to 2400 ms. send_after never fires EARLY, so the lower
      # bounds (800 / 1600) are safe. The upper bounds are widened to absorb
      # scheduling and message-handling overhead on top of the maximum jittered
      # delay. The bands [800,1400] and [1600,2800] still do not overlap, so the
      # strict-increase guarantee (gap2 > gap1, ~400 ms real margin) continues to
      # hold and remains the actual "strictly increasing" check.
      assert gap1 >= 800 and gap1 <= 1_400,
             "first backoff gap #{gap1}ms outside expected [800,1400]"

      assert gap2 >= 1_600 and gap2 <= 2_800,
             "second backoff gap #{gap2}ms outside expected [1600,2800]"

      assert gap2 > gap1, "second wait (#{gap2}ms) must be strictly longer than first (#{gap1}ms)"
    after
      GenServer.stop(runner)
      Agent.stop(controller)
      Supervisor.stop(task_supervisor)
    end
  end

  # ----------------------------------------------------------------------------
  # Deterministic examples: timeout outcome (P30 — 25.2, 25.4)
  #
  # P30 lists `timeout` as an attempt outcome. A scripted slow delivery (sleeps
  # longer than the runner's short `timeout:`) exercises the runner's timeout
  # path: the in-flight task is killed, the attempt is counted in
  # `metrics.timeout`, and — when it is the terminal attempt — the failure
  # record carries `error_class: :timeout`. Timeouts are kept SHORT so the
  # examples stay fast.
  # ----------------------------------------------------------------------------

  test "a timed-out attempt is retried and a later success still succeeds, counting the timeout" do
    # Attempt 1 sleeps 200 ms > timeout 50 ms → times out (retryable). Attempt 2
    # succeeds. max_attempts: 2, so the retry after the timeout is allowed.
    script = [{:sleep, 200, :ok}, :ok]
    anomaly_id = "timeout-then-ok-#{System.unique_integer([:positive])}"

    {:ok, task_supervisor} = Task.Supervisor.start_link()
    {:ok, controller} = Agent.start_link(fn -> {0, script} end)

    {:ok, runner} =
      PluginRunner.start_link(
        plugin: ScriptedPlugin,
        config: %{parent: self(), controller: controller},
        task_supervisor: task_supervisor,
        timeout: 50,
        max_attempts: 2,
        breaker_threshold: 100
      )

    try do
      assert :ok = PluginRunner.enqueue(runner, %{anomaly_id: anomaly_id})

      # Attempt 1 begins (then sleeps past the timeout); attempt 2 is the retry.
      assert_receive {:attempt, 1, {:sleep, 200}, _}, @settle_timeout
      assert_receive {:attempt, 2, :ok, _}, @settle_timeout

      settle(runner)

      metrics = PluginRunner.metrics(runner)
      failures = for f <- PluginRunner.failures(runner), f.anomaly == anomaly_id, do: f

      assert metrics.timeout == 1, "the timed-out attempt must be counted once as a timeout"
      assert metrics.success == 1, "the eventual success must be counted"
      assert failures == [], "a run that ultimately succeeds must not record a failure"
    after
      GenServer.stop(runner)
      Agent.stop(controller)
      Supervisor.stop(task_supervisor)
    end
  end

  test "a lone timed-out attempt produces a :timeout failure record" do
    # The only attempt sleeps past the timeout. max_attempts: 1, so there is no
    # retry and the terminal failure class is :timeout.
    script = [{:sleep, 200, :ok}]
    anomaly_id = "timeout-only-#{System.unique_integer([:positive])}"

    {:ok, task_supervisor} = Task.Supervisor.start_link()
    {:ok, controller} = Agent.start_link(fn -> {0, script} end)

    {:ok, runner} =
      PluginRunner.start_link(
        plugin: ScriptedPlugin,
        config: %{parent: self(), controller: controller},
        task_supervisor: task_supervisor,
        timeout: 50,
        max_attempts: 1,
        breaker_threshold: 100
      )

    try do
      assert :ok = PluginRunner.enqueue(runner, %{anomaly_id: anomaly_id})

      assert_receive {:attempt, 1, {:sleep, 200}, _}, @settle_timeout

      settle(runner)

      metrics = PluginRunner.metrics(runner)
      failures = for f <- PluginRunner.failures(runner), f.anomaly == anomaly_id, do: f

      assert metrics.timeout == 1
      assert metrics.success == 0

      assert [record] = failures
      assert record.plugin == ScriptedPlugin
      assert record.anomaly == anomaly_id
      assert record.error_class == :timeout
    after
      GenServer.stop(runner)
      Agent.stop(controller)
      Supervisor.stop(task_supervisor)
    end
  end

  # ----------------------------------------------------------------------------
  # Helpers
  # ----------------------------------------------------------------------------

  # Drains exactly `count` {:attempt, n, outcome, _mono} messages in arrival
  # order, returning {n, outcome} pairs. Counting attempts (not timing) keeps
  # capture deterministic across the real backoff delays.
  defp collect_attempts(0), do: []

  defp collect_attempts(count), do: collect_attempts(count, [])

  defp collect_attempts(0, acc), do: Enum.reverse(acc)

  defp collect_attempts(count, acc) do
    receive do
      {:attempt, n, outcome, _mono} -> collect_attempts(count - 1, [{n, outcome} | acc])
    after
      @settle_timeout ->
        flunk("timed out waiting for #{count} more attempt(s); got #{length(acc)} so far")
    end
  end

  # Attempt numbers are assigned by the controller in call order, so for a single
  # alert the observed numbers must be 1, 2, ... with no gaps or reordering.
  defp observed_indices_are_sequential(observed) do
    observed
    |> Enum.map(fn {n, _outcome} -> n end)
    |> Enum.with_index(1)
    |> Enum.all?(fn {n, expected} -> n == expected end)
  end

  # Spins until the runner has no attempt in flight and no pending retry timer,
  # i.e. the alert has been fully processed (succeeded or recorded as a failure).
  defp settle(runner), do: settle(runner, 300)

  defp settle(_runner, 0), do: flunk("runner did not settle in time")

  defp settle(runner, attempts) do
    state = :sys.get_state(runner)

    if is_nil(state.current) and is_nil(state.timer) do
      :ok
    else
      Process.sleep(20)
      settle(runner, attempts - 1)
    end
  end
end

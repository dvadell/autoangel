defmodule Autoangel.Alert.PluginRunnerBreakerPropertyTest do
  @moduledoc """
  Property and example tests for the PluginRunner circuit breaker.

  Validates Property 31 (Requirements 25.5, 25.6, 25.7):

    * 25.5 — the breaker opens exactly when consecutive failed deliveries
      exceed the threshold (`consecutive > breaker_threshold`).
    * 25.6 — while open and inside the cool-off no `deliver` call happens and
      each skipped alert is recorded (metric `:skipped`, failure
      `error_class: :circuit_open`).
    * 25.7 — the first alert after cool-off is attempted; success closes the
      breaker and resets the consecutive-failure count, while a failure in
      half-open reopens it for a full cool-off.

  The breaker's cool-off uses real `Process.send_after`, so the timing cannot
  be made fully generative. A focused property exercises the opening boundary
  across arbitrary failure counts, and deterministic examples cover the
  half-open success/failure transitions with a small cool-off.
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Autoangel.Alert.PluginRunner

  # A plugin that always reports a permanent failure. With `max_attempts: 1`
  # each enqueued alert becomes exactly one failed delivery.
  defmodule PermanentPlugin do
    @behaviour Autoangel.Plugin
    def validate_config(_), do: :ok

    def deliver(alert, config) do
      if parent = config[:parent], do: send(parent, {:delivered, alert})
      {:permanent, :rejected}
    end
  end

  # A plugin whose per-attempt outcome is scripted via an Agent holding a list
  # of outcomes (`:ok` or `{:permanent, _}` / `{:retryable, _}`). Each delivery
  # pops the next outcome; it also signals the test pid that it was attempted.
  defmodule ScriptedPlugin do
    @behaviour Autoangel.Plugin
    def validate_config(_), do: :ok

    def deliver(alert, config) do
      send(config[:parent], {:delivered, alert})

      Agent.get_and_update(config[:outcomes], fn
        [outcome | rest] -> {outcome, rest}
        [] -> {:ok, []}
      end)
    end
  end

  setup do
    {:ok, task_supervisor} = Task.Supervisor.start_link()
    %{task_supervisor: task_supervisor}
  end

  # ---------------------------------------------------------------------------
  # 25.5 — opens exactly when consecutive failures exceed the threshold
  # ---------------------------------------------------------------------------

  property "breaker opens exactly when consecutive failures exceed the threshold",
           %{task_supervisor: task_supervisor} do
    check all(threshold <- integer(1..4), max_runs: 12) do
      {:ok, runner} =
        PluginRunner.start_link(
          plugin: PermanentPlugin,
          config: %{},
          task_supervisor: task_supervisor,
          timeout: 100,
          max_attempts: 1,
          breaker_threshold: threshold,
          # Large cool-off: once open it must stay open for the duration of the
          # assertions below.
          cooloff: 60_000
        )

      # Each of the first `threshold` failures must leave the breaker closed:
      # `consecutive > threshold` is still false (consecutive == threshold).
      for i <- 1..threshold do
        assert :ok = PluginRunner.enqueue(runner, %{anomaly_id: "fail-#{i}"})

        assert_eventually(
          fn -> PluginRunner.metrics(runner) end,
          fn m -> m.permanent == i end
        )

        assert PluginRunner.breaker(runner) == :closed,
               "breaker opened too early after #{i} of #{threshold} failures"
      end

      # The (threshold + 1)-th failure makes consecutive == threshold + 1,
      # which is strictly greater than the threshold: the breaker opens.
      assert :ok = PluginRunner.enqueue(runner, %{anomaly_id: "fail-open"})

      assert_eventually(
        fn -> PluginRunner.breaker(runner) end,
        fn breaker -> match?({:open, _}, breaker) end
      )
    end
  end

  # ---------------------------------------------------------------------------
  # 25.6 — no deliver while open + inside cool-off; each skip recorded
  # ---------------------------------------------------------------------------

  test "while open and inside cool-off no delivery happens and skips are recorded",
       %{task_supervisor: task_supervisor} do
    {:ok, runner} =
      PluginRunner.start_link(
        plugin: PermanentPlugin,
        config: %{parent: self()},
        task_supervisor: task_supervisor,
        timeout: 100,
        max_attempts: 1,
        breaker_threshold: 2,
        # Large cool-off so the breaker stays open throughout this test.
        cooloff: 60_000
      )

    # Drive three permanent failures to open the breaker (opens on the 3rd,
    # since consecutive 3 > threshold 2).
    for i <- 1..3 do
      id = "open-#{i}"
      assert :ok = PluginRunner.enqueue(runner, %{anomaly_id: id})
      assert_receive {:delivered, %{anomaly_id: ^id}}, 500
    end

    assert_eventually(
      fn -> PluginRunner.breaker(runner) end,
      fn breaker -> match?({:open, _}, breaker) end
    )

    skipped_before = PluginRunner.metrics(runner).skipped

    # Now enqueue more alerts while open: none must be delivered.
    for i <- 1..3 do
      assert :ok = PluginRunner.enqueue(runner, %{anomaly_id: "skip-#{i}"})
    end

    # The breaker is open with a huge cool-off; deliver must never be called for
    # the skipped alerts.
    refute_receive {:delivered, %{anomaly_id: "skip-" <> _}}, 200

    # Each skipped alert increments the :skipped metric by exactly one.
    assert_eventually(
      fn -> PluginRunner.metrics(runner) end,
      fn m -> m.skipped == skipped_before + 3 end
    )

    # And each skip is recorded as a failure with error_class :circuit_open.
    circuit_open =
      runner
      |> PluginRunner.failures()
      |> Enum.filter(&(&1.error_class == :circuit_open))

    assert length(circuit_open) == 3
    assert Enum.all?(circuit_open, &match?(%{error: :circuit_open}, &1))
  end

  # ---------------------------------------------------------------------------
  # 25.7a — first alert after cool-off is attempted; success closes + resets
  # ---------------------------------------------------------------------------

  test "first alert after cool-off is attempted and success closes the breaker",
       %{task_supervisor: task_supervisor} do
    {:ok, outcomes} = start_supervised_agent([])

    {:ok, runner} =
      PluginRunner.start_link(
        plugin: ScriptedPlugin,
        config: %{parent: self(), outcomes: outcomes},
        task_supervisor: task_supervisor,
        timeout: 100,
        max_attempts: 1,
        breaker_threshold: 2,
        # A comfortably observable cool-off: large enough that the bounded poll
        # reliably catches the {:open, _} window before it flips to :half_open,
        # yet still fast.
        cooloff: 300
      )

    # Script: three failures to open, then a success in half-open, then one
    # more failure to prove the consecutive count was reset.
    Agent.update(outcomes, fn _ ->
      [
        {:permanent, :a},
        {:permanent, :b},
        {:permanent, :c},
        :ok,
        {:permanent, :d}
      ]
    end)

    # Open the breaker with three failures.
    for i <- 1..3 do
      id = "open-#{i}"
      assert :ok = PluginRunner.enqueue(runner, %{anomaly_id: id})
      assert_receive {:delivered, %{anomaly_id: ^id}}, 500
    end

    assert_eventually(
      fn -> PluginRunner.breaker(runner) end,
      fn breaker -> match?({:open, _}, breaker) end
    )

    # Wait for the cool-off to elapse: the breaker flips to :half_open.
    assert_eventually(
      fn -> PluginRunner.breaker(runner) end,
      fn breaker -> breaker == :half_open end
    )

    # The first alert after cool-off IS attempted (delivered) and, succeeding,
    # closes the breaker.
    assert :ok = PluginRunner.enqueue(runner, %{anomaly_id: "half-open-success"})
    assert_receive {:delivered, %{anomaly_id: "half-open-success"}}, 500

    assert_eventually(
      fn -> PluginRunner.breaker(runner) end,
      fn breaker -> breaker == :closed end
    )

    # The consecutive-failure count was reset: a single subsequent failure
    # (threshold 2) must NOT reopen the breaker.
    assert :ok = PluginRunner.enqueue(runner, %{anomaly_id: "post-reset-fail"})
    assert_receive {:delivered, %{anomaly_id: "post-reset-fail"}}, 500

    assert_eventually(
      fn -> PluginRunner.metrics(runner) end,
      fn m -> m.permanent == 4 end
    )

    assert PluginRunner.breaker(runner) == :closed,
           "a single failure after a reset must not reopen the breaker"
  end

  # ---------------------------------------------------------------------------
  # 25.7b — a failure in half-open reopens the breaker for a full cool-off
  # ---------------------------------------------------------------------------

  test "a failure in half-open reopens the breaker for a full cool-off",
       %{task_supervisor: task_supervisor} do
    {:ok, outcomes} = start_supervised_agent([])

    {:ok, runner} =
      PluginRunner.start_link(
        plugin: ScriptedPlugin,
        config: %{parent: self(), outcomes: outcomes},
        task_supervisor: task_supervisor,
        timeout: 100,
        max_attempts: 1,
        breaker_threshold: 2,
        # Observable cool-off (see 25.7a): large enough to catch the open window
        # under load, small enough to keep the test fast.
        cooloff: 300
      )

    # Script: three failures to open, then a failure in half-open.
    Agent.update(outcomes, fn _ ->
      [
        {:permanent, :a},
        {:permanent, :b},
        {:permanent, :c},
        {:permanent, :half_open_fail}
      ]
    end)

    for i <- 1..3 do
      id = "open-#{i}"
      assert :ok = PluginRunner.enqueue(runner, %{anomaly_id: id})
      assert_receive {:delivered, %{anomaly_id: ^id}}, 500
    end

    assert_eventually(
      fn -> PluginRunner.breaker(runner) end,
      fn breaker -> match?({:open, _}, breaker) end
    )

    {:open, first_open_ts} =
      assert_eventually(
        fn -> PluginRunner.breaker(runner) end,
        fn breaker -> match?({:open, _}, breaker) end
      )

    # Wait for half-open.
    assert_eventually(
      fn -> PluginRunner.breaker(runner) end,
      fn breaker -> breaker == :half_open end
    )

    # The single attempt in half-open IS made and fails, reopening the breaker
    # for a full cool-off (a fresh {:open, ts} with a later timestamp).
    assert :ok = PluginRunner.enqueue(runner, %{anomaly_id: "half-open-fail"})
    assert_receive {:delivered, %{anomaly_id: "half-open-fail"}}, 500

    {:open, reopen_ts} =
      assert_eventually(
        fn -> PluginRunner.breaker(runner) end,
        fn breaker -> match?({:open, _}, breaker) end
      )

    assert reopen_ts > first_open_ts,
           "reopening must start a fresh cool-off window with a later timestamp"

    skipped_before = PluginRunner.metrics(runner).skipped

    # Prove the breaker is open for a FULL cool-off again, not merely that the
    # timestamp advanced: an alert enqueued immediately after the reopen must be
    # skipped — no delivery attempt, and a :circuit_open failure record.
    assert :ok = PluginRunner.enqueue(runner, %{anomaly_id: "reopened-skip"})
    refute_receive {:delivered, %{anomaly_id: "reopened-skip"}}, 150

    assert_eventually(
      fn -> PluginRunner.metrics(runner) end,
      fn m -> m.skipped == skipped_before + 1 end
    )

    circuit_open =
      runner
      |> PluginRunner.failures()
      |> Enum.filter(&(&1.error_class == :circuit_open and &1.anomaly == "reopened-skip"))

    assert length(circuit_open) == 1,
           "the alert enqueued after the reopen must be recorded as a :circuit_open skip"
  end

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  defp start_supervised_agent(initial) do
    {:ok, pid} = Agent.start_link(fn -> initial end)
    on_exit(fn -> if Process.alive?(pid), do: Agent.stop(pid) end)
    {:ok, pid}
  end

  defp assert_eventually(fun, predicate, attempts \\ 100)
  defp assert_eventually(_fun, _predicate, 0), do: flunk("condition was not met")

  defp assert_eventually(fun, predicate, attempts) do
    value = fun.()

    if predicate.(value) do
      value
    else
      Process.sleep(10)
      assert_eventually(fun, predicate, attempts - 1)
    end
  end
end

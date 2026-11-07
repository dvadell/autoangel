defmodule Autoangel.SelfMonitorEpisodePropertyTest do
  # async: false because each iteration starts the SelfMonitor GenServer and a
  # collector GenServer under the test supervisor and drives them with a shared,
  # mutable clock Agent. There is no process-global state, but keeping the suite
  # serial avoids any supervisor-name contention and keeps the deterministic
  # time driver unambiguous.
  use ExUnit.Case, async: false
  use ExUnitProperties

  alias Autoangel.SelfMonitor

  # **Validates: Requirements 32.1, 32.2, 32.3, 32.4, 32.7**
  # Feature: v1, Property 35: self-monitoring episodes
  #
  # "For any timeline of accepted Samples, storage outcomes, drop events and
  #  per-Plugin delivery outcomes, the Self_Monitor raises an alert when a
  #  condition becomes active (no-ingest duration elapsed, database-unreachable
  #  duration elapsed, Samples dropped, consecutive Plugin failures above the
  #  count), raises no further alert for that condition until it ends, and raises
  #  a new alert for a new occurrence after it ends."
  #
  # Strategy
  # --------
  # The property drives the REAL `Autoangel.SelfMonitor` through a randomly
  # generated timeline of events and compares it against an INDEPENDENT
  # reference model of each condition's active/inactive state. Determinism comes
  # from two devices, so the test never sleeps:
  #
  #   * a clock Agent whose value the test advances explicitly; the monitor reads
  #     it via the injected `clock:` fn. Time-elapsed conditions (`:no_ingest`,
  #     `:database_unreachable`) are only re-evaluated on a `:tick`, so after
  #     advancing the clock the test SENDS `:tick` to force re-evaluation.
  #   * `SelfMonitor.state/1` is a `GenServer.call`, so calling it after each
  #     `send/2` acts as a sync barrier: the prior async message is fully
  #     processed before the state (and the collector) is inspected.
  #
  # Observing raised alerts
  # -----------------------
  # An alert is "raised" when `activate/3` delivers it to a plugin runner via
  # `PluginRunner.enqueue(runner, alert)` (a `GenServer.call(runner, {:enqueue,
  # alert})` expecting `:ok | {:error, :full}`). The test therefore wires a tiny
  # collector GenServer (see `Collector` below) as a runner and counts the
  # alerts it receives, grouped by condition.
  #
  # `SelfMonitor.deliver/3` excludes the FAILING plugin from the recipients of a
  # `{:plugin_failure, plugin}` alert. To observe plugin-failure alerts too, the
  # monitor is configured with TWO self-monitoring plugins:
  #
  #   * "p"       -- the plugin that fails; it has NO runner, and generated
  #                  plugin failures always target it, so it is never itself a
  #                  recipient; and
  #   * "watcher" -- a plugin whose runner IS the collector.
  #
  # With this wiring every raised alert (including plugin-failure alerts, whose
  # failing plugin "p" is excluded) reaches exactly the "watcher" runner, i.e.
  # the collector receives exactly one delivery per raised alert. Non-plugin
  # alerts (nil failing plugin) also reach only "watcher" because "p" has no
  # runner and is filtered out.

  @no_ingest_ms 100
  @database_unreachable_ms 100
  @plugin_failure_count 2
  @failing_plugin "p"
  @watcher_plugin "watcher"

  # ---------------------------------------------------------------------------
  # Collector: a minimal PluginRunner stand-in. It answers {:enqueue, alert}
  # with :ok (the contract PluginRunner.enqueue/2 expects) and records the
  # condition of every alert it receives so the test can count deliveries per
  # condition.
  # ---------------------------------------------------------------------------
  defmodule Collector do
    @moduledoc false
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))

    def counts(server), do: GenServer.call(server, :counts)

    @impl true
    def init(_opts), do: {:ok, %{}}

    @impl true
    def handle_call({:enqueue, alert}, _from, counts) do
      {:reply, :ok, Map.update(counts, alert.condition, 1, &(&1 + 1))}
    end

    def handle_call(:counts, _from, counts), do: {:reply, counts, counts}
  end

  # ---------------------------------------------------------------------------
  # Reference model. `active` is a MapSet of currently-active conditions;
  # `starts` counts, per condition, how many times it transitioned inactive ->
  # active (i.e. how many alerts SHOULD have been delivered); `failures` tracks
  # the running consecutive-failure count for the failing plugin; `last_ingest`
  # and `last_db` are the model's notion of the monitor's reset timestamps.
  # ---------------------------------------------------------------------------
  defp new_model(now) do
    %{
      active: MapSet.new(),
      starts: %{},
      failures: 0,
      last_ingest: now,
      last_db: now
    }
  end

  defp bump_start(model, condition) do
    update_in(model.starts, fn starts -> Map.update(starts, condition, 1, &(&1 + 1)) end)
  end

  # Activate a condition in the model: only counts as a NEW alert when the
  # condition was not already active (mirrors `activate/3`, which is a no-op for
  # an already-alerted condition).
  defp model_activate(model, condition) do
    if MapSet.member?(model.active, condition) do
      model
    else
      model
      |> Map.update!(:active, &MapSet.put(&1, condition))
      |> bump_start(condition)
    end
  end

  # Recover a condition in the model: drop it from the active set (idempotent),
  # mirroring `recover/3`.
  defp model_recover(model, condition) do
    Map.update!(model, :active, &MapSet.delete(&1, condition))
  end

  # Apply one timeline event to the reference model. `now` is the current clock
  # value (already advanced for this step).
  defp model_apply(model, now, :tick) do
    model =
      if now - model.last_ingest >= @no_ingest_ms,
        do: model_activate(model, :no_ingest),
        else: model_recover(model, :no_ingest)

    # database condition is only evaluated when storage is configured, which it
    # always is in this test.
    if now - model.last_db >= @database_unreachable_ms,
      do: model_activate(model, :database_unreachable),
      else: model_recover(model, :database_unreachable)
  end

  defp model_apply(model, now, :accepted_sample) do
    %{model | last_ingest: now} |> model_recover(:no_ingest)
  end

  defp model_apply(model, now, :database_success) do
    %{model | last_db: now} |> model_recover(:database_unreachable)
  end

  defp model_apply(model, _now, {:storage_dropped, _n}) do
    model_activate(model, :storage_overflow)
  end

  defp model_apply(model, _now, {:storage_enqueued, true}) do
    model_recover(model, :storage_overflow)
  end

  defp model_apply(model, _now, {:plugin_failure, _p}) do
    count = model.failures + 1
    model = %{model | failures: count}

    if count > @plugin_failure_count,
      do: model_activate(model, {:plugin_failure, @failing_plugin}),
      else: model
  end

  defp model_apply(model, _now, {:plugin_success, _p}) do
    %{model | failures: 0} |> model_recover({:plugin_failure, @failing_plugin})
  end

  # ---------------------------------------------------------------------------
  # Timeline generation. Each step is a {clock_delta, event} pair. Deltas are
  # chosen from a small set that straddles the thresholds (0, a sub-threshold
  # bump, and an over-threshold bump) so that elapsed conditions both do and do
  # not trigger across a run. Events cover every category in the property.
  # ---------------------------------------------------------------------------
  defp event_generator do
    StreamData.one_of([
      StreamData.constant(:tick),
      StreamData.constant(:accepted_sample),
      StreamData.constant(:database_success),
      StreamData.constant({:storage_dropped, 1}),
      StreamData.member_of([{:storage_dropped, 1}, {:storage_dropped, 3}]),
      StreamData.constant({:storage_enqueued, true}),
      StreamData.constant({:plugin_failure, @failing_plugin}),
      StreamData.constant({:plugin_success, @failing_plugin})
    ])
  end

  defp step_generator do
    StreamData.tuple({
      StreamData.member_of([0, 40, 150]),
      event_generator()
    })
  end

  # Normalize a condition the monitor stores into the shape the model uses as a
  # key. The monitor keys plugin-failure conditions by the canonical plugin
  # name string; the model uses the same.
  defp monitor_condition_set(state) do
    state.conditions |> Map.keys() |> MapSet.new()
  end

  property "each self-monitoring condition raises exactly one alert per episode" do
    check all(
            steps <- StreamData.list_of(step_generator(), min_length: 1, max_length: 40),
            max_runs: 50
          ) do
      {:ok, clock_agent} = Agent.start_link(fn -> 0 end)
      now_fun = fn -> Agent.get(clock_agent, & &1) end

      collector_id = {:collector, make_ref()}
      monitor_id = {:monitor, make_ref()}

      collector = start_supervised!({Collector, name: nil}, id: collector_id)

      monitor =
        start_supervised!(
          {SelfMonitor,
           clock: now_fun,
           no_ingest_ms: @no_ingest_ms,
           database_unreachable_ms: @database_unreachable_ms,
           plugin_failure_count: @plugin_failure_count,
           storage_configured?: true,
           plugins: [@failing_plugin, @watcher_plugin],
           runners: %{@watcher_plugin => collector},
           name: nil},
          id: monitor_id
        )

      model =
        Enum.reduce(steps, new_model(0), fn {delta, event}, model ->
          # Advance the shared clock, then drive the real monitor and the model
          # through the same event.
          if delta > 0, do: Agent.update(clock_agent, &(&1 + delta))
          now = now_fun.()

          send(monitor, event)
          # Sync barrier: a GenServer.call flushes the async send above.
          state = SelfMonitor.state(monitor)

          model = model_apply(model, now, event)

          # Invariant 2 (and the active-set agreement): after every step the
          # monitor's active conditions match the model's active set exactly.
          assert monitor_condition_set(state) == model.active,
                 "active conditions diverged after #{inspect(event)} at t=#{now}: " <>
                   "monitor=#{inspect(MapSet.to_list(monitor_condition_set(state)))} " <>
                   "model=#{inspect(MapSet.to_list(model.active))}"

          model
        end)

      # Invariants 1 and 3: the number of alerts actually delivered to the
      # collector for each condition equals the number of episode-starts the
      # model recorded. This simultaneously asserts:
      #   * exactly one alert at the moment a condition first becomes active,
      #   * no further alert while the condition stays active (repeat
      #     ticks/triggers add no deliveries), and
      #   * one new alert for each fresh occurrence after the condition ended.
      delivered = Collector.counts(collector)

      assert delivered == model.starts,
             "delivered alert counts per condition disagree with episode starts: " <>
               "delivered=#{inspect(delivered)} expected=#{inspect(model.starts)}"

      # Tear down this iteration's children and clock so 50 runs do not
      # accumulate processes under the test supervisor.
      stop_supervised!(monitor_id)
      stop_supervised!(collector_id)
      Agent.stop(clock_agent)
    end
  end
end

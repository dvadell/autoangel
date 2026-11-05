defmodule Autoangel.SelfMonitorTest do
  use ExUnit.Case, async: true

  test "raises one no-ingest episode and recovers after an accepted sample" do
    {:ok, clock} = Agent.start_link(fn -> 0 end)

    now = fn ->
      Agent.get_and_update(clock, fn value -> {value, value + 2} end)
    end

    monitor = start_supervised!({Autoangel.SelfMonitor, no_ingest_ms: 1, clock: now, plugins: []})

    send(monitor, :tick)
    assert %{conditions: %{no_ingest: %{alerted?: true}}} = Autoangel.SelfMonitor.state(monitor)

    send(monitor, :accepted_sample)
    assert %{conditions: conditions} = Autoangel.SelfMonitor.state(monitor)
    refute Map.has_key?(conditions, :no_ingest)
  end

  test "does not register storage conditions without storage" do
    monitor = start_supervised!({Autoangel.SelfMonitor, storage_configured?: false, plugins: []})

    send(monitor, {:storage_dropped, 4})
    state = Autoangel.SelfMonitor.state(monitor)

    assert state.dropped == 0
    refute Map.has_key?(state.conditions, :storage_overflow)
  end

  test "plugin failures create an episode only after the configured count" do
    monitor =
      start_supervised!(
        {Autoangel.SelfMonitor, plugin_failure_count: 1, plugins: [:email], runners: %{}}
      )

    send(monitor, {:plugin_failure, :email})
    refute Map.has_key?(Autoangel.SelfMonitor.state(monitor).conditions, {:plugin_failure, "email"})

    send(monitor, {:plugin_failure, :email})

    assert %{conditions: %{{:plugin_failure, "email"} => %{alerted?: true}}} =
             Autoangel.SelfMonitor.state(monitor)

    send(monitor, {:plugin_success, :email})
    refute Map.has_key?(Autoangel.SelfMonitor.state(monitor).conditions, {:plugin_failure, "email"})
  end
end

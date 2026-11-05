defmodule Autoangel.Alert.PolicyTest do
  use ExUnit.Case, async: true

  alias Autoangel.Alert.Policy

  defp anomaly(id, opts \\ []) do
    Map.merge(
      %{
        id: id,
        series_key: {"requests", [{"host", "one"}]},
        start_ms: 1_000,
        end_ms: nil,
        learning?: false,
        observed: 12.0,
        lower: 1.0,
        upper: 2.0,
        score: 90.0
      },
      Map.new(opts)
    )
  end

  test "opens once, persists and delivers to every plugin" do
    state = Policy.new(plugins: [:email, :telegram])
    {state, actions} = Policy.on_event(state, {:anomaly_started, anomaly("a")}, 1_000)

    assert Enum.count(actions, &match?({:deliver, _, _}, &1)) == 2
    assert [{:incident, 1}] = Map.values(state.series_incident)
    {_state, actions} = Policy.on_event(state, {:anomaly_started, anomaly("b")}, 2_000)
    assert actions == []
  end

  test "restored closed incidents do not block and ids advance past open incidents" do
    restored = %{
      {:incident, 4} => %{
        id: {:incident, 4},
        series_key: "closed",
        series_keys: ["closed"],
        open?: false
      },
      {:incident, 7} => %{
        id: {:incident, 7},
        series_key: "open",
        series_keys: ["open"],
        open?: true
      }
    }

    state = Policy.new(incidents: restored)
    assert not Map.has_key?(state.series_incident, "closed")

    {state, _actions} =
      Policy.on_event(state, {:anomaly_started, anomaly("new", series_key: "closed")}, 10_000)

    assert Map.has_key?(state.incidents, {:incident, 8})
  end

  test "learning events are ignored and resolution starts cooldown" do
    state = Policy.new(cooldown_ms: 100)

    {state, []} =
      Policy.on_event(state, {:anomaly_started, anomaly("learning", learning?: true)}, 1_000)

    {state, _} = Policy.on_event(state, {:anomaly_started, anomaly("a")}, 1_000)

    {state, actions} =
      Policy.on_event(state, {:anomaly_resolved, anomaly("a", end_ms: 2_000)}, 2_000)

    assert {:persist, {:close, %{open?: false}}} =
             Enum.find(actions, &match?({:persist, {:close, _}}, &1))

    {_state, actions} = Policy.on_event(state, {:anomaly_started, anomaly("new")}, 2_050)
    assert actions == []
    {_state, actions} = Policy.on_event(state, {:anomaly_started, anomaly("new")}, 2_101)
    assert Enum.any?(actions, &match?({:persist, {:open, _}}, &1))
  end
end

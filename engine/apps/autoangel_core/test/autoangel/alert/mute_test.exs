defmodule Autoangel.Alert.MuteTest do
  @moduledoc "Task 23.3: the Dispatcher honours mutes while detection continues (Req 30.1)."
  use ExUnit.Case, async: true

  alias Autoangel.Alert.{Dispatcher, Policy}

  @a {"latency", [{"host", "h1"}]}
  @b {"errors", [{"host", "h1"}]}

  defmodule Runner do
    @moduledoc false
    use GenServer
    def start_link(test), do: GenServer.start_link(__MODULE__, test)
    @impl true
    def init(test), do: {:ok, test}
    @impl true
    def handle_call({:enqueue, alert}, _from, test) do
      send(test, {:delivered, alert.name})
      {:reply, :ok, test}
    end
  end

  defp anomaly(key, id, start \\ 1_000),
    do: %{id: id, series_key: key, start_ms: start, score: 90.0, learning?: false}

  defp deliveries(actions), do: for({:deliver, p, alert} <- actions, do: {p, alert})

  test "a muted Series gets an Incident but no alert; other Series still alert" do
    policy = Policy.new(plugins: [:email, :telegram]) |> Policy.mute({:series, @a})

    {policy, actions} = Policy.on_event(policy, {:anomaly_started, anomaly(@a, "a1"), []}, 1_000)
    assert deliveries(actions) == []
    assert [{:open_incident, %{muted?: true}} | _] = actions
    assert Map.has_key?(policy.series_incident, @a)

    {_policy, actions} = Policy.on_event(policy, {:anomaly_started, anomaly(@b, "b1"), []}, 1_000)
    assert [{:email, _}, {:telegram, _}] = deliveries(actions)
  end

  test "an unmuted Series alerts once per plugin" do
    {_p, actions} =
      Policy.new(plugins: [:email]) |> Policy.on_event({:anomaly_started, anomaly(@a, "a1"), []}, 1)

    assert [{:email, _}] = deliveries(actions)
  end

  test "muting is idempotent" do
    once = Policy.new() |> Policy.mute({:series, @a})
    assert Policy.mute(once, {:series, @a}).mutes == once.mutes
  end

  test "a muted Incident suppresses its further notifications" do
    {policy, _} =
      Policy.new(plugins: [:email]) |> Policy.on_event({:anomaly_started, anomaly(@a, "a1"), []}, 1)

    # The Incident id is the mute target, exactly as Feedback sends it.
    [{:incident, _} = id] = Map.keys(policy.incidents)
    refute Policy.notifications_muted?(policy, policy.incidents[id])

    policy = Policy.mute(policy, id)
    assert policy.incidents[id].muted?
    assert Policy.notifications_muted?(policy, policy.incidents[id])
  end

  test "an Incident mute in Feedback's target format reaches the open Incident" do
    {:ok, r} = Runner.start_link(self())
    {:ok, d} = Dispatcher.start_link(name: nil, plugins: [{:email, r}])
    :ok = Dispatcher.handle_event(d, {:anomaly_started, anomaly(@a, "a1"), []}, 1_000)
    :ok = Dispatcher.put_mute(d, {:incident, 1})

    policy = Dispatcher.state(d)
    assert Policy.notifications_muted?(policy, policy.incidents[{:incident, 1}])
  end

  test "a restored mute also suppresses an Incident restored without the flag" do
    incident = %{id: {:incident, 4}, series_key: @a, series_keys: [@a], open?: true}
    policy = Policy.new(incidents: %{{:incident, 4} => incident}, mutes: [{:incident, 4}])
    assert Policy.notifications_muted?(policy, policy.incidents[{:incident, 4}])
  end

  test "through the Dispatcher: no delivery for a muted Series, delivery for others" do
    {:ok, r} = Runner.start_link(self())
    {:ok, d} = Dispatcher.start_link(name: nil, plugins: [{:email, r}])
    :ok = Dispatcher.put_mute(d, {:series, @a})

    :ok = Dispatcher.handle_event(d, {:anomaly_started, anomaly(@a, "a1"), []}, 1_000)
    :ok = Dispatcher.handle_event(d, {:anomaly_started, anomaly(@b, "b1"), []}, 1_000)

    assert_receive {:delivered, "errors"}, 500
    refute_receive {:delivered, "latency"}, 100
  end
end

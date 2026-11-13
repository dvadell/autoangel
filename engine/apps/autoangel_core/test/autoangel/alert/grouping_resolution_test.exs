defmodule Autoangel.Alert.GroupingResolutionTest do
  @moduledoc """
  Tasks 36.3 to 36.6: Incident grouping, resolution notifications, and routing
  of resolutions only to plugins whose original alert arrived (Property 29).
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Autoangel.Alert.{Policy, Render}

  @window 600_000
  @a {"cpu", [{"host", "a"}]}
  @b {"mem", [{"host", "a"}]}
  @c {"disk", [{"host", "a"}]}

  defp policy(opts \\ []),
    do: Policy.new(Keyword.merge([plugins: [:email, :telegram], grouping_window_ms: @window], opts))

  defp anomaly(key, id, start),
    do: %{id: id, series_key: key, start_ms: start, score: 90.0, learning?: false}

  defp resolved(key, id, start, ended), do: Map.put(anomaly(key, id, start), :end_ms, ended)

  defp start(policy, key, id, start, now \\ nil),
    do: Policy.on_event(policy, {:anomaly_started, anomaly(key, id, start), []}, now || start)

  defp deliveries(actions), do: for({:deliver, p, alert} <- actions, do: {p, alert})

  describe "grouping" do
    test "an Anomaly inside the window joins the open Incident without a separate alert" do
      {p, actions} = start(policy(), @a, "a1", 1_000)
      assert length(deliveries(actions)) == 2

      {p, actions} = start(p, @b, "b1", 1_000 + 60_000)
      assert deliveries(actions) == []
      assert [{:join_incident, {:incident, 1}, @b}, {:persist, _}] = actions

      assert map_size(p.incidents) == 1
      assert p.incidents[{:incident, 1}].series_keys == [@a, @b]
      assert p.series_incident == %{@a => {:incident, 1}, @b => {:incident, 1}}
    end

    test "an Anomaly outside the window opens its own Incident and alerts" do
      {p, _} = start(policy(), @a, "a1", 1_000)
      {p, actions} = start(p, @b, "b1", 1_000 + @window + 1)
      assert length(deliveries(actions)) == 2
      assert map_size(p.incidents) == 2
    end

    test "the window edge is inside" do
      {p, _} = start(policy(), @a, "a1", 1_000)
      {_p, actions} = start(p, @b, "b1", 1_000 + @window)
      assert [{:join_incident, _, @b} | _] = actions
    end

    test "with grouping off every Series gets its own Incident" do
      {p, _} = start(policy(grouping_window_ms: 0), @a, "a1", 1_000)
      {p, actions} = start(p, @b, "b1", 1_001)
      assert length(deliveries(actions)) == 2
      assert map_size(p.incidents) == 2
    end

    test "the closest open Incident wins; equal distances go to the lowest number" do
      p = policy()
      {p, _} = start(p, @a, "a1", 0)
      {p, _} = start(p, @b, "b1", @window * 2)
      assert map_size(p.incidents) == 2

      # Closer to the second Incident.
      {_, actions} = start(p, @c, "c1", @window * 2 - 100_000)
      assert [{:join_incident, {:incident, 2}, @c} | _] = actions

      # Same distance from both: the lower number.
      {_, actions} = start(p, @c, "c2", @window)
      assert [{:join_incident, {:incident, 1}, @c} | _] = actions
    end

    test "a joining Anomaly counts its distance to any member of the Incident" do
      {p, _} = start(policy(), @a, "a1", 0)
      {p, _} = start(p, @b, "b1", @window)
      {_, actions} = start(p, @c, "c1", @window * 2)
      assert [{:join_incident, {:incident, 1}, @c} | _] = actions
    end

    test "the Incident stays open until its last member resolves" do
      {p, _} = start(policy(), @a, "a1", 1_000)
      {p, _} = start(p, @b, "b1", 2_000)

      {p, actions} =
        Policy.on_event(p, {:anomaly_resolved, resolved(@a, "a1", 1_000, 5_000)}, 5_000)

      assert actions == []
      assert p.incidents[{:incident, 1}].open?
      assert map_size(p.series_incident) == 2

      {p, actions} =
        Policy.on_event(p, {:anomaly_resolved, resolved(@b, "b1", 2_000, 9_000)}, 9_000)

      assert [{:close_incident, %{open?: false, closed_at: 9_000}} | _] = actions
      assert p.series_incident == %{}
    end

    test "every member is in cooldown after the close" do
      {p, _} = start(policy(cooldown_ms: 10_000), @a, "a1", 1_000)
      {p, _} = start(p, @b, "b1", 2_000)
      {p, _} = Policy.on_event(p, {:anomaly_resolved, resolved(@a, "a1", 1_000, 3_000)}, 3_000)
      {p, _} = Policy.on_event(p, {:anomaly_resolved, resolved(@b, "b1", 2_000, 4_000)}, 4_000)

      for key <- [@a, @b] do
        {_p, actions} = start(p, key, "again", 5_000)
        assert actions == []
      end
    end

    test "a muted Incident still takes new members, and sends nothing" do
      p = policy() |> Policy.mute({:series, @a})
      {p, actions} = start(p, @a, "a1", 1_000)
      assert deliveries(actions) == []
      {p, actions} = start(p, @b, "b1", 2_000)
      assert [{:join_incident, _, @b} | _] = actions

      {p, actions} =
        Policy.on_event(p, {:anomaly_resolved, resolved(@a, "a1", 1_000, 3_000)}, 3_000)

      {_p, closing} =
        Policy.on_event(p, {:anomaly_resolved, resolved(@b, "b1", 2_000, 4_000)}, 4_000)

      assert actions == []
      assert deliveries(closing) == []
    end
  end

  describe "resolution notifications" do
    defp open_and_deliver(plugins_delivered) do
      {p, _} = start(policy(), @a, "a1", 1_000)
      {p, _} = start(p, @b, "b1", 2_000)

      Enum.reduce(plugins_delivered, p, fn plugin, acc ->
        {acc, []} = Policy.on_event(acc, {:delivered, 1, plugin}, 1_500)
        acc
      end)
    end

    defp close_all(p, now \\ 9_000) do
      {p, _} = Policy.on_event(p, {:anomaly_resolved, resolved(@a, "a1", 1_000, 5_000)}, 5_000)
      Policy.on_event(p, {:anomaly_resolved, resolved(@b, "b1", 2_000, now)}, now)
    end

    test "one resolution per plugin whose alert arrived, listing the members and times" do
      p = open_and_deliver([:email])
      {_p, actions} = close_all(p)

      assert [{:email, alert}] = deliveries(actions)
      assert alert.resolved?
      assert alert.incident_id == 1
      assert Enum.map(alert.members, & &1.name) == ["cpu", "mem"]
      assert alert.start_time == 1_000
      assert alert.end_time == 9_000

      text = Render.text(alert)
      assert text =~ "status: resolved"
      assert text =~ "start: 1970-01-01T00:00:01Z"
      assert text =~ "end: 1970-01-01T00:00:09Z"
      assert text =~ "- cpu [host=a]"
      assert text =~ "- mem [host=a]"
      refute text =~ "console:"
    end

    test "the Console link is added only when configured" do
      p = policy(render: [console_base_url: "https://console.example/"])
      {p, _} = start(p, @a, "a1", 1_000)
      {p, []} = Policy.on_event(p, {:delivered, 1, :email}, 1_500)

      {_p, actions} =
        Policy.on_event(p, {:anomaly_resolved, resolved(@a, "a1", 1_000, 5_000)}, 5_000)

      assert [{:email, alert}] = deliveries(actions)
      assert Render.text(alert) =~ "console: https://console.example/anomalies/a1"
    end

    test "no plugin got the alert, so none gets a resolution" do
      {_p, actions} = close_all(open_and_deliver([]))
      assert deliveries(actions) == []
    end

    test "a delivery that arrives after the close triggers the resolution once" do
      {p, actions} = close_all(open_and_deliver([:email]))
      assert [{:email, _}] = deliveries(actions)

      {p, actions} = Policy.on_event(p, {:delivered, 1, :telegram}, 10_000)
      assert [{:telegram, %{resolved?: true}}] = deliveries(actions)

      {_p, actions} = Policy.on_event(p, {:delivered, 1, :telegram}, 11_000)
      assert actions == []
      {_p, actions} = Policy.on_event(p, {:delivered, 1, :email}, 11_000)
      assert actions == []
    end

    test "a muted Incident sends no resolution" do
      p = open_and_deliver([:email]) |> Policy.mute({:incident, 1})
      {_p, actions} = close_all(p)
      assert deliveries(actions) == []
    end

    test "an unknown Incident is ignored" do
      {p, _} = start(policy(), @a, "a1", 1_000)
      assert {^p, []} = Policy.on_event(p, {:delivered, 99, :email}, 2_000)
    end

    # Feature: v1, Property 29: Resolution notifications go only where the alert arrived
    property "a resolution goes to exactly the plugins that reported the alert, once each" do
      plugins = [:email, :telegram, :slack]

      check all(
              delivered_before <- list_of(member_of(plugins), max_length: 5),
              delivered_after <- list_of(member_of(plugins), max_length: 5),
              muted? <- boolean()
            ) do
        p = policy(plugins: plugins)
        p = if muted?, do: Policy.mute(p, {:series, @a}), else: p
        {p, _} = start(p, @a, "a1", 1_000)

        p =
          Enum.reduce(delivered_before, p, fn plugin, acc ->
            elem(Policy.on_event(acc, {:delivered, 1, plugin}, 1_500), 0)
          end)

        {p, close_actions} =
          Policy.on_event(p, {:anomaly_resolved, resolved(@a, "a1", 1_000, 5_000)}, 5_000)

        {_p, later} =
          Enum.reduce(delivered_after, {p, []}, fn plugin, {acc, sent} ->
            {acc, actions} = Policy.on_event(acc, {:delivered, 1, plugin}, 6_000)
            {acc, sent ++ actions}
          end)

        sent = Enum.map(deliveries(close_actions) ++ deliveries(later), &elem(&1, 0))
        expected = if muted?, do: [], else: Enum.uniq(delivered_before ++ delivered_after)

        assert Enum.sort(sent) == Enum.sort(expected)

        assert Enum.all?(deliveries(close_actions) ++ deliveries(later), fn {_, a} ->
                 a.resolved?
               end)
      end
    end
  end

  describe "through the Dispatcher" do
    alias Autoangel.Alert.Dispatcher

    defmodule Runner do
      @moduledoc false
      use GenServer
      def start_link(test), do: GenServer.start_link(__MODULE__, test)
      @impl true
      def init(test), do: {:ok, test}
      @impl true
      def handle_call({:enqueue, alert}, _from, test) do
        send(test, {:delivered, alert.resolved?, alert.name})
        {:reply, :ok, test}
      end
    end

    test "a plugin runner's report leads to a resolution at close" do
      {:ok, runner} = Runner.start_link(self())

      {:ok, dispatcher} =
        Dispatcher.start_link(name: nil, plugins: %{email: runner}, grouping_window_ms: @window)

      Dispatcher.handle_event(dispatcher, {:anomaly_started, anomaly(@a, "a1", 1_000), []}, 1_000)
      assert_receive {:delivered, false, "cpu"}

      GenServer.cast(dispatcher, {:delivered, 1, EnginePluginEmail})
      _ = Dispatcher.state(dispatcher)
      GenServer.cast(dispatcher, {:delivered, 1, Autoangel.Plugin.Email})
      _ = Dispatcher.state(dispatcher)

      Dispatcher.handle_event(
        dispatcher,
        {:anomaly_resolved, resolved(@a, "a1", 1_000, 5_000)},
        5_000
      )

      assert_receive {:delivered, true, "cpu"}
    end
  end
end

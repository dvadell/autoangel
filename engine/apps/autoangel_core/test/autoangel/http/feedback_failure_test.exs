defmodule Autoangel.HTTP.FeedbackFailureTest do
  @moduledoc """
  Task 23.8: feedback failure paths at the Control_API (Req 30.8, 45.4).
  A State_Store write failure answers 500 and leaves mute and sensitivity state
  (State_Store and memory) exactly as before; before boot completes the endpoint
  answers 503 and applies nothing.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Plug.Conn
  import Plug.Test

  alias Autoangel.{Feedback, SeriesRegistry, StateStore}
  alias Autoangel.HTTP.Control

  @config %{control_api: %{token: "t"}, auth_exemptions: []}
  @a SeriesRegistry.canonical_key("a", %{"host" => "h"})
  @b SeriesRegistry.canonical_key("b", %{"host" => "h"})

  # Passes the first `allow` puts to the real store, then fails every put.
  defmodule FailingStore do
    @moduledoc false
    use GenServer
    def start_link({real, allow}), do: GenServer.start_link(__MODULE__, {real, allow})
    @impl true
    def init({real, allow}), do: {:ok, %{real: real, allow: allow}}
    @impl true
    def handle_call(msg, _from, %{allow: n} = s) when elem(msg, 0) in [:put, :put_many] and n > 0,
      do: {:reply, GenServer.call(s.real, msg), %{s | allow: n - 1}}

    def handle_call(msg, _from, s) when elem(msg, 0) in [:put, :put_many],
      do: {:reply, {:error, :disk_full}, s}

    def handle_call(msg, _from, s), do: {:reply, GenServer.call(s.real, msg), s}
  end

  setup do
    dir = Path.join(System.tmp_dir!(), "aa-ff-#{System.unique_integer([:positive])}")
    {:ok, real} = StateStore.start_link(name: nil, path: dir)
    :ok = StateStore.put_sensitivity(real, SeriesRegistry.series_id(@a), 0.8)
    on_exit(fn -> File.rm_rf(dir) end)
    {:ok, real: real}
  end

  defp feedback(store) do
    test = self()

    {:ok, fb} =
      Feedback.start_link(
        name: nil,
        state_store: store,
        lookup_series: fn key ->
          if key in [@a, @b],
            do: {:ok, %{id: SeriesRegistry.series_id(key), key: key, worker: 0, kind: :gauge}},
            else: :error
        end,
        incident_series: fn
          {:incident, 1} -> {:ok, [@a, @b]}
          _ -> :error
        end,
        apply_mute: &send(test, {:mutated, &1}),
        apply_sensitivity: &send(test, {:mutated, &1.key, &2})
      )

    fb
  end

  defp post(fb, action, target) do
    conn =
      conn(:post, "/api/v1/feedback", JSON.encode!(%{"action" => action, "target" => target}))
      |> put_req_header("authorization", "Bearer t")
      |> Control.call(config: @config, feedback: fb)

    {conn.status, JSON.decode!(conn.resp_body)}
  end

  defp snapshot(real, fb),
    do:
      {StateStore.records(real, :mute), StateStore.records(real, :sensitivity),
       Feedback.overrides(fb)}

  describe "State_Store write failure" do
    setup do
      Autoangel.Boot.set_phase(:ready)
      on_exit(fn -> Autoangel.Boot.set_phase(:starting) end)
    end

    for {label, action, target, allow} <- [
          {"mute of a Series", "mute",
           %{"series" => %{"name" => "a", "labels" => %{"host" => "h"}}}, 0},
          {"mute of an Incident", "mute", %{"incident_id" => "1"}, 0},
          {"this was normal on a Series", "this was normal",
           %{"series" => %{"name" => "a", "labels" => %{"host" => "h"}}}, 0},
          {"this was normal on a multi-Series Incident", "this was normal", %{"incident_id" => "1"},
           0}
        ] do
      test "#{label} answers 500 and changes nothing", %{real: real} do
        {:ok, store} = FailingStore.start_link({real, unquote(allow)})
        fb = feedback(store)
        before = snapshot(real, fb)

        capture_log(fn ->
          assert {500, %{"error" => "store_failed"}} =
                   post(fb, unquote(action), unquote(Macro.escape(target)))
        end)

        assert snapshot(real, fb) == before
        refute_received {:mutated, _}
        refute_received {:mutated, _, _}
      end
    end
  end

  describe "partial failure inside one request" do
    setup do
      Autoangel.Boot.set_phase(:ready)
      on_exit(fn -> Autoangel.Boot.set_phase(:starting) end)
    end

    # The store accepts exactly one write call and refuses every later one. A
    # request touching two Series must still be all-or-nothing: either both
    # overrides are stored (one atomic call, 200) or neither is (500).
    test "a multi-Series 'this was normal' is never half-applied", %{real: real} do
      {:ok, store} = FailingStore.start_link({real, 1})
      fb = feedback(store)
      before = StateStore.records(real, :sensitivity)

      {{status, _body}, _log} =
        with_log(fn -> post(fb, "this was normal", %{"incident_id" => "1"}) end)

      stored = StateStore.records(real, :sensitivity) |> Map.new()

      case status do
        200 ->
          assert map_size(stored) == 2
          assert Enum.all?(Map.values(stored), &(&1 == 0.5))

        500 ->
          assert StateStore.records(real, :sensitivity) == before
          assert Feedback.overrides(fb) == Map.new(before)
          refute_received {:mutated, _, _}
      end
    end
  end

  describe "before boot completes" do
    setup do
      Autoangel.Boot.set_phase(:starting)
    end

    for action <- ["mute", "this was normal"] do
      test "#{action} answers 503 'engine starting' and applies nothing", %{real: real} do
        fb = feedback(real)
        before = snapshot(real, fb)

        assert {503, %{"error" => "engine starting"}} =
                 post(fb, unquote(action), %{"incident_id" => "1"})

        assert snapshot(real, fb) == before
        refute_received {:mutated, _}
        refute_received {:mutated, _, _}
      end
    end
  end
end

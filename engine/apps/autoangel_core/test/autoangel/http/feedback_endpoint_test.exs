defmodule Autoangel.HTTP.FeedbackEndpointTest do
  @moduledoc "POST /api/v1/feedback (Requirements 30.3, 30.4, 30.6, 30.7, 30.8, 45.4)."
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Plug.Conn
  import Plug.Test

  alias Autoangel.{Feedback, SeriesRegistry, StateStore}
  alias Autoangel.HTTP.Control

  @config %{
    control_api: %{token: "control-secret"},
    ingest: %{token: "ingest-secret"},
    auth_exemptions: []
  }
  @key SeriesRegistry.canonical_key("latency", %{"host" => "h1"})
  @series_target %{"series" => %{"name" => "latency", "labels" => %{"host" => "h1"}}}

  defmodule DownStore do
    @moduledoc false
    use GenServer
    def start_link(_), do: GenServer.start_link(__MODULE__, nil)
    @impl true
    def init(_), do: {:ok, nil}
    @impl true
    def handle_call({:put, _, _}, _from, s), do: {:reply, {:error, :disk_full}, s}
    def handle_call({:put_many, _}, _from, s), do: {:reply, {:error, :disk_full}, s}
    def handle_call({:records, _}, _from, s), do: {:reply, [], s}
  end

  setup do
    Autoangel.Boot.set_phase(:ready)
    on_exit(fn -> Autoangel.Boot.set_phase(:starting) end)
    :ok
  end

  defp tmp_store do
    dir = Path.join(System.tmp_dir!(), "aa-fbe-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, store} = StateStore.start_link(name: nil, path: dir)
    store
  end

  defp feedback(store) do
    test = self()

    {:ok, pid} =
      Feedback.start_link(
        name: nil,
        state_store: store,
        lookup_series: fn
          @key -> {:ok, %{id: SeriesRegistry.series_id(@key), key: @key, worker: 0, kind: :gauge}}
          _ -> :error
        end,
        incident_series: fn _ -> :error end,
        apply_mute: &send(test, {:muted, &1}),
        apply_sensitivity: &send(test, {:sensitivity, &1.key, &2})
      )

    pid
  end

  defp post(body, fb, auth \\ "Bearer control-secret") do
    conn =
      conn(:post, "/api/v1/feedback", body) |> put_req_header("content-type", "application/json")

    conn = if auth, do: put_req_header(conn, "authorization", auth), else: conn
    conn = Control.call(conn, config: @config, feedback: fb)
    {conn.status, decode(conn.resp_body)}
  end

  defp decode(body) do
    case JSON.decode(body || "") do
      {:ok, value} -> value
      _ -> body
    end
  end

  defp body(action, target), do: JSON.encode!(%{"action" => action, "target" => target})

  test "an authenticated mute is applied, persisted and answered 200" do
    store = tmp_store()
    fb = feedback(store)

    assert {200, %{"applied" => "mute", "target" => @series_target}} =
             post(body("mute", @series_target), fb)

    assert_received {:muted, {:series, @key}}
    assert [{{:series, @key}, true}] = StateStore.records(store, :mute)
  end

  test "repeating the same action is idempotent and succeeds again" do
    store = tmp_store()
    fb = feedback(store)
    assert {200, _} = post(body("this was normal", @series_target), fb)
    first = StateStore.records(store, :sensitivity)
    assert {200, _} = post(body("this was normal", @series_target), fb)
    assert StateStore.records(store, :sensitivity) == first
  end

  test "missing, malformed, wrong or ingest credentials get 401 and change nothing" do
    store = tmp_store()
    fb = feedback(store)

    for auth <- [nil, "Bearer", "Basic !!", "Bearer control-secreT", "Bearer ingest-secret"] do
      assert {401, _} = post(body("mute", @series_target), fb, auth), inspect(auth)
    end

    refute_received {:muted, _}
    assert StateStore.records(store, :mute) == []
  end

  test "invalid requests get 400 naming the field and change nothing" do
    store = tmp_store()
    fb = feedback(store)

    cases = [
      {"not json", "body"},
      {"[1,2]", "body"},
      {body("unmute", @series_target), "action"},
      {JSON.encode!(%{"target" => @series_target}), "action"},
      {JSON.encode!(%{"action" => "mute"}), "target"},
      {body("mute", %{"series" => %{"name" => "nope", "labels" => %{}}}), "target"},
      {body("mute", %{"incident_id" => "42"}), "target"}
    ]

    for {payload, field} <- cases do
      assert {400, %{"error" => "invalid_field", "field" => ^field}} = post(payload, fb), payload
    end

    refute_received {:muted, _}
    assert StateStore.records(store, :mute) == []
    assert StateStore.records(store, :sensitivity) == []
  end

  test "a State_Store failure gets 500 and leaves memory unchanged" do
    {:ok, store} = DownStore.start_link(nil)
    fb = feedback(store)

    capture_log(fn ->
      assert {500, %{"error" => "store_failed"}} = post(body("mute", @series_target), fb)
    end)

    refute_received {:muted, _}
  end

  test "before boot completes the endpoint answers 503 and applies nothing" do
    store = tmp_store()
    fb = feedback(store)
    Autoangel.Boot.set_phase(:starting)

    assert {503, %{"error" => "engine starting"}} = post(body("mute", @series_target), fb)
    refute_received {:muted, _}
    assert StateStore.records(store, :mute) == []
  end

  test "an unavailable Feedback process answers 503" do
    {:ok, dead} = Agent.start(fn -> nil end)
    Agent.stop(dead)
    assert {503, _} = post(body("mute", @series_target), dead)
  end

  test "an oversized body is rejected with 413 and applies nothing" do
    store = tmp_store()
    fb = feedback(store)

    big =
      JSON.encode!(%{
        "action" => "mute",
        "target" => @series_target,
        "pad" => String.duplicate("x", 70_000)
      })

    assert {413, _} = post(big, fb)
    refute_received {:muted, _}
  end

  test "only POST is accepted on the feedback path" do
    conn =
      conn(:get, "/api/v1/feedback")
      |> put_req_header("authorization", "Bearer control-secret")
      |> Control.call(config: @config)

    assert conn.status == 404
  end
end

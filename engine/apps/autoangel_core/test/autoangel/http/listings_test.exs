defmodule Autoangel.HTTP.ListingsTest do
  @moduledoc """
  Tasks 37.4 and 37.5: `GET /api/v1/incidents` and `GET /api/v1/series`
  (Property 45).
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Plug.Conn

  alias Autoangel.Alert.{Dispatcher, Policy}
  alias Autoangel.Detect.Decision.Meta
  alias Autoangel.HTTP.{Control, Listings}
  alias Autoangel.Kernel.State
  alias Autoangel.Workers
  alias Autoangel.Workers.Worker

  @token "control-token"
  @config %{control_api: %{token: @token}}

  setup_all do
    Application.put_env(:plug, :validate_header_keys_during_test, false)
    :ok
  end

  defp get_json(path, opts \\ []) do
    conn =
      Plug.Test.conn(:get, path)
      |> put_req_header("authorization", "Bearer " <> @token)
      |> Control.call(Keyword.merge([config: @config], opts))

    {conn.status, if(conn.resp_body != "", do: JSON.decode!(conn.resp_body))}
  end

  defp key_gen do
    gen all(
          name <- member_of(["cpu", "mem", "disk", "net"]),
          host <- member_of(["a", "b", "c"])
        ) do
      {name, [{"host", host}]}
    end
  end

  describe "empty state" do
    test "both listings are empty lists" do
      assert {200, %{"incidents" => []}} =
               get_json("/api/v1/incidents", incidents_fun: fn -> %{} end)

      assert {200, %{"series" => []}} = get_json("/api/v1/series", series_fun: fn -> [] end)
    end

    test "with the Dispatcher and Workers not running" do
      assert {200, %{"incidents" => []}} = get_json("/api/v1/incidents")
      assert {200, %{"series" => []}} = get_json("/api/v1/series")
    end
  end

  describe "authentication" do
    test "the Control_API credential is required" do
      for path <- ["/api/v1/incidents", "/api/v1/series"] do
        conn = Plug.Test.conn(:get, path) |> Control.call(config: @config)
        assert conn.status == 401
        assert conn.resp_body == "unauthorized"

        ingest = %{ingest: %{token: "ingest-token"}, control_api: %{token: @token}}

        wrong =
          Plug.Test.conn(:get, path)
          |> put_req_header("authorization", "Bearer ingest-token")
          |> Control.call(config: ingest)

        assert wrong.status == 401
      end
    end

    test "only GET is served" do
      conn =
        Plug.Test.conn(:post, "/api/v1/incidents")
        |> put_req_header("authorization", "Bearer " <> @token)
        |> Control.call(config: @config)

      assert conn.status == 404
    end
  end

  # Feature: v1, Property 45: Listings reflect Engine state and change nothing
  property "listings equal the held Incidents and Series, in a fixed order, and never alter them" do
    check all(
            keys <- list_of(key_gen(), max_length: 8),
            resolve_first? <- boolean(),
            statuses <- list_of(member_of([:learning, :healthy, :anomalous, :silent]), length: 8),
            kinds <- list_of(member_of([:gauge, :counter]), length: 8)
          ) do
      keys = Enum.uniq(keys)

      # Incidents, built the way the Dispatcher builds them (grouping off).
      {policy, _} =
        keys
        |> Enum.with_index()
        |> Enum.reduce({Policy.new(plugins: []), []}, fn {key, i}, {p, acc} ->
          anomaly = %{
            id: "a#{i}",
            series_key: key,
            start_ms: i * 1_000,
            score: 90.0,
            learning?: false
          }

          {p, actions} = Policy.on_event(p, {:anomaly_started, anomaly, []}, i * 1_000)
          {p, acc ++ actions}
        end)

      policy =
        case {resolve_first?, keys} do
          {true, [first | _]} ->
            ended = %{id: "a0", series_key: first, start_ms: 0, end_ms: 500, score: 90.0}
            elem(Policy.on_event(policy, {:anomaly_resolved, ended}, 600), 0)

          _ ->
            policy
        end

      entries =
        keys
        |> Enum.zip(Enum.zip(statuses, kinds))
        |> Enum.map(fn {key, {status, kind}} -> %{key: key, kind: kind, status: status} end)

      incidents_before = policy.incidents

      {200, %{"incidents" => incidents}} =
        get_json("/api/v1/incidents", incidents_fun: fn -> policy.incidents end)

      {200, %{"series" => series}} = get_json("/api/v1/series", series_fun: fn -> entries end)

      # One Incident per held Incident, with its members.
      assert length(incidents) == map_size(policy.incidents)

      assert incidents |> Enum.map(& &1["id"]) ==
               policy.incidents |> Map.keys() |> Enum.map(fn {:incident, n} -> n end) |> Enum.sort()

      for item <- incidents do
        held = Map.fetch!(policy.incidents, {:incident, item["id"]})
        assert item["open"] == held.open?
        assert item["opened_at"] == held.opened_at
        assert item["closed_at"] == held.closed_at
        assert Enum.map(item["series"], & &1["name"]) == Enum.map(held.series_keys, &elem(&1, 0))
      end

      # One entry per held Series, sorted.
      assert length(series) == length(entries)

      assert Enum.sort(Enum.map(series, &{&1["name"], &1["labels"]["host"]})) ==
               Enum.sort(Enum.map(entries, fn %{key: {n, [{"host", h}]}} -> {n, h} end))

      assert series == Enum.sort_by(series, &{&1["name"], Enum.sort(&1["labels"])})

      # The same state gives the same answer, and the state is untouched.
      assert Listings.incidents(policy.incidents) == incidents
      assert Listings.series(entries) == series
      assert policy.incidents == incidents_before
    end
  end

  describe "against the running Engine parts" do
    test "the Dispatcher's Incidents are listed and the Dispatcher is not changed" do
      {:ok, dispatcher} = Dispatcher.start_link(name: Dispatcher, plugins: %{})
      on_exit(fn -> if Process.alive?(dispatcher), do: GenServer.stop(dispatcher) end)

      key = {"cpu", [{"host", "a"}]}
      anomaly = %{id: "a1", series_key: key, start_ms: 1_000, score: 90.0, learning?: false}
      Dispatcher.handle_event({:anomaly_started, anomaly, []}, 1_000)

      before = Dispatcher.state()
      assert {200, %{"incidents" => [incident]}} = get_json("/api/v1/incidents")
      assert incident["open"]
      assert incident["series"] == [%{"name" => "cpu", "labels" => %{"host" => "a"}}]
      assert Dispatcher.state() == before
    end

    test "the Workers' Series are listed and the Workers are not changed" do
      sup =
        start_supervised!(
          {Workers, name: Workers, pool_size: 2, state_store: nil, sweep_interval: 3_600_000}
        )

      _ = sup
      keys = for n <- ["cpu", "mem", "disk"], do: {n, [{"host", "a"}]}

      for {key, index} <- Enum.with_index(keys) do
        pid = Workers.worker(rem(index, 2))
        meta = %Meta{key: key, kind: :gauge, status: :healthy}

        Worker.put_series(
          pid,
          key,
          State.encode(State.new()),
          meta
        )
      end

      before = for i <- 0..1, do: Worker.snapshot(Workers.worker(i))
      assert {200, %{"series" => series}} = get_json("/api/v1/series")
      assert Enum.map(series, & &1["name"]) == ["cpu", "disk", "mem"]
      assert Enum.all?(series, &(&1["status"] == "healthy" and &1["kind"] == "gauge"))
      assert for(i <- 0..1, do: Worker.snapshot(Workers.worker(i))) == before
    end
  end
end

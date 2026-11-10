defmodule AutoangelTsdb.StoredCandidatesPropertyTest do
  @moduledoc """
  Property 26: stored Candidates round trip (Req 21.1, 21.3, 21.4, 21.5).

  Runs against real TimescaleDB (127.0.0.1:5433) in a private schema and skips
  gracefully when the database is unreachable.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  alias Autoangel.SeriesRegistry
  alias Autoangel.Storage.Rows
  alias AutoangelTsdb.Retention

  @moduletag :integration

  @conn_opts [
    hostname: "127.0.0.1",
    port: 5433,
    username: "autoangel",
    password: "autoangel",
    database: "autoangel",
    connect_timeout: 2_000,
    timeout: 10_000
  ]

  @target {"target_metric", [{"job", "api"}]}
  @pool for i <- 1..10, do: {"cand_#{i}", [{"host", "h#{rem(i, 3)}"}]}

  setup do
    with true <- reachable?(),
         schema = "aa_p26_#{System.system_time(:microsecond)}_#{:erlang.phash2(make_ref())}",
         {:ok, conn} <-
           Postgrex.start_link(@conn_opts ++ [parameters: [search_path: "#{schema},public"]]) do
      {:ok, _} = Postgrex.query(conn, "CREATE SCHEMA #{schema}", [])
      {:ok, _} = Postgrex.query(conn, "SET search_path TO #{schema}, public", [])
      {:ok, 3} = AutoangelTsdb.migrate(conn)
      rows = for key <- [@target | @pool], do: Rows.series(key, :gauge, :healthy)
      :ok = AutoangelTsdb.write_series(conn, rows)

      on_exit(fn ->
        with {:ok, c} <- Postgrex.start_link(@conn_opts) do
          _ = Postgrex.query(c, "DROP SCHEMA IF EXISTS #{schema} CASCADE", [])
          GenServer.stop(c)
        end
      end)

      {:ok, conn: conn}
    else
      _ -> {:ok, skip: true}
    end
  end

  defp reachable? do
    case :gen_tcp.connect(~c"127.0.0.1", 5433, [], 1_000) do
      {:ok, socket} -> :gen_tcp.close(socket) == :ok
      _ -> false
    end
  end

  defp run(%{skip: true}, _fun), do: IO.puts("[integration] skipped: TimescaleDB unreachable")
  defp run(%{conn: conn}, fun), do: fun.(conn)

  # --- generators ---------------------------------------------------------

  defp evidence_gen do
    fixed_map(%{
      delta_ms: integer(-600_000..600_000),
      before?: boolean(),
      score: float(min: 0.0, max: 100.0),
      shared_labels: list_of(member_of(["host", "job", "service"]), max_length: 3),
      support: integer(0..50),
      co_occurrence_window_ms: constant(600_000),
      event_like?: boolean()
    })
  end

  defp candidates_gen do
    gen all(
          n <- integer(0..8),
          order <- list_of(integer(), length: length(@pool)),
          evidence <- list_of(evidence_gen(), length: n),
          weak <- list_of(boolean(), length: n)
        ) do
      keys =
        @pool
        |> Enum.zip(order)
        |> Enum.sort_by(&elem(&1, 1))
        |> Enum.map(&elem(&1, 0))
        |> Enum.take(n)

      [keys, evidence, weak]
      |> Enum.zip()
      |> Enum.with_index(1)
      |> Enum.map(fn {{key, ev, weak?}, rank} ->
        %{series_key: key, rank: rank, weak?: weak?, evidence: ev}
      end)
    end
  end

  defp anomaly(id, start_ms, end_ms \\ nil) do
    %{
      id: id,
      series_key: @target,
      start_ms: start_ms,
      end_ms: end_ms,
      score: 90.0,
      observed: 5.0,
      lower: 1.0,
      upper: 2.0,
      baseline_type: :robust
    }
  end

  defp new_id, do: :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)

  # --- reads --------------------------------------------------------------

  defp read_candidates(conn, id) do
    {:ok, %{rows: rows}} =
      Postgrex.query(
        conn,
        "SELECT series_id, rank, evidence, weak FROM anomaly_candidates WHERE anomaly_id = $1 ORDER BY rank",
        [Rows.anomaly_uuid(id)]
      )

    for [sid, rank, ev, weak] <- rows, do: {sid, rank, decode(ev), weak}
  end

  defp decode(json) when is_binary(json), do: Jason.decode!(json)
  defp decode(map), do: map

  defp expected(candidates) do
    for c <- candidates do
      {SeriesRegistry.series_id(c.series_key), c.rank,
       c.evidence |> Map.put(:weak?, c.weak?) |> Jason.encode!() |> Jason.decode!(), c.weak?}
    end
  end

  defp status(conn, id) do
    {:ok, %{rows: rows}} =
      Postgrex.query(conn, "SELECT status FROM anomalies WHERE id = $1", [Rows.anomaly_uuid(id)])

    rows
  end

  # --- properties ---------------------------------------------------------

  property "candidates read back identically and survive later updates", ctx do
    run(ctx, fn conn ->
      check all(
              candidates <- candidates_gen(),
              updates <- list_of(member_of([:still_open, :resolved]), max_length: 3),
              max_runs: 40
            ) do
        id = new_id()
        start = 1_700_000_000_000

        assert :ok =
                 AutoangelTsdb.write_anomalies(conn, [
                   Rows.anomaly_item(anomaly(id, start), candidates)
                 ])

        stored = read_candidates(conn, id)
        assert stored == expected(candidates)
        assert Enum.map(stored, &elem(&1, 1)) == Enum.to_list(1..length(candidates)//1)

        for {u, i} <- Enum.with_index(updates, 1) do
          end_ms = if u == :resolved, do: start + i * 1_000

          assert :ok =
                   AutoangelTsdb.write_anomalies(conn, [
                     Rows.anomaly_update(anomaly(id, start, end_ms))
                   ])

          assert read_candidates(conn, id) == stored
          assert status(conn, id) == [[if(u == :resolved, do: "resolved", else: "open")]]
        end
      end
    end)
  end

  test "retention removes an ended anomaly and its candidates", ctx do
    run(ctx, fn conn ->
      id = new_id()
      [c | _] = @pool
      cands = [%{series_key: c, rank: 1, weak?: false, evidence: %{support: 1}}]
      :ok = AutoangelTsdb.write_anomalies(conn, [Rows.anomaly_item(anomaly(id, 1_000), cands)])
      now = ~U[2024-01-10 00:00:00Z]
      end_ms = DateTime.to_unix(now, :millisecond) - 3 * 86_400_000
      :ok = AutoangelTsdb.write_anomalies(conn, [Rows.anomaly_update(anomaly(id, 1_000, end_ms))])

      {:ok, _} = Retention.cleanup_anomalies(conn, 86_400, now: now)
      assert status(conn, id) == []
      assert read_candidates(conn, id) == []
    end)
  end

  property "every candidate row references an existing anomaly across writes and retention", ctx do
    run(ctx, fn conn ->
      check all(
              ops <-
                list_of(
                  one_of([
                    tuple({constant(:start), candidates_gen()}),
                    tuple({constant(:resolve), integer(0..20), integer(0..3)}),
                    tuple({constant(:retention), integer(1..4)})
                  ]),
                  max_length: 10
                ),
              max_runs: 25
            ) do
        {:ok, _} = Postgrex.query(conn, "DELETE FROM anomalies", [])
        now = ~U[2024-01-10 00:00:00Z]

        live =
          Enum.reduce(ops, %{}, fn
            {:start, cands}, live ->
              id = new_id()

              :ok =
                AutoangelTsdb.write_anomalies(conn, [Rows.anomaly_item(anomaly(id, 1_000), cands)])

              Map.put(live, id, cands)

            {:resolve, pick, days_ago}, live when map_size(live) > 0 ->
              id = live |> Map.keys() |> Enum.sort() |> Enum.at(rem(pick, map_size(live)))
              end_ms = DateTime.to_unix(now, :millisecond) - days_ago * 86_400_000

              :ok =
                AutoangelTsdb.write_anomalies(conn, [
                  Rows.anomaly_update(anomaly(id, 1_000, end_ms))
                ])

              live

            {:retention, days}, live ->
              {:ok, _} = Retention.cleanup_anomalies(conn, days * 86_400, now: now)

              Map.filter(live, fn {id, _} -> status(conn, id) != [] end)

            _op, live ->
              live
          end)

        {:ok, %{rows: [[orphans]]}} =
          Postgrex.query(
            conn,
            "SELECT count(*) FROM anomaly_candidates c LEFT JOIN anomalies a ON a.id = c.anomaly_id WHERE a.id IS NULL",
            []
          )

        assert orphans == 0

        for {id, cands} <- live, do: assert(read_candidates(conn, id) == expected(cands))
      end
    end)
  end
end

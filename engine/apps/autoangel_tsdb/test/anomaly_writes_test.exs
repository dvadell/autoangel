defmodule AutoangelTsdb.AnomalyWritesTest do
  @moduledoc """
  Task 21.1 against real TimescaleDB (127.0.0.1:5433), skipping gracefully when
  unreachable. Each test runs in its own schema.

  Covers: Series map upserts and status updates, an Anomaly written with all its
  Candidates (rank, evidence, weak) in one transaction, later updates touching
  only the anomalies row, an empty Candidate list writing zero candidate rows,
  baseline band rows, and transactional rollback when a Candidate is invalid.
  """
  use ExUnit.Case, async: false

  alias Autoangel.Storage.Rows

  @moduletag :integration

  @conn_opts [
    hostname: "127.0.0.1",
    port: 5433,
    username: "autoangel",
    password: "autoangel",
    database: "autoangel",
    connect_timeout: 2_000,
    timeout: 5_000
  ]

  setup do
    with true <- reachable?(),
         {:ok, conn} <- Postgrex.start_link(@conn_opts) do
      schema = "aa_aw_#{System.system_time(:microsecond)}_#{:erlang.phash2(make_ref())}"
      {:ok, _} = Postgrex.query(conn, "CREATE SCHEMA #{schema}", [])
      {:ok, _} = Postgrex.query(conn, "SET search_path TO #{schema}, public", [])
      {:ok, 2} = AutoangelTsdb.migrate(conn)

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

  defp skip_or(%{skip: true}, _fun), do: IO.puts("[integration] skipped: TimescaleDB unreachable")
  defp skip_or(%{conn: conn}, fun), do: fun.(conn)

  defp reachable? do
    case :gen_tcp.connect(~c"127.0.0.1", 5433, [], 1_000) do
      {:ok, socket} -> :gen_tcp.close(socket) == :ok
      _ -> false
    end
  end

  @target {"requests_total", [{"job", "api"}]}
  @cand_a {"cpu", [{"host", "a"}]}
  @cand_b {"mem", [{"host", "a"}]}

  defp seed_series(conn) do
    rows = for key <- [@target, @cand_a, @cand_b], do: Rows.series(key, :gauge, :learning)
    assert :ok = AutoangelTsdb.write_series(conn, rows)
  end

  defp anomaly(opts \\ []) do
    %{
      id: Keyword.get(opts, :id, "0123456789abcdef0123456789abcdef"),
      series_key: @target,
      kind: :value,
      start_ms: 1_700_000_000_000,
      end_ms: Keyword.get(opts, :end_ms),
      score: 91.5,
      observed: 12.5,
      lower: 1.0,
      upper: 2.0,
      baseline_type: :robust,
      learning?: Keyword.get(opts, :learning?, false)
    }
  end

  defp candidates do
    [
      %{
        series_key: @cand_a,
        rank: 1,
        weak?: false,
        evidence: %{support: 3, delta_ms: -10, before?: true}
      },
      %{
        series_key: @cand_b,
        rank: 2,
        weak?: true,
        evidence: %{support: 0, delta_ms: 40, before?: false}
      }
    ]
  end

  defp stored_candidates(conn, id) do
    {:ok, %{rows: rows}} =
      Postgrex.query(
        conn,
        "SELECT series_id, rank, evidence, weak FROM anomaly_candidates WHERE anomaly_id = $1 ORDER BY rank",
        [Rows.anomaly_uuid(id)]
      )

    Enum.map(rows, fn [sid, rank, evidence, weak] -> [sid, rank, decode(evidence), weak] end)
  end

  defp decode(json) when is_binary(json), do: Jason.decode!(json)
  defp decode(map), do: map

  defp anomaly_row(conn, id) do
    {:ok, %{rows: rows}} =
      Postgrex.query(
        conn,
        "SELECT status, end_time, score, kind, learning, baseline_type FROM anomalies WHERE id = $1",
        [Rows.anomaly_uuid(id)]
      )

    rows
  end

  test "series maps upsert and status changes update the row", ctx do
    skip_or(ctx, fn conn ->
      seed_series(conn)
      assert :ok = AutoangelTsdb.write_series(conn, [Rows.series(@target, :gauge, :anomalous)])

      {:ok, %{rows: [[name, labels, status]]}} =
        Postgrex.query(conn, "SELECT name, labels, status FROM series WHERE id = $1", [
          Autoangel.SeriesRegistry.series_id(@target)
        ])

      assert {name, decode(labels), status} == {"requests_total", %{"job" => "api"}, "anomalous"}
    end)
  end

  test "an Anomaly is written with all its Candidates, rank, evidence and weak", ctx do
    skip_or(ctx, fn conn ->
      seed_series(conn)
      a = anomaly()
      assert :ok = AutoangelTsdb.write_anomalies(conn, [Rows.anomaly_item(a, candidates())])

      assert [["open", nil, 91.5, "value", false, "robust"]] = anomaly_row(conn, a.id)

      assert [[sa, 1, ev_a, false], [sb, 2, ev_b, true]] = stored_candidates(conn, a.id)
      assert sa == Autoangel.SeriesRegistry.series_id(@cand_a)
      assert sb == Autoangel.SeriesRegistry.series_id(@cand_b)
      assert ev_a["support"] == 3 and ev_a["weak?"] == false
      assert ev_b["support"] == 0 and ev_b["weak?"] == true
    end)
  end

  test "a later update touches only the anomalies row", ctx do
    skip_or(ctx, fn conn ->
      seed_series(conn)
      a = anomaly()
      assert :ok = AutoangelTsdb.write_anomalies(conn, [Rows.anomaly_item(a, candidates())])
      before = stored_candidates(conn, a.id)

      resolved = %{a | end_ms: 1_700_000_060_000}
      assert :ok = AutoangelTsdb.write_anomalies(conn, [Rows.anomaly_update(resolved)])

      assert [["resolved", %DateTime{}, _, _, _, _]] = anomaly_row(conn, a.id)
      assert stored_candidates(conn, a.id) == before
    end)
  end

  test "a retried start is idempotent and does not duplicate candidates", ctx do
    skip_or(ctx, fn conn ->
      seed_series(conn)
      item = Rows.anomaly_item(anomaly(), candidates())
      assert :ok = AutoangelTsdb.write_anomalies(conn, [item])
      assert :ok = AutoangelTsdb.write_anomalies(conn, [item])
      assert length(stored_candidates(conn, anomaly().id)) == 2
    end)
  end

  test "an empty Candidate list writes the Anomaly with zero candidate rows", ctx do
    skip_or(ctx, fn conn ->
      seed_series(conn)
      a = anomaly(learning?: true)
      assert :ok = AutoangelTsdb.write_anomalies(conn, [Rows.anomaly_item(a, [])])
      assert [["open", nil, _, _, true, _]] = anomaly_row(conn, a.id)
      assert stored_candidates(conn, a.id) == []
    end)
  end

  test "the Anomaly and its Candidates commit together or not at all", ctx do
    skip_or(ctx, fn conn ->
      seed_series(conn)
      a = anomaly()
      bad = [%{series_key: {"never_written", []}, rank: 1, weak?: false, evidence: %{}}]

      # A foreign-key violation is permanent: retrying can never succeed.
      assert {:permanent, _} = AutoangelTsdb.write_anomalies(conn, [Rows.anomaly_item(a, bad)])
      assert anomaly_row(conn, a.id) == []
    end)
  end

  test "baseline band rows are written and upserted per series and timestamp", ctx do
    skip_or(ctx, fn conn ->
      seed_series(conn)
      band = Rows.band(@target, 1_700_000_000_000, 1.0, 2.0, :robust)
      assert :ok = AutoangelTsdb.write_bands(conn, [band])
      assert :ok = AutoangelTsdb.write_bands(conn, [%{band | upper: 3.0}])

      {:ok, %{rows: rows}} =
        Postgrex.query(
          conn,
          "SELECT lower_bound, upper_bound, baseline_type FROM baseline_bands",
          []
        )

      assert rows == [[1.0, 3.0, "robust"]]
    end)
  end

  test "a band for an unknown Series is a permanent failure, not an endless retry", ctx do
    skip_or(ctx, fn conn ->
      band = Rows.band({"never_written", []}, 1_700_000_000_000, 1.0, 2.0, :robust)
      assert {:permanent, _} = AutoangelTsdb.write_bands(conn, [band])
    end)
  end
end

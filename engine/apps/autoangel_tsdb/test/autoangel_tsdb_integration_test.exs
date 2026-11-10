defmodule AutoangelTsdb.IntegrationTest.CapturingBackend do
  @moduledoc """
  A storage backend double used to prove Requirement 38.10: the engine must not
  write Samples until migrations complete successfully.

  Every behaviour callback reports to the configured test pid before returning,
  so a test can assert the backend is NEVER called while the writer is not
  migration-ready. Implementing the full `Autoangel.Storage` behaviour keeps the
  Writer's runtime callback detection happy.
  """
  @behaviour Autoangel.Storage

  @impl true
  def init(config) when is_map(config), do: {:ok, %{report_to: Map.get(config, :report_to)}}

  @impl true
  def append_samples(handle, samples), do: report(handle, {:append_samples, samples})

  @impl true
  def write_anomalies(handle, rows), do: report(handle, {:write_anomalies, rows})

  @impl true
  def write_series(handle, rows), do: report(handle, {:write_series, rows})

  @impl true
  def write_bands(handle, rows), do: report(handle, {:write_bands, rows})

  @impl true
  def read_range(handle, _ids, _from, _to) do
    _ = report(handle, {:read_range, []})
    {:ok, []}
  end

  @impl true
  def read_aggregates(handle, _ids, _from, _to, _resolution) do
    _ = report(handle, {:read_aggregates, []})
    {:ok, []}
  end

  defp report(%{report_to: pid}, payload) when is_pid(pid) do
    send(pid, {:backend_called, payload})
    :ok
  end

  defp report(_handle, _payload), do: :ok
end

defmodule AutoangelTsdb.IntegrationTest do
  @moduledoc """
  Integration tests exercising the real TimescaleDB schema contract.

  These tests require a running TimescaleDB instance reachable with `@conn_opts`.
  They are tagged `:integration` and skip gracefully (never fail) when the
  database is unreachable, so the suite stays green on machines without the
  container.

  ## Isolation

  Each test runs inside its own freshly created PostgreSQL schema
  (`CREATE SCHEMA` + `SET search_path`). This was chosen over a fresh database
  because:

    * The migrations create tables with unqualified names plus
      `create_hypertable`/continuous-aggregate views, all of which respect
      `search_path`, so they land cleanly inside the per-test schema (verified
      against this TimescaleDB 2.x / PostgreSQL 16 instance).
    * A new schema on an existing connection is dramatically cheaper than
      `CREATE DATABASE`, which cannot run inside a transaction and requires a
      second connection to the maintenance database.
    * Retention/refresh policies are discoverable per schema via
      `timescaledb_information.jobs.hypertable_schema` (for `samples`) and via
      the continuous aggregate's materialization hypertable (for the refresh
      policies), so assertions can be scoped precisely to the test's schema.

  The shared `CREATE EXTENSION IF NOT EXISTS timescaledb` is global and harmless
  to repeat. Every schema is dropped with `DROP SCHEMA ... CASCADE` in `on_exit`,
  tolerant of errors.
  """

  use ExUnit.Case, async: false

  alias Autoangel.Storage.Writer
  alias AutoangelTsdb.IntegrationTest.CapturingBackend
  alias AutoangelTsdb.Migrations

  @moduletag :integration

  @conn_opts [
    hostname: "127.0.0.1",
    port: 5433,
    username: "autoangel",
    password: "autoangel",
    database: "autoangel"
  ]

  # A short timeout so the skip path is fast on machines without the container.
  @connect_timeout 2_000

  setup do
    case try_connect() do
      {:ok, conn} ->
        schema = unique_schema()
        {:ok, _} = Postgrex.query(conn, "CREATE SCHEMA #{schema}", [])
        {:ok, _} = Postgrex.query(conn, "SET search_path TO #{schema}, public", [])

        on_exit(fn ->
          # A fresh short-lived connection: the test's connection is already gone
          # by the time on_exit runs. Tolerant of any failure.
          _ =
            with {:ok, cleanup} <- try_connect() do
              _ = Postgrex.query(cleanup, "DROP SCHEMA IF EXISTS #{schema} CASCADE", [])
              GenServer.stop(cleanup)
            end
        end)

        {:ok, conn: conn, schema: schema}

      :error ->
        {:ok, skip: true}
    end
  end

  describe "schema migrations against real TimescaleDB" do
    # Requirement 38.1: migrating an empty database creates the full schema.
    test "migrates an empty schema to the current version", ctx do
      skip_or(ctx, fn %{conn: conn, schema: schema} ->
        assert {:ok, version} = AutoangelTsdb.migrate(conn)
        assert version == Migrations.version()
        assert version == 3

        # schema_version row records the applied contract version.
        assert version == stored_version(conn)

        # Every expected base table exists inside this schema.
        for table <- ~w(series samples ingest_batches anomalies anomaly_candidates
                        engine_status baseline_bands schema_version) do
          assert table_exists?(conn, schema, table),
                 "expected table #{table} to exist in schema #{schema}"
        end

        # samples (and baseline_bands) are hypertables in this schema.
        assert hypertable?(conn, schema, "samples")
        assert hypertable?(conn, schema, "baseline_bands")

        # Continuous aggregates created by v1.
        assert continuous_aggregate?(conn, schema, "samples_1m")
        assert continuous_aggregate?(conn, schema, "samples_1h")
      end)
    end

    # Requirement 38.9: re-running migrations is a no-op and preserves stored data.
    test "re-running migrations is a no-op and leaves stored rows untouched", ctx do
      skip_or(ctx, fn %{conn: conn, schema: schema} ->
        assert {:ok, 3} = AutoangelTsdb.migrate(conn)

        tables_before = table_set(conn, schema)
        version_before = stored_version(conn)

        # Insert a durable row that must survive a second migration run.
        series_id = :crypto.strong_rand_bytes(16)

        {:ok, _} =
          Postgrex.query(
            conn,
            "INSERT INTO series (id, name, labels, kind, status) " <>
              "VALUES ($1, 'integration.noop', '{}'::jsonb, 'gauge', 'learning')",
            [series_id]
          )

        # Second run must succeed and change nothing structural.
        assert :ok = Migrations.run(conn)
        assert {:ok, 3} = AutoangelTsdb.migrate(conn)

        assert table_set(conn, schema) == tables_before
        assert stored_version(conn) == version_before

        # The previously stored row is still present and unchanged.
        assert {:ok, %{rows: [["integration.noop"]]}} =
                 Postgrex.query(conn, "SELECT name FROM series WHERE id = $1", [series_id])
      end)
    end

    # Requirement 38.10: a failed migration leaves the last good state and blocks writes.
    test "a failed migration preserves the last state and blocks sample writes", ctx do
      skip_or(ctx, fn %{conn: conn, schema: schema} ->
        # Pre-create a conflicting `samples` table so create_hypertable('samples','ts')
        # fails deterministically during migration version 1.
        {:ok, _} = Postgrex.query(conn, "CREATE TABLE samples (x int)", [])

        assert {:error, {:migration_failed, 1, _reason}} = Migrations.run(conn)

        # Version must NOT advance to 1; it stays at the last applied state (0).
        assert stored_version(conn) == 0

        # The pre-existing (last-good) samples definition is intact: the failed
        # version rolled back without clobbering it.
        assert ["x"] == column_names(conn, schema, "samples")

        # No expected v1 object was partially created.
        refute table_exists?(conn, schema, "series")
        refute table_exists?(conn, schema, "anomalies")

        # Writes are blocked until migrations complete successfully.
        #
        # The engine never fabricates a ready handle: a Storage.Writer started
        # with migration_ready: false and then told the migration FAILED must
        # not hand any Sample to its backend. A capturing backend records every
        # append_samples/write_* call; after enqueueing Samples the backend must
        # have been called zero times, proving the engine does not write Samples
        # until migrations complete successfully (Requirement 38.10). This is a
        # faithful end-to-end check of the gate (append_samples/2's internal
        # ready clause is bypassed by a bare pid, so asserting on it would test
        # nothing real).
        test_pid = self()

        {:ok, writer} =
          Writer.start_link(
            backend: CapturingBackend,
            backend_config: %{report_to: test_pid},
            migration_ready: false
          )

        Writer.migration_failed(writer)

        :ok =
          Writer.enqueue_samples(writer, [
            %{series_id: :crypto.strong_rand_bytes(16), ts_ms: 1, value: 1.0}
          ])

        # Give any (erroneous) dispatch a chance to run before asserting silence.
        _ = Writer.stats(writer)
        refute_receive {:backend_called, _}, 200

        # The samples are still queued, not written or dropped.
        stats = Writer.stats(writer)
        assert stats.ready? == false
        assert stats.queued == 1
        assert stats.successful == 0

        GenServer.stop(writer)
      end)
    end
  end

  describe "retention policies against real TimescaleDB" do
    # Requirement 39.2: retention policies are applied with the configured windows.
    # Requirement 39.7: re-applying replaces the earlier windows instead of duplicating.
    test "applies retention/refresh policies and replaces them on re-apply", ctx do
      skip_or(ctx, fn %{conn: conn, schema: schema} ->
        assert {:ok, 3} = AutoangelTsdb.migrate(conn)

        first = %{retention: %{raw: 7 * 86_400, aggregate: 30 * 86_400, anomaly: 90 * 86_400}}
        assert {:ok, _results} = AutoangelTsdb.apply_retention_policies(conn, first)

        # 39.2: exactly one raw retention policy on `samples`, with the configured window.
        assert [{"samples", "7 days"}] == retention_policies(conn, schema)

        # Refresh policies exist on the samples_1m / samples_1h continuous aggregates.
        # refresh start_offset == max(raw - 1, 1) seconds == 604799s == 167:59:59.
        refresh_first = refresh_offsets(conn, schema)
        assert map_size(refresh_first) == 2
        assert Map.values(refresh_first) |> Enum.uniq() == ["167:59:59"]

        # 39.7 (aggregate retention): aggregate retention policies are applied on
        # the samples_1m / samples_1h continuous aggregates with the configured
        # aggregate window. These `policy_retention` jobs live on the caggs'
        # materialization hypertables under `_timescaledb_internal`, NOT under the
        # test schema, so `retention_policies/2` (scoped to the test schema) can
        # never see them; they must be found via the materialization hypertable
        # names. Exactly two, both at the first aggregate window (30 days).
        assert ["30 days", "30 days"] == aggregate_retention_windows(conn, schema)

        # 39.7: re-applying with different windows REPLACES, not duplicates.
        second = %{retention: %{raw: 21 * 86_400, aggregate: 60 * 86_400, anomaly: 90 * 86_400}}
        assert {:ok, _results} = AutoangelTsdb.apply_retention_policies(conn, second)

        # Still exactly one raw retention policy, now with the new window.
        assert [{"samples", "21 days"}] == retention_policies(conn, schema)

        # Refresh offsets updated in place (21d - 1s == 1814399s == 503:59:59), still 2 policies.
        refresh_second = refresh_offsets(conn, schema)
        assert map_size(refresh_second) == 2
        assert Map.values(refresh_second) |> Enum.uniq() == ["503:59:59"]

        # 39.7 (aggregate retention): the aggregate retention windows are REPLACED
        # in place, not duplicated — still exactly two policies, now at the second
        # window (60 days). A duplicated policy would show four entries (or a mix
        # of 30/60 days); a dropped one would show fewer than two.
        assert ["60 days", "60 days"] == aggregate_retention_windows(conn, schema)
      end)
    end
  end

  # --- helpers ---------------------------------------------------------------

  # Runs `fun` with the test context when the database is available; otherwise
  # skips (prints a notice and returns :ok) so the suite stays green without a DB.
  defp skip_or(%{skip: true}, _fun) do
    IO.puts("[integration] skipped: TimescaleDB unreachable at #{inspect(@conn_opts[:hostname])}")
    :ok
  end

  defp skip_or(ctx, fun), do: fun.(ctx)

  # Establishes a verified connection, or returns :error so the test skips.
  #
  # A fast TCP pre-check avoids Postgrex's background reconnect loop (and the
  # linked-process crash it can cause) when the container is absent, keeping the
  # skip path instant and incapable of failing the suite. Exits are trapped
  # defensively so that a linked connection crash can never bubble up as a test
  # failure.
  defp try_connect do
    if port_open?(@conn_opts[:hostname], @conn_opts[:port]) do
      previous = Process.flag(:trap_exit, true)

      try do
        start_and_verify()
      catch
        _, _ -> :error
      after
        Process.flag(:trap_exit, previous)
      end
    else
      :error
    end
  end

  defp port_open?(host, port) do
    case :gen_tcp.connect(to_charlist(host), port, [:binary, active: false], @connect_timeout) do
      {:ok, socket} ->
        :gen_tcp.close(socket)
        true

      {:error, _} ->
        false
    end
  catch
    _, _ -> false
  end

  defp start_and_verify do
    opts = @conn_opts ++ [connect_timeout: @connect_timeout, timeout: @connect_timeout]

    case Postgrex.start_link(opts) do
      {:ok, pid} ->
        case Postgrex.query(pid, "SELECT 1", [], timeout: @connect_timeout) do
          {:ok, _} -> {:ok, pid}
          _ -> stop_and_error(pid)
        end

      _ ->
        :error
    end
  end

  defp stop_and_error(pid) do
    _ =
      try do
        GenServer.stop(pid)
      catch
        _, _ -> :ok
      end

    :error
  end

  defp unique_schema,
    do: "aa_it_#{System.system_time(:microsecond)}_#{:erlang.phash2(make_ref())}"

  defp stored_version(conn) do
    case Postgrex.query(conn, "SELECT version FROM schema_version WHERE id = true", []) do
      {:ok, %{rows: [[version]]}} -> version
      {:ok, %{rows: []}} -> 0
      {:error, _} -> 0
    end
  end

  defp table_exists?(conn, schema, table) do
    {:ok, %{rows: [[count]]}} =
      Postgrex.query(
        conn,
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2",
        [schema, table]
      )

    count == 1
  end

  defp table_set(conn, schema) do
    {:ok, %{rows: rows}} =
      Postgrex.query(
        conn,
        "SELECT table_name FROM information_schema.tables WHERE table_schema = $1 ORDER BY table_name",
        [schema]
      )

    rows |> Enum.map(&hd/1) |> MapSet.new()
  end

  defp column_names(conn, schema, table) do
    {:ok, %{rows: rows}} =
      Postgrex.query(
        conn,
        "SELECT column_name FROM information_schema.columns " <>
          "WHERE table_schema = $1 AND table_name = $2 ORDER BY column_name",
        [schema, table]
      )

    Enum.map(rows, &hd/1)
  end

  defp hypertable?(conn, schema, name) do
    {:ok, %{rows: [[count]]}} =
      Postgrex.query(
        conn,
        "SELECT count(*) FROM timescaledb_information.hypertables " <>
          "WHERE hypertable_schema = $1 AND hypertable_name = $2",
        [schema, name]
      )

    count == 1
  end

  defp continuous_aggregate?(conn, schema, view) do
    {:ok, %{rows: [[count]]}} =
      Postgrex.query(
        conn,
        "SELECT count(*) FROM timescaledb_information.continuous_aggregates " <>
          "WHERE view_schema = $1 AND view_name = $2",
        [schema, view]
      )

    count == 1
  end

  # Raw retention policies scoped to this schema: [{hypertable_name, drop_after}].
  defp retention_policies(conn, schema) do
    {:ok, %{rows: rows}} =
      Postgrex.query(
        conn,
        "SELECT hypertable_name, config->>'drop_after' FROM timescaledb_information.jobs " <>
          "WHERE proc_name = 'policy_retention' AND hypertable_schema = $1 " <>
          "ORDER BY hypertable_name",
        [schema]
      )

    Enum.map(rows, fn [name, drop_after] -> {name, drop_after} end)
  end

  # Refresh policy start_offsets keyed by continuous aggregate view name for this schema.
  defp refresh_offsets(conn, schema) do
    {:ok, %{rows: cagg_rows}} =
      Postgrex.query(
        conn,
        "SELECT view_name, materialization_hypertable_name " <>
          "FROM timescaledb_information.continuous_aggregates " <>
          "WHERE view_schema = $1 AND view_name IN ('samples_1m', 'samples_1h')",
        [schema]
      )

    mat_to_view = Map.new(cagg_rows, fn [view, mat] -> {mat, view} end)
    mat_names = Map.keys(mat_to_view)

    {:ok, %{rows: job_rows}} =
      Postgrex.query(
        conn,
        "SELECT hypertable_name, config->>'start_offset' FROM timescaledb_information.jobs " <>
          "WHERE proc_name = 'policy_refresh_continuous_aggregate' AND hypertable_name = ANY($1)",
        [mat_names]
      )

    Map.new(job_rows, fn [mat, offset] -> {Map.fetch!(mat_to_view, mat), offset} end)
  end

  # Aggregate retention windows (`drop_after`) for the samples_1m / samples_1h
  # continuous aggregates, sorted. `apply_policies` creates these via
  # `add_retention_policy('samples_1m'/'samples_1h', ...)`; the resulting
  # `policy_retention` jobs attach to each cagg's materialization hypertable
  # (under `_timescaledb_internal`), so they are discovered by materialization
  # hypertable name rather than by the test schema. Returning a sorted list lets
  # the test assert both the count (duplication/replacement) and the window.
  defp aggregate_retention_windows(conn, schema) do
    {:ok, %{rows: cagg_rows}} =
      Postgrex.query(
        conn,
        "SELECT materialization_hypertable_name " <>
          "FROM timescaledb_information.continuous_aggregates " <>
          "WHERE view_schema = $1 AND view_name IN ('samples_1m', 'samples_1h')",
        [schema]
      )

    mat_names = Enum.map(cagg_rows, &hd/1)

    {:ok, %{rows: job_rows}} =
      Postgrex.query(
        conn,
        "SELECT config->>'drop_after' FROM timescaledb_information.jobs " <>
          "WHERE proc_name = 'policy_retention' AND hypertable_name = ANY($1)",
        [mat_names]
      )

    job_rows |> Enum.map(&hd/1) |> Enum.sort()
  end
end

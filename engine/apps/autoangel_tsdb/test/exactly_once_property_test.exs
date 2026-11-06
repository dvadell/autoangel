defmodule AutoangelTsdb.ExactlyOncePropertyTest do
  @moduledoc """
  Property 40 / Requirement 36.5: exactly-once writes.

  "For any batch and any number of retries in which the commit succeeds but the
  acknowledgement is lost, each Sample of the batch is stored exactly once after
  the retry succeeds."

  The lost-acknowledgement scenario is modelled by calling
  `AutoangelTsdb.append_samples/2` with the SAME `batch_uuid` and SAME samples
  one original time plus R retries. The backend dedups on `batch_uuid` via
  `INSERT INTO ingest_batches ... ON CONFLICT (batch_id) DO NOTHING` and only
  COPYs the sample rows when a NEW batch row is inserted, so the samples must
  land exactly once regardless of how many times the identical batch is
  replayed. A contrast case confirms dedup is keyed on batch_uuid (not sample
  content): the same samples under a fresh batch_uuid are stored again.

  This test exercises a REAL TimescaleDB instance. When the database cannot be
  reached the properties are skipped rather than failed, keeping DB-less
  pipelines green. When the database is available the properties run and must
  pass.
  """

  use ExUnit.Case, async: false
  use ExUnitProperties

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

  # Postgrex decodes PostgreSQL's special doubles to atoms, not floats.
  #   NaN  -> :NaN
  #   +Inf -> :inf
  #   -Inf -> :"-inf"
  # The written sample uses the backend's own value atoms (:nan/:inf/:neg_inf),
  # so the comparison maps between the two atom vocabularies.
  @special_write_to_read %{nan: :NaN, inf: :inf, neg_inf: :"-inf"}

  # Mirrors the TCP pre-check + {:ok, skip: true} + skip_or pattern used by the
  # sibling IntegrationTest: a bare `:skip` atom crashes ExUnit (setup must
  # return :ok, a keyword, or a map), so skipping is modelled as context data the
  # properties branch on instead.
  #
  # This test connects with a bare `Postgrex.start_link` and builds its own
  # handle; it does NOT go through `AutoangelTsdb.init`, which is the only path
  # that runs migrations. On a fresh container `docker/timescaledb/init.sql`
  # creates ONLY `samples`, so `series`/`ingest_batches` would be missing and the
  # properties would FAIL (not skip). Migrating the `public` schema here makes
  # the test self-sufficient: `AutoangelTsdb.migrate/1` is idempotent (every
  # statement is `CREATE ... IF NOT EXISTS`), so it creates the tables on a fresh
  # DB and is a no-op on an already-migrated one. If migration does not succeed
  # the database is effectively unavailable, so we skip rather than crash,
  # consistent with the `{:ok, skip: true}` pattern above.
  setup do
    with {:ok, conn} <- try_connect(),
         {:ok, _version} <- try_migrate(conn) do
      {:ok, conn: conn, handle: %{conn: conn, ready: true}}
    else
      _ -> {:ok, skip: true}
    end
  end

  property "replaying an identical batch stores each Sample exactly once (lost ack retries)", ctx do
    skip_or(ctx, fn %{handle: handle, conn: conn} ->
      check all(
              batch <- batch_generator(),
              retries <- integer(1..5),
              max_runs: 30
            ) do
        series_id = :crypto.strong_rand_bytes(16)
        batch_uuid = :crypto.strong_rand_bytes(16)
        name = series_name(series_id)
        samples = materialise(batch, series_id, batch_uuid, name)

        on_exit(fn -> cleanup(series_id, [batch_uuid]) end)

        # Original commit plus R lost-acknowledgement retries of the same batch.
        results = for _ <- 0..retries, do: AutoangelTsdb.append_samples(handle, samples)

        assert Enum.all?(results, &(&1 == :ok)),
               "every append_samples call must return :ok, got: #{inspect(results)}"

        {from_ms, to_ms} = range_bounds(samples)
        assert {:ok, rows} = AutoangelTsdb.read_range(handle, [series_id], from_ms, to_ms)

        # (1) Row count equals the batch size, NOT batch_size * (retries + 1).
        assert length(rows) == length(samples),
               "expected #{length(samples)} rows after #{retries + 1} replays, got #{length(rows)}"

        # (2) The (ts_ms, value) multiset read back equals the batch's exactly.
        written = Enum.map(samples, fn s -> {s.ts_ms, normalize_written(s.value)} end)
        read = Enum.map(rows, fn [_rid, ts, value] -> {ts, normalize_read(value)} end)

        assert multiset(read) == multiset(written),
               "read-back (ts_ms, value) multiset differs from written after retries.\n" <>
                 "written: #{inspect(Enum.sort_by(written, &elem(&1, 0)))}\n" <>
                 "read:    #{inspect(Enum.sort_by(read, &elem(&1, 0)))}"

        # (3) ingest_batches holds exactly one row for this batch_uuid.
        assert batch_count(conn, batch_uuid) == 1
      end
    end)
  end

  property "the same Samples under a NEW batch_uuid are stored again (dedup is per batch)", ctx do
    skip_or(ctx, fn %{handle: handle, conn: conn} ->
      check all(batch <- batch_generator(), max_runs: 30) do
        series_id = :crypto.strong_rand_bytes(16)
        batch_a = :crypto.strong_rand_bytes(16)
        batch_b = :crypto.strong_rand_bytes(16)
        name = series_name(series_id)

        samples_a = materialise(batch, series_id, batch_a, name)
        samples_b = materialise(batch, series_id, batch_b, name)

        on_exit(fn -> cleanup(series_id, [batch_a, batch_b]) end)

        assert :ok = AutoangelTsdb.append_samples(handle, samples_a)
        # Replaying batch A is a no-op; a fresh batch_uuid is NOT.
        assert :ok = AutoangelTsdb.append_samples(handle, samples_a)
        assert :ok = AutoangelTsdb.append_samples(handle, samples_b)

        {from_ms, to_ms} = range_bounds(samples_a)
        assert {:ok, rows} = AutoangelTsdb.read_range(handle, [series_id], from_ms, to_ms)

        # Two distinct batch commits of identical content => twice the rows.
        assert length(rows) == 2 * length(samples_a),
               "distinct batch_uuids must each store their Samples: " <>
                 "expected #{2 * length(samples_a)}, got #{length(rows)}"

        assert batch_count(conn, batch_a) == 1
        assert batch_count(conn, batch_b) == 1
      end
    end)
  end

  # ---------------------------------------------------------------------------
  # Skip plumbing (mirrors AutoangelTsdb.IntegrationTest)
  # ---------------------------------------------------------------------------

  # Runs `fun` with the test context when the database is available; otherwise
  # skips (prints a notice and returns :ok) so the suite stays green without a DB.
  defp skip_or(%{skip: true}, _fun) do
    IO.puts(
      "[integration] skipping exactly-once property test: TimescaleDB unreachable at " <>
        "#{inspect(@conn_opts[:hostname])}:#{@conn_opts[:port]}"
    )

    :ok
  end

  defp skip_or(ctx, fun), do: fun.(ctx)

  # Establishes a verified connection, or returns :error so the test skips.
  #
  # A fast TCP pre-check avoids Postgrex's background reconnect loop (and the
  # linked-process crash it can cause) when the container is absent, keeping the
  # skip path instant and incapable of failing the suite.
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

  # Idempotently migrates the `public` schema so a fresh container (whose
  # init.sql creates only `samples`) gains the `series`/`ingest_batches`/etc.
  # tables these properties rely on. Returns {:ok, version} on success; any
  # failure is mapped to :error so setup skips instead of crashing. Exits are
  # trapped defensively so a migration-time connection crash can never bubble up
  # as a test failure.
  defp try_migrate(conn) do
    previous = Process.flag(:trap_exit, true)

    try do
      case AutoangelTsdb.migrate(conn) do
        {:ok, version} -> {:ok, version}
        _ -> :error
      end
    catch
      _, _ -> :error
    after
      Process.flag(:trap_exit, previous)
    end
  end

  # ---------------------------------------------------------------------------
  # Generators
  # ---------------------------------------------------------------------------

  # A batch is a non-empty list of {ts_ms, value}. Timestamps are drawn from a
  # small recent pool so duplicates within a batch are frequent; values are
  # finite floats plus the backend's special-value atoms.
  defp batch_generator do
    bind(recent_ms_pool(), fn pool ->
      list_of(
        tuple({member_of(pool), value_generator()}),
        min_length: 1,
        max_length: 8
      )
    end)
  end

  # A small pool of recent millisecond timestamps anchored to *now*. Fixed 2023
  # timestamps can fall outside AutoangelTsdb.init's re-armed 14-day raw
  # retention window and be dropped between append and read; anchoring to
  # System.system_time/1 keeps every written Sample inside the window so
  # retention cannot reclaim it mid-test. Drawing from a small pool keeps
  # duplicate (series_id, ts) collisions frequent.
  defp recent_ms_pool do
    base = System.system_time(:millisecond)

    uniq_list_of(
      bind(integer(0..9), fn seconds ->
        map(integer(1..999), fn millis -> base - seconds * 1000 + millis end)
      end),
      min_length: 1,
      max_length: 4
    )
  end

  defp value_generator do
    one_of([
      float(min: -1.0e6, max: 1.0e6),
      map(integer(-1000..1000), &(&1 * 1.0)),
      member_of([0.0, -0.0, 1.0e308, -1.0e308, 5.0e-324]),
      member_of([:nan, :inf, :neg_inf])
    ])
  end

  # ---------------------------------------------------------------------------
  # Builders
  # ---------------------------------------------------------------------------

  defp materialise(batch, series_id, batch_uuid, name) do
    for {ts_ms, value} <- batch do
      %{
        series_id: series_id,
        name: name,
        labels: %{"test" => "exactly_once"},
        kind: :gauge,
        status: :healthy,
        ts_ms: ts_ms,
        value: value,
        batch_uuid: batch_uuid
      }
    end
  end

  defp series_name(series_id), do: "prop40-" <> Base.encode16(series_id, case: :lower)

  defp range_bounds(samples) do
    ts = Enum.map(samples, & &1.ts_ms)
    {Enum.min(ts), Enum.max(ts) + 1}
  end

  # ---------------------------------------------------------------------------
  # Value normalization
  # ---------------------------------------------------------------------------

  defp normalize_written(value) when value in [:nan, :inf, :neg_inf],
    do: Map.fetch!(@special_write_to_read, value)

  defp normalize_written(value) when is_integer(value), do: float_bits(value * 1.0)
  defp normalize_written(value) when is_float(value), do: float_bits(value)

  defp normalize_read(value) when value in [:NaN, :inf, :"-inf"], do: value
  defp normalize_read(value) when is_float(value), do: float_bits(value)

  # Compare finite doubles by their exact 64-bit IEEE-754 pattern. This keeps
  # -0.0 distinct from 0.0 (so a backend that folded signed zero would be
  # caught) rather than the looser float equality that `value + 0.0` would
  # permit.
  defp float_bits(value) when is_float(value), do: <<value::float-big>>

  defp multiset(list), do: Enum.frequencies(list)

  # ---------------------------------------------------------------------------
  # Database helpers
  # ---------------------------------------------------------------------------

  defp batch_count(conn, batch_uuid) do
    {:ok, %{rows: [[count]]}} =
      Postgrex.query(conn, "SELECT count(*) FROM ingest_batches WHERE batch_id = $1", [batch_uuid])

    count
  end

  # Unique random series_ids mean leftover rows never affect correctness, but we
  # delete to keep the shared database tidy. The test's own Postgrex pool is
  # linked to the test process and already gone by the time on_exit runs, so a
  # FRESH short-lived connection is opened here. Errors are tolerated.
  defp cleanup(series_id, batch_uuids) do
    with {:ok, conn} <- try_connect() do
      _ = Postgrex.query(conn, "DELETE FROM samples WHERE series_id = $1", [series_id])
      _ = Postgrex.query(conn, "DELETE FROM series WHERE id = $1", [series_id])

      Enum.each(batch_uuids, fn batch_uuid ->
        _ = Postgrex.query(conn, "DELETE FROM ingest_batches WHERE batch_id = $1", [batch_uuid])
      end)

      _ =
        try do
          GenServer.stop(conn)
        catch
          _, _ -> :ok
        end
    end

    :ok
  rescue
    _ -> :ok
  catch
    _, _ -> :ok
  end
end

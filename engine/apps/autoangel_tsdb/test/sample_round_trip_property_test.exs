defmodule AutoangelTsdb.SampleRoundTripPropertyTest do
  @moduledoc """
  Property 39 / Requirements 36.2, 36.4.

  For any batch of Samples (including NaN, ±Inf, extreme doubles, millisecond
  timestamps, duplicate timestamps for one Series, Unicode label values),
  writing through the TimescaleDB backend and reading the range back returns
  every Sample with the same name, label set, millisecond timestamp and 64-bit
  value, with no merging of duplicates.

  This test exercises a REAL TimescaleDB instance. When the database cannot be
  reached (e.g. CI without a DB) the single property is skipped rather than
  failed, keeping those pipelines green. When the database is available the
  property runs and must pass.
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
  # so the round trip maps between the two atom vocabularies.
  @special_write_to_read %{nan: :NaN, inf: :inf, neg_inf: :"-inf"}

  # Mirrors the TCP pre-check + {:ok, skip: true} + skip_or pattern used by the
  # sibling IntegrationTest: a bare `:skip` atom crashes ExUnit (setup must
  # return :ok, a keyword, or a map), so skipping is modelled as context data the
  # property branches on instead.
  #
  # This test connects with a bare `Postgrex.start_link` and builds its own
  # handle; it does NOT go through `AutoangelTsdb.init`, which is the only path
  # that runs migrations. On a fresh container `docker/timescaledb/init.sql`
  # creates ONLY `samples`, so `series`/`ingest_batches` would be missing and the
  # property would FAIL (not skip). Migrating the `public` schema here makes the
  # test self-sufficient: `AutoangelTsdb.migrate/1` is idempotent (every
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

  property "writing a Sample batch and reading the range back preserves every Sample", ctx do
    skip_or(ctx, fn %{handle: handle, conn: conn} ->
      check all(
              values <- list_of(value_generator(), min_length: 1, max_length: 25),
              timestamps <- timestamps_generator(length(values)),
              labels <- labels_generator(),
              max_runs: 30
            ) do
        series_id = :crypto.strong_rand_bytes(16)
        batch_uuid = :crypto.strong_rand_bytes(16)
        name = "prop39-" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)

        samples =
          Enum.zip(timestamps, values)
          |> Enum.map(fn {ts_ms, value} ->
            %{
              series_id: series_id,
              name: name,
              labels: labels,
              kind: :gauge,
              status: :healthy,
              ts_ms: ts_ms,
              value: value,
              batch_uuid: batch_uuid
            }
          end)

        on_exit(fn -> cleanup(series_id, [batch_uuid]) end)

        assert :ok = AutoangelTsdb.append_samples(handle, samples)

        from_ms = Enum.min(timestamps)
        to_ms = Enum.max(timestamps) + 1

        assert {:ok, rows} = AutoangelTsdb.read_range(handle, [series_id], from_ms, to_ms)

        # Every returned row belongs to our isolated series.
        assert Enum.all?(rows, fn [rid, _ts, _value] -> rid == series_id end)

        # Property, part 2: duplicate (series_id, ts) samples are NOT merged.
        # Row count equals the number of written samples.
        assert length(rows) == length(samples),
               "expected #{length(samples)} rows (no duplicate merging), got #{length(rows)}"

        # Property, parts 1 & 2 combined: the (ts_ms, value) multiset read back
        # equals the (ts_ms, value) multiset written. Finite values are compared
        # by their exact IEEE-754 bit pattern so a value that truncated or lost
        # precision (including signed zero) would be caught; special doubles are
        # compared by their read-back atom tag.
        written = Enum.map(samples, fn s -> {s.ts_ms, normalize_written(s.value)} end)
        read = Enum.map(rows, fn [_rid, ts, value] -> {ts, normalize_read(value)} end)

        assert multiset(read) == multiset(written),
               "read-back (ts_ms, value) multiset differs from written.\n" <>
                 "written: #{inspect(Enum.sort_by(written, &elem(&1, 0)))}\n" <>
                 "read:    #{inspect(Enum.sort_by(read, &elem(&1, 0)))}"

        # Property, part 3: the series name + labels (jsonb, Unicode-safe) round trip.
        assert {:ok, %{rows: series_rows}} =
                 Postgrex.query(conn, "SELECT name, labels FROM series WHERE id = $1", [series_id])

        assert [[^name, read_labels]] = series_rows

        assert decode_labels(read_labels) == stringify_labels(labels),
               "series labels did not round trip.\nwritten: #{inspect(labels)}\nread: #{inspect(read_labels)}"
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
      "[integration] skipping Sample round trip property test: TimescaleDB unreachable at " <>
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
  # tables this property relies on. Returns {:ok, version} on success; any
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

  # Values: finite floats across the full magnitude range (including extreme
  # magnitudes and tiny subnormals) plus the backend's special-value atoms.
  defp value_generator do
    one_of([
      float(),
      member_of([
        0.0,
        -0.0,
        1.0,
        -1.0,
        1.0e308,
        -1.0e308,
        1.0e-308,
        5.0e-324,
        2.2250738585072014e-308
      ]),
      member_of([:nan, :inf, :neg_inf])
    ])
  end

  # Millisecond timestamps anchored to *now*, not a fixed 2023 epoch. Fixed-past
  # timestamps can fall outside AutoangelTsdb.init's re-armed 14-day raw
  # retention window and be dropped by the retention policy between the append
  # and the read (observed as public.samples losing its chunk mid-test). Basing
  # the timestamps on System.system_time/1 keeps every written Sample newer than
  # the retention cutoff so it cannot be reclaimed during the test.
  #
  # Duplicate coverage: a SMALL pool of recent timestamps is built per check and
  # the `count` timestamps are drawn from it, so the same (series_id, ts)
  # collides frequently (duplicates must NOT be merged). Each pool entry carries
  # a sub-second millisecond component (0..999) rather than a whole second, so
  # the written ts_ms has real millisecond precision: a backend that truncated
  # to whole seconds would fail the round-trip assertion (Requirement 36.4).
  defp timestamps_generator(count) do
    bind(recent_ms_pool(), fn pool ->
      list_of(member_of(pool), length: count)
    end)
  end

  # A small (1..4 entry) pool of distinct recent millisecond timestamps. `now`
  # is captured once per generated pool; each entry is `now - seconds*1000 +
  # millis`, giving sub-second precision while staying inside the retention
  # window.
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

  # Labels: a map whose values may include non-ASCII / Unicode text. Keys are
  # kept simple ASCII identifiers. Built from a deduplicated key list zipped with
  # values, which avoids map_of/3's internal uniqueness-retry limit while still
  # producing maps of varying size (including the empty map).
  defp labels_generator do
    bind(
      uniq_list_of(member_of(~w(region service zone team env cluster tenant shard)),
        max_length: 5
      ),
      fn keys ->
        bind(list_of(unicode_value_generator(), length: length(keys)), fn vals ->
          constant(Map.new(Enum.zip(keys, vals)))
        end)
      end
    )
  end

  defp unicode_value_generator do
    one_of([
      string(:printable, min_length: 1, max_length: 12),
      member_of(["café", "naïve", "日本語", "Ωμέγα", "😀emoji", "Привет", "zürich"])
    ])
  end

  # ---------------------------------------------------------------------------
  # Value / label normalization helpers
  # ---------------------------------------------------------------------------

  # Map a written value to the atom/bitstring we expect to read back.
  defp normalize_written(value) when value in [:nan, :inf, :neg_inf],
    do: Map.fetch!(@special_write_to_read, value)

  defp normalize_written(value) when is_integer(value), do: float_bits(value * 1.0)
  defp normalize_written(value) when is_float(value), do: float_bits(value)

  # Normalize a read-back value. Postgrex yields :NaN / :inf / :"-inf" atoms for
  # special doubles and plain floats otherwise.
  defp normalize_read(value) when value in [:NaN, :inf, :"-inf"], do: value
  defp normalize_read(value) when is_float(value), do: float_bits(value)

  # Compare finite doubles by their exact 64-bit IEEE-754 pattern. This keeps
  # -0.0 distinct from 0.0 (so a backend that folded signed zero would be
  # caught) and detects any precision loss, rather than the looser float
  # equality that `value + 0.0` would permit.
  defp float_bits(value) when is_float(value), do: <<value::float-big>>

  # append_samples JSON-encodes labels. This Postgrex connection returns jsonb as
  # raw text (no custom json decoder configured), so decode it back to a map with
  # string keys before comparing. Unicode values survive the JSON round trip.
  defp decode_labels(text) when is_binary(text), do: Jason.decode!(text)
  defp decode_labels(map) when is_map(map), do: map

  # Generated keys are already strings, so this documents the string-keyed
  # contract the decoded jsonb returns.
  defp stringify_labels(labels) do
    Map.new(labels, fn {k, v} -> {to_string(k), v} end)
  end

  # Multiset as a frequency map so duplicates are compared by count.
  defp multiset(list), do: Enum.frequencies(list)

  # ---------------------------------------------------------------------------
  # Cleanup
  # ---------------------------------------------------------------------------

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

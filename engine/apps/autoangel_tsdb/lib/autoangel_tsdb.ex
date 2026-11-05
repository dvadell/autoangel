defmodule AutoangelTsdb do
  @moduledoc "TimescaleDB storage backend and schema owner for the Engine."
  @behaviour Autoangel.Storage

  alias AutoangelTsdb.Migrations

  @copy_header <<"PGCOPY\n", 0xFF, "\r\n\0", 0::signed-big-32, 0::signed-big-32>>
  @copy_trailer <<-1::signed-big-16>>
  @pg_epoch_us 946_684_800_000_000

  @type row :: {binary(), integer(), float()}
  @type handle :: %{conn: Postgrex.conn(), ready: boolean()}

  @impl true
  def init(config) when is_map(config) do
    with {:ok, conn} <- connection(config),
         :ok <- Migrations.run(conn, Map.get(config, :migration_options, [])),
         {:ok, _policy_results} <- AutoangelTsdb.Retention.apply_policies(conn, config) do
      {:ok, %{conn: conn, ready: true}}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  def init(config) when is_list(config), do: init(Map.new(config))

  @impl true
  def append_samples(%{conn: conn, ready: true}, samples) when is_list(samples) do
    write_samples(conn, samples)
  end

  def append_samples(%{ready: false}, _samples), do: {:retryable, :migrations_pending}
  def append_samples(conn, samples) when is_pid(conn), do: write_samples(conn, samples)

  @impl true
  def write_series(handle, rows), do: with_conn(handle, fn conn -> upsert_series(conn, rows) end)

  @impl true
  def write_anomalies(handle, rows),
    do: with_conn(handle, fn conn -> write_anomaly_rows(conn, rows) end)

  @impl true
  def write_bands(handle, rows), do: with_conn(handle, fn conn -> write_band_rows(conn, rows) end)

  @impl true
  def read_range(handle, series_ids, from, to) do
    with_conn(handle, fn conn ->
      sql =
        "SELECT series_id, (extract(epoch from ts) * 1000)::bigint, value FROM samples WHERE series_id = ANY($1) AND ts >= $2 AND ts < $3 ORDER BY ts"

      query_read(conn, sql, [series_ids, timestamp(from), timestamp(to)])
    end)
  end

  @impl true
  def read_aggregates(handle, series_ids, from, to, resolution)
      when resolution in [:minute, :hour] do
    with_conn(handle, fn conn ->
      table = if resolution == :minute, do: "samples_1m", else: "samples_1h"

      sql =
        "SELECT series_id, (extract(epoch from bucket) * 1000)::bigint, minimum, maximum, mean, sample_count FROM #{table} WHERE series_id = ANY($1) AND bucket >= $2 AND bucket < $3 ORDER BY bucket"

      query_read(conn, sql, [series_ids, timestamp(from), timestamp(to)])
    end)
  end

  @doc "Runs pending migrations and returns the resulting schema version."
  def migrate(conn, opts \\ []) do
    case Migrations.run(conn, opts) do
      :ok -> {:ok, Migrations.version()}
      error -> error
    end
  end

  @doc "Encodes rows using PostgreSQL's binary COPY format."
  @spec copy_binary([row()]) :: binary()
  def copy_binary(rows),
    do: IO.iodata_to_binary([@copy_header, Enum.map(rows, &encode_row/1), @copy_trailer])

  @doc false
  def benchmark_rows(series_count, samples_per_series) do
    base_ts = DateTime.utc_now() |> DateTime.to_unix(:microsecond) |> Kernel.-(@pg_epoch_us)

    for series <- 1..series_count, sample <- 1..samples_per_series do
      interval = 10 + rem(series * 17, 51)

      {<<series::unsigned-big-128>>, base_ts + sample * interval * 1_000_000,
       series * 1.0 + sample / 10}
    end
  end

  @doc "Writes with COPY and falls back to one parameterised multi-row INSERT."
  def write_batch(conn, rows, opts \\ []) when is_list(rows) do
    if Keyword.get(opts, :force_fallback, false),
      do: insert_write(conn, rows),
      else: copy_or_fallback(conn, rows, opts)
  end

  defp connection(%{conn: conn}) when is_pid(conn), do: {:ok, conn}
  defp connection(%{"conn" => conn}) when is_pid(conn), do: {:ok, conn}

  defp connection(config) do
    opts = Map.get(config, :connection, Map.get(config, "connection", config)) |> normalize_opts()
    Postgrex.start_link(opts)
  end

  defp normalize_opts(opts) when is_binary(opts) do
    uri = URI.parse(opts)

    options = [
      hostname: uri.host || "localhost",
      port: uri.port || 5432,
      database: database(uri.path)
    ]

    case uri.userinfo do
      nil ->
        options

      userinfo ->
        case String.split(userinfo, ":", parts: 2) do
          [username, password] ->
            Keyword.merge(options, username: URI.decode(username), password: URI.decode(password))

          [username] ->
            Keyword.put(options, :username, URI.decode(username))
        end
    end
  end

  defp normalize_opts(opts) when is_list(opts), do: opts
  defp normalize_opts(opts) when is_map(opts), do: Map.to_list(opts)

  defp database(nil), do: "postgres"
  defp database(path), do: String.trim_leading(path, "/")

  defp write_samples(_conn, []), do: :ok

  defp write_samples(conn, samples) do
    batch_uuid = batch_uuid(samples)
    rows = Enum.map(samples, &sample_row/1)
    series = Enum.map(samples, &series_row/1) |> Enum.uniq_by(&elem(&1, 0))

    case Postgrex.transaction(conn, fn tx -> write_transaction(tx, batch_uuid, rows, series) end) do
      {:ok, :ok} -> :ok
      {:error, reason} -> classify(reason)
    end
  rescue
    exception in [ArgumentError, Jason.EncodeError] -> {:permanent, exception}
    exception -> {:retryable, exception}
  end

  defp write_transaction(tx, batch_uuid, rows, series) do
    with :ok <- upsert_series(tx, series),
         {:ok, %{num_rows: rows_inserted}} <-
           Postgrex.query(
             tx,
             "INSERT INTO ingest_batches (batch_id) VALUES ($1) ON CONFLICT (batch_id) DO NOTHING",
             [batch_uuid]
           ),
         :ok <- if(rows_inserted == 1, do: copy_write(tx, rows), else: :ok) do
      :ok
    else
      {:error, reason} -> Postgrex.rollback(tx, reason)
    end
  end

  defp upsert_series(_conn, []), do: :ok

  defp upsert_series(conn, rows) do
    Enum.reduce_while(rows, :ok, fn {id, name, labels, kind, status}, :ok ->
      sql =
        "INSERT INTO series (id, name, labels, kind, status) VALUES ($1, $2, $3::jsonb, $4, $5) ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, labels = EXCLUDED.labels, kind = EXCLUDED.kind, status = EXCLUDED.status"

      case Postgrex.query(conn, sql, [
             id,
             name,
             Jason.encode!(labels),
             to_string(kind),
             to_string(status)
           ]) do
        {:ok, _} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp write_anomaly_rows(_conn, []), do: :ok

  defp write_anomaly_rows(conn, rows) do
    Enum.reduce_while(rows, :ok, fn row, :ok ->
      values = anomaly_values(row)

      sql =
        "INSERT INTO anomalies (id, series_id, start_time, end_time, score, observed, expected_lower, expected_upper, baseline_type, status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT (id) DO UPDATE SET end_time=EXCLUDED.end_time, score=EXCLUDED.score, status=EXCLUDED.status"

      case Postgrex.query(conn, sql, values) do
        {:ok, _} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp write_band_rows(_conn, []), do: :ok

  defp write_band_rows(conn, rows) do
    Enum.reduce_while(rows, :ok, fn row, :ok ->
      {series_id, ts, lower, upper, type} = band_values(row)

      sql =
        "INSERT INTO baseline_bands (series_id, ts, lower_bound, upper_bound, baseline_type) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (series_id, ts) DO UPDATE SET lower_bound=EXCLUDED.lower_bound, upper_bound=EXCLUDED.upper_bound, baseline_type=EXCLUDED.baseline_type"

      case Postgrex.query(conn, sql, [series_id, timestamp(ts), lower, upper, type]) do
        {:ok, _} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp copy_write(conn, rows) do
    stream = Postgrex.stream(conn, "COPY samples (series_id, ts, value) FROM STDIN BINARY", [])
    _ = Enum.into([copy_binary(rows)], stream)
    :ok
  rescue
    exception -> {:error, exception}
  end

  defp copy_or_fallback(conn, rows, opts) do
    sql = Keyword.get(opts, :copy_sql, "COPY samples (series_id, ts, value) FROM STDIN BINARY")
    payload = copy_binary(rows)

    case Postgrex.transaction(conn, fn tx ->
           stream = Postgrex.stream(tx, sql, [])
           Enum.into([payload], stream)
         end) do
      {:ok, _} ->
        {:ok, %{method: :copy, round_trips: 1, rows: length(rows)}}

      {:error, reason} ->
        case insert_write(conn, rows) do
          {:ok, result} -> {:ok, Map.put(result, :copy_fallback_reason, reason)}
          error -> error
        end
    end
  rescue
    exception -> {:error, exception}
  end

  defp insert_write(conn, rows) do
    {values, params} =
      rows
      |> Enum.with_index(1)
      |> Enum.map_reduce([], fn {{id, ts, value}, i}, acc ->
        {"($#{i * 3 - 2},$#{i * 3 - 1},$#{i * 3})",
         [id, DateTime.from_unix!(ts + @pg_epoch_us, :microsecond), value | acc]}
      end)

    case Postgrex.query(
           conn,
           "INSERT INTO samples (series_id, ts, value) VALUES #{Enum.join(values, ",")}",
           Enum.reverse(params)
         ) do
      {:ok, _} -> {:ok, %{method: :insert, round_trips: 1, rows: length(rows)}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp query_read(conn, sql, params) do
    case Postgrex.query(conn, sql, params) do
      {:ok, %{rows: rows}} -> {:ok, rows}
      {:error, reason} -> classify(reason)
    end
  end

  defp with_conn(%{conn: conn, ready: true}, fun), do: fun.(conn)
  defp with_conn(%{ready: false}, _fun), do: {:retryable, :migrations_pending}
  defp with_conn(conn, fun) when is_pid(conn), do: fun.(conn)
  defp with_conn(_, _fun), do: {:permanent, :invalid_handle}

  defp classify(reason), do: {:retryable, reason}
  defp timestamp(%DateTime{} = value), do: value
  defp timestamp(ms) when is_integer(ms), do: DateTime.from_unix!(ms, :millisecond)

  defp batch_uuid([%{batch_uuid: uuid} | _]), do: uuid
  defp batch_uuid([%{"batch_uuid" => uuid} | _]), do: uuid
  defp batch_uuid(_), do: :crypto.strong_rand_bytes(16)

  defp sample_row(%{series_id: id, ts_ms: ts, value: value}),
    do: {id, ts * 1000 - @pg_epoch_us, value}

  defp sample_row(%{series_id: id, timestamp: ts, value: value}),
    do: {id, ts * 1000 - @pg_epoch_us, value}

  defp sample_row({id, ts, value}), do: {id, ts * 1000 - @pg_epoch_us, value}

  defp series_row(%{series_id: id, name: name, labels: labels, kind: kind, status: status}),
    do: {id, name, labels, kind, status}

  defp series_row(%{id: id, name: name, labels: labels, kind: kind, status: status}),
    do: {id, name, labels, kind, status}

  defp series_row(_), do: raise(ArgumentError, "sample must include series metadata")

  defp anomaly_values(row),
    do: [
      row.id,
      row.series_id,
      timestamp(row.start_time),
      timestamp_or_nil(row.end_time),
      row.score,
      row.observed,
      row.lower,
      row.upper,
      to_string(row.baseline_type),
      to_string(row.status)
    ]

  defp band_values(row),
    do: {row.series_id, row.ts, row.lower, row.upper, to_string(row.baseline_type)}

  defp timestamp_or_nil(nil), do: nil
  defp timestamp_or_nil(value), do: timestamp(value)

  defp encode_row({id, ts, value}) when byte_size(id) == 16 do
    [
      <<3::signed-big-16>>,
      <<16::signed-big-32, id::binary>>,
      <<8::signed-big-32, ts::signed-big-64>>,
      <<8::signed-big-32, encode_value(value)::unsigned-big-64>>
    ]
  end

  defp encode_value(:nan), do: 0x7FF8_0000_0000_0000
  defp encode_value(:inf), do: 0x7FF0_0000_0000_0000
  defp encode_value(:neg_inf), do: 0xFFF0_0000_0000_0000
  defp encode_value(value) when is_integer(value), do: encode_value(value * 1.0)

  defp encode_value(value) when is_float(value),
    do: <<value::float-big>> |> :binary.decode_unsigned()

  @doc "Applies configured retention and refresh policies."
  def apply_retention_policies(connection, config, opts \\ []),
    do: AutoangelTsdb.Retention.apply_policies(connection, config, opts)

  @doc "Removes ended anomaly records past the configured cutoff."
  def cleanup_anomalies(connection, retention_seconds, opts \\ []),
    do: AutoangelTsdb.Retention.cleanup_anomalies(connection, retention_seconds, opts)
end

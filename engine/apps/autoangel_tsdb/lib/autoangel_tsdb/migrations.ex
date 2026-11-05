defmodule AutoangelTsdb.Migrations do
  @moduledoc "Idempotent, versioned SQL migrations owned by the Engine."

  @schema_version 1

  @doc "The schema contract version supported by this backend."
  @spec version() :: 1
  def version, do: @schema_version

  @doc "Runs every pending migration. Each version is committed only after success."
  @spec run(Postgrex.conn(), keyword()) :: :ok | {:error, term()}
  def run(conn, opts \\ []) do
    with :ok <- ensure_version_table(conn),
         {:ok, current} <- current_version(conn),
         :ok <- reject_newer(current) do
      apply_pending(conn, current, opts)
    end
  rescue
    exception -> {:error, exception}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp ensure_version_table(conn) do
    query = """
    CREATE TABLE IF NOT EXISTS schema_version (
      id boolean PRIMARY KEY DEFAULT true CHECK (id),
      version integer NOT NULL
    )
    """

    case Postgrex.query(conn, query, []) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, {:version_table, reason}}
    end
  end

  defp current_version(conn) do
    case Postgrex.query(conn, "SELECT version FROM schema_version WHERE id = true", []) do
      {:ok, %{rows: []}} -> {:ok, 0}
      {:ok, %{rows: [[version]]}} -> {:ok, version}
      {:error, reason} -> {:error, {:read_version, reason}}
    end
  end

  defp reject_newer(current) when current <= @schema_version, do: :ok
  defp reject_newer(current), do: {:error, {:unsupported_schema_version, current}}

  defp apply_pending(conn, current, opts) do
    Enum.reduce_while(
      if(current < @schema_version, do: (current + 1)..@schema_version, else: []),
      :ok,
      fn version, :ok ->
        case apply_one(conn, version, opts) do
          :ok -> {:cont, :ok}
          {:error, reason} -> {:halt, {:error, {:migration_failed, version, reason}}}
        end
      end
    )
  end

  defp apply_one(conn, 1, opts) do
    Postgrex.transaction(conn, fn tx ->
      with :ok <- query_ok(tx, "CREATE EXTENSION IF NOT EXISTS timescaledb", opts),
           :ok <- query_ok(tx, series_sql(), opts),
           :ok <- query_ok(tx, samples_sql(), opts),
           :ok <- query_ok(tx, batches_sql(), opts),
           :ok <- query_ok(tx, anomalies_sql(), opts),
           :ok <- query_ok(tx, status_sql(), opts),
           :ok <- query_ok(tx, insert_version_sql(1), opts) do
        :ok
      else
        {:error, reason} -> Postgrex.rollback(tx, reason)
      end
    end)
    |> transaction_result()
  end

  defp query_ok(conn, sql, opts) do
    case Postgrex.query(conn, sql, [], timeout: Keyword.get(opts, :timeout, 15_000)) do
      {:ok, _} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp transaction_result({:ok, :ok}), do: :ok
  defp transaction_result({:error, reason}), do: {:error, reason}

  defp series_sql do
    """
    CREATE TABLE IF NOT EXISTS series (
      id uuid PRIMARY KEY,
      name text NOT NULL,
      labels jsonb NOT NULL DEFAULT '{}'::jsonb,
      kind text NOT NULL CHECK (kind IN ('counter', 'gauge')),
      status text NOT NULL CHECK (status IN ('learning', 'healthy', 'anomalous', 'silent')),
      inserted_at timestamptz NOT NULL DEFAULT now(),
      UNIQUE (name, labels)
    )
    """
  end

  defp samples_sql do
    """
    CREATE TABLE IF NOT EXISTS samples (
      series_id uuid NOT NULL REFERENCES series(id) ON DELETE CASCADE,
      ts timestamptz NOT NULL,
      value double precision NOT NULL
    );
    SELECT create_hypertable('samples', 'ts', if_not_exists => TRUE);
    CREATE INDEX IF NOT EXISTS samples_series_ts_idx ON samples (series_id, ts DESC)
    """
  end

  defp batches_sql do
    """
    CREATE TABLE IF NOT EXISTS ingest_batches (
      batch_id uuid PRIMARY KEY,
      written_at timestamptz NOT NULL DEFAULT now()
    )
    """
  end

  defp anomalies_sql do
    """
    CREATE TABLE IF NOT EXISTS anomalies (
      id uuid PRIMARY KEY,
      series_id uuid NOT NULL REFERENCES series(id) ON DELETE CASCADE,
      start_time timestamptz NOT NULL,
      end_time timestamptz,
      score double precision,
      observed double precision,
      expected_lower double precision,
      expected_upper double precision,
      baseline_type text,
      status text NOT NULL,
      inserted_at timestamptz NOT NULL DEFAULT now()
    );
    CREATE TABLE IF NOT EXISTS anomaly_candidates (
      anomaly_id uuid NOT NULL REFERENCES anomalies(id) ON DELETE CASCADE,
      series_id uuid NOT NULL REFERENCES series(id) ON DELETE CASCADE,
      rank integer NOT NULL,
      evidence double precision NOT NULL,
      PRIMARY KEY (anomaly_id, series_id)
    );
    CREATE INDEX IF NOT EXISTS anomalies_series_start_idx ON anomalies (series_id, start_time DESC)
    """
  end

  defp status_sql do
    """
    CREATE TABLE IF NOT EXISTS engine_status (
      heartbeat_at timestamptz NOT NULL,
      engine_version text NOT NULL
    );
    CREATE TABLE IF NOT EXISTS baseline_bands (
      series_id uuid NOT NULL REFERENCES series(id) ON DELETE CASCADE,
      ts timestamptz NOT NULL,
      lower_bound double precision,
      upper_bound double precision,
      baseline_type text,
      PRIMARY KEY (series_id, ts)
    );
    SELECT create_hypertable('baseline_bands', 'ts', if_not_exists => TRUE);
    CREATE MATERIALIZED VIEW IF NOT EXISTS samples_1m WITH (timescaledb.continuous) AS
      SELECT series_id, time_bucket(INTERVAL '1 minute', ts) AS bucket,
             min(value) FILTER (WHERE value NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)) AS minimum,
             max(value) FILTER (WHERE value NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)) AS maximum,
             avg(value) FILTER (WHERE value NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)) AS mean,
             count(value) FILTER (WHERE value NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)) AS sample_count
      FROM samples GROUP BY series_id, bucket;
    CREATE MATERIALIZED VIEW IF NOT EXISTS samples_1h WITH (timescaledb.continuous) AS
      SELECT series_id, time_bucket(INTERVAL '1 hour', ts) AS bucket,
             min(value) FILTER (WHERE value NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)) AS minimum,
             max(value) FILTER (WHERE value NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)) AS maximum,
             avg(value) FILTER (WHERE value NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)) AS mean,
             count(value) FILTER (WHERE value NOT IN ('NaN'::double precision, 'Infinity'::double precision, '-Infinity'::double precision)) AS sample_count
      FROM samples GROUP BY series_id, bucket;
    CREATE MATERIALIZED VIEW IF NOT EXISTS baseline_bands_1h WITH (timescaledb.continuous) AS
      SELECT series_id, time_bucket(INTERVAL '1 hour', ts) AS bucket,
             min(lower_bound) AS minimum, max(upper_bound) AS maximum,
             avg(lower_bound) AS mean, count(*) AS sample_count
      FROM baseline_bands GROUP BY series_id, bucket;
    """
  end

  defp insert_version_sql(version),
    do:
      "INSERT INTO schema_version (id, version) VALUES (true, #{version}) ON CONFLICT (id) DO UPDATE SET version = EXCLUDED.version"
end

defmodule Mix.Tasks.Autoangel.BenchmarkDisk do
  @moduledoc """
  Disk per Series-day, with compression off and on (Requirements 72.1 to 72.3).

      AUTOANGEL_TSDB_URL=ecto://user:pass@host:port/db \\
        mix autoangel.benchmark_disk [--series 300] [--interval 60] [--days 1] [--no-record]

  Writes `--series` Series for `--days` days of Samples at `--interval` seconds into
  a schema of its own, measures the raw `samples` hypertable (indexes included),
  compresses every chunk and measures again, then appends one block to
  `docs/decisions.md` and drops the schema.
  """
  @shortdoc "Benchmark disk use per Series-day"
  use Mix.Task

  @batch 5_000

  @impl Mix.Task
  def run(args) do
    {opts, _rest, _invalid} =
      OptionParser.parse(args,
        strict: [series: :integer, interval: :integer, days: :integer, record: :boolean]
      )

    series = Keyword.get(opts, :series, 300)
    interval = Keyword.get(opts, :interval, 60)
    days = Keyword.get(opts, :days, 1)
    Mix.Task.run("app.start", [])

    url = System.get_env("AUTOANGEL_TSDB_URL") || Mix.raise("set AUTOANGEL_TSDB_URL")
    uri = URI.parse(url)
    [user, password] = String.split(uri.userinfo || "autoangel:autoangel", ":", parts: 2)

    base = [
      hostname: uri.host,
      port: uri.port || 5432,
      username: user,
      password: password,
      database: String.trim_leading(uri.path || "/autoangel", "/"),
      timeout: 600_000
    ]

    schema = "aa_disk_#{System.unique_integer([:positive])}"
    {:ok, admin} = Postgrex.start_link(base)
    Postgrex.query!(admin, "CREATE SCHEMA #{schema}", [])
    {:ok, conn} = Postgrex.start_link(base ++ [parameters: [search_path: "#{schema},public"]])
    :ok = AutoangelTsdb.Migrations.run(conn)

    try do
      rows = write(conn, series, interval, days)
      Postgrex.query!(conn, "ANALYZE samples", [])
      raw = size(conn)

      # The same set-up and policy the Engine applies, without waiting for its schedule.
      :ok = AutoangelTsdb.Compression.apply_policy(conn, %{})

      %{rows: [[compressed_chunks]]} =
        Postgrex.query!(
          conn,
          "SELECT count(compress_chunk(c, if_not_compressed => true)) FROM show_chunks('samples') c",
          []
        )

      compressed = size(conn)
      block = report(series, interval, days, rows, raw, compressed, compressed_chunks)
      IO.puts(block)
      if Keyword.get(opts, :record, true), do: record(block)
    after
      Postgrex.query(admin, "DROP SCHEMA #{schema} CASCADE", [])
    end
  end

  defp write(conn, series, interval, days) do
    now = System.system_time(:millisecond)
    per_series = div(days * 86_400, interval)
    ids = for _ <- 1..series, do: :crypto.strong_rand_bytes(16)

    :rand.seed(:exsss, {1, 2, 3})

    for {id, index} <- Enum.with_index(ids),
        chunk <- Enum.chunk_every(0..(per_series - 1), @batch) do
      samples =
        for i <- chunk do
          %{
            series_id: id,
            name: "disk_metric",
            labels: %{"series" => Integer.to_string(index)},
            kind: :gauge,
            status: :healthy,
            ts_ms: now - (per_series - i) * interval * 1_000,
            value: 100.0 + :math.sin(i / 50) * 10 + :rand.uniform()
          }
        end

      :ok = AutoangelTsdb.append_samples(conn, samples)
    end

    series * per_series
  end

  defp size(conn) do
    %{rows: [[bytes]]} = Postgrex.query!(conn, "SELECT hypertable_size('samples')", [])
    bytes
  end

  defp report(series, interval, days, rows, raw, compressed, chunks) do
    series_days = series * days
    per_day = fn bytes -> Float.round(bytes / series_days / 1_024, 1) end

    """
    ### Disk benchmark (#{DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()})

    - Conditions: TimescaleDB 2.17 (PostgreSQL 16) in a container, #{series} Series, one Sample per #{interval} s for #{days} day(s), #{rows} rows, the `samples` hypertable with its index, #{chunks} chunk(s) compressed (segment by Series, order by time).
    - Disk per Series-day, compression off: #{per_day.(raw)} KiB (#{Float.round(raw / rows, 1)} bytes per Sample).
    - Disk per Series-day, compression on: #{per_day.(compressed)} KiB (#{Float.round(compressed / rows, 1)} bytes per Sample), #{Float.round(raw / compressed, 1)} times smaller.
    """
  end

  defp record(block) do
    decisions = Path.expand("../../../../../../docs/decisions.md", __DIR__)
    document = File.read!(decisions)
    heading = "## Scale and footprint"

    updated =
      if String.contains?(document, heading),
        do:
          String.replace(document, heading, "#{heading}\n\n#{String.trim_trailing(block)}",
            global: false
          ),
        else: document <> "\n#{heading}\n\n#{block}"

    File.write!(decisions, updated)
  end
end

defmodule Mix.Tasks.Autoangel.BenchmarkTimescaledb do
  @moduledoc "Benchmarks the TimescaleDB COPY prototype and INSERT fallback."
  @shortdoc "Benchmark TimescaleDB sample writes"
  use Mix.Task

  @series_count 20_000
  @samples_per_series 6
  @batch_size 5_000

  @impl Mix.Task
  def run(args) do
    {opts, _args, _invalid} = OptionParser.parse(args, strict: [fallback: :boolean])
    Mix.Task.run("app.start", [])

    with {:ok, url} <- database_url(),
         {:ok, conn} <- Postgrex.start_link(parse_url(url)) do
      rows = AutoangelTsdb.benchmark_rows(@series_count, @samples_per_series)
      method = if Keyword.get(opts, :fallback, false), do: :insert, else: :copy
      started = System.monotonic_time()
      result = write_batches(conn, rows, method)

      elapsed_us =
        System.convert_time_unit(System.monotonic_time() - started, :native, :microsecond)

      GenServer.stop(conn)

      case result do
        {:ok, round_trips, methods} ->
          report(rows, elapsed_us, round_trips, methods)

        {:error, reason} ->
          Mix.raise("TimescaleDB benchmark failed: #{Exception.format(:error, reason)}")
      end
    else
      {:error, :missing_url} ->
        Mix.raise("set AUTOANGEL_TSDB_URL to run the TimescaleDB benchmark")

      {:error, reason} ->
        Mix.raise("could not connect to TimescaleDB: #{inspect(reason)}")
    end
  end

  defp database_url do
    case System.get_env("AUTOANGEL_TSDB_URL") do
      url when is_binary(url) and url != "" -> {:ok, url}
      _ -> {:error, :missing_url}
    end
  end

  defp parse_url(url) do
    uri = URI.parse(url)

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
            options
            |> Keyword.put(:username, URI.decode(username))
            |> Keyword.put(:password, URI.decode(password))

          [username] ->
            Keyword.put(options, :username, URI.decode(username))
        end
    end
  end

  defp database(nil), do: "postgres"
  defp database(path), do: String.trim_leading(path, "/")

  defp write_batches(conn, rows, method) do
    rows
    |> Enum.chunk_every(@batch_size)
    |> Enum.reduce_while({:ok, 0, []}, fn batch, {:ok, round_trips, methods} ->
      options = if method == :insert, do: [force_fallback: true], else: []

      case AutoangelTsdb.write_batch(conn, batch, options) do
        {:ok, result} ->
          {:cont, {:ok, round_trips + result.round_trips, [result.method | methods]}}

        error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, round_trips, methods} -> {:ok, round_trips, Enum.uniq(methods)}
      error -> error
    end
  end

  defp report(rows, elapsed_us, round_trips, methods) do
    samples_per_second = length(rows) / max(elapsed_us / 1_000_000, 0.000001)
    memory = machine_memory()

    cpu_count =
      case :erlang.system_info(:logical_processors) do
        :unknown -> System.schedulers_online()
        count -> count
      end

    disk = disk_type()
    batches = div(length(rows) + @batch_size - 1, @batch_size)
    method = methods |> Enum.map(&Atom.to_string/1) |> Enum.sort() |> Enum.join("+")

    line =
      "- TimescaleDB #{method} (#{@series_count} Series, 10 s to 60 s intervals, #{length(rows)} Samples): " <>
        "#{Float.round(samples_per_second, 2)} Samples/s, #{Float.round(round_trips / batches, 2)} " <>
        "client write round trips/batch (#{round_trips} total), #{cpu_count} logical CPUs, " <>
        "#{memory}, #{disk} disk, #{Float.round(elapsed_us / 1_000_000, 3)} s elapsed."

    decisions = Path.expand("../../../../../../docs/decisions.md", __DIR__)
    document = File.read!(decisions)
    marker = "## Storage gate"
    File.write!(decisions, String.replace(document, marker, "#{marker}\n\n#{line}", global: false))
    IO.puts(line)
  end

  defp machine_memory do
    case File.read("/proc/meminfo") do
      {:ok, contents} ->
        case Regex.run(~r/^MemTotal:\s+(\d+)\s+kB$/m, contents) do
          [_, kib] -> "#{kib} kB machine memory"
          _ -> "unknown machine memory"
        end

      _ ->
        "unknown machine memory"
    end
  end

  defp disk_type do
    case System.cmd("df", ["-P", "."], stderr_to_stdout: true) do
      {output, 0} ->
        device = output |> String.split("\n") |> List.last() |> String.split() |> List.first()
        disk_type(device)

      _ ->
        "unknown"
    end
  end

  defp disk_type(nil), do: "unknown"

  defp disk_type(device) do
    case System.cmd("lsblk", ["-ndo", "rota", device], stderr_to_stdout: true) do
      {"0\n", 0} -> "non-rotational"
      {"1\n", 0} -> "rotational"
      _ -> "unknown"
    end
  end
end

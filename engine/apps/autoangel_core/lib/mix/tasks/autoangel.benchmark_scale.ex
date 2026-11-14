defmodule Mix.Tasks.Autoangel.BenchmarkScale do
  @moduledoc """
  Scale and footprint benchmark (Requirements 71.1 to 71.4 and 72.1 to 72.3).

      mix autoangel.benchmark_scale [--series 20000] [--interval 10] [--cycles 6]
                                    [--batch 1000] [--no-record]

  Runs the Engine in this node (Topology_A, no storage backend) and pushes one
  Sample per Series every `--interval` seconds, for `--cycles` cycles, through the
  real HTTP ingest endpoint, in batches of `--batch` spread across each interval.

  It records the machine, the topology, the duration, the detection latency
  histogram, the deepest Worker queue, the longest scoring call, the memory per
  1,000 Series, and appends one block to `docs/decisions.md`.

  The run fails (non-zero exit) when the largest detection latency is above 1 s or
  when any Sample was shed or any push was refused (a queue overflow).
  Disk per Series-day is measured by `mix autoangel.benchmark_disk`.
  """
  @shortdoc "Benchmark the Engine at scale"
  use Mix.Task

  alias Autoangel.{Boot, Metrics}
  alias Autoangel.Ingest.Metrics, as: IngestMetrics
  alias Autoangel.Workers.Worker

  @max_latency_seconds 1.0
  @metric_names 20

  @impl Mix.Task
  def run(args) do
    {opts, _rest, _invalid} =
      OptionParser.parse(args,
        strict: [
          series: :integer,
          interval: :integer,
          cycles: :integer,
          batch: :integer,
          record: :boolean
        ]
      )

    series = Keyword.get(opts, :series, 20_000)
    interval = Keyword.get(opts, :interval, 10)
    cycles = Keyword.get(opts, :cycles, 6)
    batch = Keyword.get(opts, :batch, 1_000)
    record? = Keyword.get(opts, :record, true)

    {ingest_port, control_port} = {free_port(), free_port()}
    start_engine(series, ingest_port, control_port)

    memory_before = :erlang.memory(:total)
    sampler = start_queue_sampler()
    started = System.monotonic_time(:millisecond)
    refused = push_load(series, interval, cycles, batch, ingest_port)
    settle()
    duration = (System.monotonic_time(:millisecond) - started) / 1_000

    max_queue = stop_queue_sampler(sampler)
    :erlang.garbage_collect()
    memory_after = :erlang.memory(:total)

    result = %{
      series: series,
      interval: interval,
      cycles: cycles,
      duration: duration,
      refused: refused,
      shed: IngestMetrics.load_shed(),
      max_queue: max_queue,
      max_latency: Metrics.max_observed(:detection_latency_seconds),
      max_nif: Metrics.max_observed(:nif_call_duration_seconds),
      observations: Metrics.observations(:detection_latency_seconds),
      buckets: Metrics.buckets(:detection_latency_seconds),
      memory_per_1000: (memory_after - memory_before) / series * 1_000
    }

    block = report(result)
    IO.puts(block)
    if record?, do: record(block)
    check(result)
  end

  defp start_engine(series, ingest_port, control_port) do
    toml = """
    [ingest]
    token = "bench-ingest"
    [control_api]
    token = "bench-control"
    [http]
    ingest_port = #{ingest_port}
    control_port = #{control_port}
    [ingest_rate_limit]
    rate = 1000000
    burst = 1000000
    [cardinality]
    per_metric = #{div(series, @metric_names) + 1_000}
    total = #{series + 1_000}
    """

    {:ok, config} = Autoangel.Config.load(toml, %{})

    System.put_env(
      "AUTOANGEL_STATE_PATH",
      Path.join(System.tmp_dir!(), "autoangel-scale-#{System.unique_integer([:positive])}")
    )

    Application.put_env(:autoangel_core, :config, config)
    {:ok, _} = Application.ensure_all_started(:autoangel_core)
    wait_until(&Boot.ready?/0)
    {:ok, _} = Application.ensure_all_started(:inets)
  end

  # One Sample per Series per cycle, in batches, evenly spread over the interval.
  defp push_load(series, interval, cycles, batch, port) do
    batches = ceil(series / batch)
    pause = max(div(interval * 1_000, batches), 1)

    Enum.reduce(1..cycles, 0, fn cycle, refused ->
      cycle_started = System.monotonic_time(:millisecond)
      refused = push_cycle(cycle, {series, batch, batches}, pause, port, refused)

      # Keep to the cadence even when a cycle's pushes finished early.
      remaining = interval * 1_000 - (System.monotonic_time(:millisecond) - cycle_started)
      if remaining > 0, do: Process.sleep(remaining)
      refused
    end)
  end

  defp push_cycle(cycle, {series, batch, batches}, pause, port, refused) do
    Enum.reduce(0..(batches - 1), refused, fn index, acc ->
      first = index * batch
      last = min(first + batch, series) - 1
      status = push(body(first..last, cycle), port)
      Process.sleep(pause)
      if status in 200..299, do: acc, else: acc + 1
    end)
  end

  defp body(range, cycle) do
    Enum.map_join(range, "", fn id ->
      value = 100.0 + :math.sin((id + cycle) / 3.0) * 5.0
      "bench_metric_#{rem(id, @metric_names)}{series=\"#{id}\"} #{value}\n"
    end)
  end

  defp push(body, port) do
    url = ~c"http://127.0.0.1:#{port}/api/v1/push"
    headers = [{~c"authorization", ~c"Bearer bench-ingest"}]

    case :httpc.request(:post, {url, headers, ~c"text/plain", body}, [timeout: 30_000], []) do
      {:ok, {{_, status, _}, _, _}} -> status
      {:error, _reason} -> 0
    end
  end

  # Wait for every Worker queue to drain so the last latencies are counted.
  defp settle do
    Enum.each(workers(), &:sys.get_state/1)
    Process.sleep(500)
  end

  defp workers do
    for {_id, pid, _, _} <- Supervisor.which_children(Autoangel.Workers), is_pid(pid), do: pid
  end

  defp start_queue_sampler do
    parent = self()

    spawn_link(fn -> sample_queues(parent, 0) end)
  end

  defp sample_queues(parent, max) do
    receive do
      :stop -> send(parent, {:max_queue, max})
    after
      250 ->
        depth =
          for index <- 0..(length(workers()) - 1),
              counter = Worker.queue_counter(index),
              counter != nil,
              reduce: 0 do
            acc -> max(acc, :atomics.get(counter, 1))
          end

        sample_queues(parent, max(max, depth))
    end
  end

  defp stop_queue_sampler(pid) do
    send(pid, :stop)

    receive do
      {:max_queue, max} -> max
    after
      5_000 -> 0
    end
  end

  defp report(r) do
    {cores, memory} = machine()
    total = max(r.observations, 1)

    histogram =
      Enum.map_join(r.buckets, ", ", fn {bound, count} ->
        "<= #{bound} s: #{count} (#{Float.round(count / total * 100, 2)}%)"
      end)

    """
    ### Scale benchmark (#{DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601()})

    - Machine: #{:erlang.system_info(:system_architecture)}, #{cores} schedulers, #{memory} RAM, OTP #{:erlang.system_info(:otp_release)}, Elixir #{System.version()}.
    - Topology: Topology_A (Engine only, no storage backend), Samples pushed over HTTP to the ingest endpoint, kernel: Elixir.
    - Load: #{r.series} Series, one Sample per Series every #{r.interval} s, #{r.cycles} cycles, #{Float.round(r.duration, 1)} s of wall time.
    - Detection latency (#{r.observations} Samples): #{histogram}; maximum #{Float.round(r.max_latency * 1_000, 1)} ms.
    - Deepest Worker queue: #{r.max_queue} Samples. Samples shed: #{r.shed}. Pushes refused: #{r.refused}.
    - Longest scoring call: #{Float.round(r.max_nif * 1_000, 3)} ms.
    - Memory: #{Float.round(r.memory_per_1000 / 1_048_576, 2)} MiB per 1,000 Series (Erlang VM total, before and after the load).
    """
  end

  defp machine do
    cores = :erlang.system_info(:schedulers_online)

    memory =
      case :os.type() do
        {:unix, :darwin} -> command_output("sysctl", ["-n", "hw.memsize"]) |> bytes()
        _ -> proc_memory()
      end

    {cores, memory}
  end

  defp proc_memory do
    case File.read("/proc/meminfo") do
      {:ok, text} ->
        case Regex.run(~r/MemTotal:\s+(\d+) kB/, text) do
          [_, kb] -> bytes(String.to_integer(kb) * 1024)
          _ -> "unknown"
        end

      _ ->
        "unknown"
    end
  end

  defp bytes(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {n, _} -> bytes(n)
      :error -> "unknown"
    end
  end

  defp bytes(n) when is_integer(n), do: "#{Float.round(n / 1_073_741_824, 1)} GiB"

  defp command_output(command, args) do
    case System.cmd(command, args, stderr_to_stdout: true) do
      {output, 0} -> output
      _ -> ""
    end
  rescue
    _ -> ""
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

  defp check(r) do
    problems =
      [
        {r.max_latency > @max_latency_seconds,
         "maximum detection latency #{Float.round(r.max_latency, 3)} s is above #{@max_latency_seconds} s"},
        {r.shed > 0, "#{r.shed} Samples were shed"},
        {r.refused > 0, "#{r.refused} pushes were refused"}
      ]
      |> Enum.filter(&elem(&1, 0))
      |> Enum.map(&elem(&1, 1))

    if problems != [], do: Mix.raise("scale benchmark failed: " <> Enum.join(problems, "; "))
    IO.puts("scale benchmark passed")
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, ip: {127, 0, 0, 1})
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end

  defp wait_until(fun, tries \\ 200) do
    cond do
      fun.() ->
        :ok

      tries == 0 ->
        Mix.raise("the Engine did not become ready")

      true ->
        Process.sleep(50)
        wait_until(fun, tries - 1)
    end
  end
end

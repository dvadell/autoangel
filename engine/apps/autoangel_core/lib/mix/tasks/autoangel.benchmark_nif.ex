defmodule Mix.Tasks.Autoangel.BenchmarkNif do
  @moduledoc "Benchmarks the Elixir kernels for the M0 NIF promotion gate."
  @shortdoc "Benchmark Elixir kernels for the NIF gate"
  use Mix.Task

  @series_count 20_000
  @batch_sizes [1, 2, 4, 8, 16, 32, 64, 128, 256]
  @throughput_target 2_000
  @nif_duration_target_us 1_000

  alias Autoangel.Exposition
  alias Autoangel.Kernel.{Params, Scoring, State}

  @impl Mix.Task
  def run(args) do
    {opts, _args, invalid} =
      OptionParser.parse(args,
        strict: [series_count: :integer, batch_sizes: :string]
      )

    if invalid != [], do: Mix.raise("invalid benchmark options: #{inspect(invalid)}")

    Mix.Task.run("app.start", [])

    series_count = Keyword.get(opts, :series_count, @series_count)
    batch_sizes = parse_batch_sizes(Keyword.get(opts, :batch_sizes, Enum.join(@batch_sizes, ",")))

    if series_count <= 0, do: Mix.raise("--series-count must be positive")

    params = %Params{}
    empty_state = State.encode(State.new(capacity: params.window_size))
    warmed_state = warmed_state(empty_state, params)
    results = Enum.map(batch_sizes, &measure_batch(&1, series_count, warmed_state, params))
    memory = measure_memory(series_count, warmed_state, params)
    parsing = measure_parsing_share(series_count, warmed_state, params)
    decision = promotion_decision(results, memory)
    dependency_note = dependency_note()

    report = report(series_count, results, memory, parsing, decision, dependency_note)

    if series_count == @series_count and batch_sizes == @batch_sizes do
      append_decisions(report)
    else
      IO.puts(
        "custom benchmark parameters were used; results were not written to docs/decisions.md"
      )
    end

    IO.puts(report)
  end

  defp parse_batch_sizes(value) do
    sizes =
      value
      |> String.split(",", trim: true)
      |> Enum.map(fn text -> String.to_integer(String.trim(text)) end)

    if sizes == [] or Enum.any?(sizes, &(&1 < 1 or &1 > 256)),
      do: Mix.raise("--batch-sizes must contain integers from 1 through 256")

    sizes
  rescue
    ArgumentError -> Mix.raise("--batch-sizes must be a comma-separated list of integers")
  end

  defp warmed_state(state, params) do
    warmup_samples = samples(params.window_size, 0)
    {new_state, _results} = score(state, warmup_samples, params)
    new_state
  end

  defp measure_batch(batch_size, series_count, state, params) do
    samples = samples(batch_size, 1)

    {elapsed_us, {duration_total_us, durations, states}} =
      :timer.tc(fn ->
        Enum.reduce(1..series_count, {0, [], []}, fn series, {duration_total, durations, states} ->
          {duration_us, new_state} = timed_score(state, samples_for_series(samples, series), params)
          {duration_total + duration_us, [duration_us | durations], [new_state | states]}
        end)
      end)

    durations = Enum.sort(durations)
    calls = series_count
    samples_scored = calls * batch_size

    %{
      batch_size: batch_size,
      elapsed_us: elapsed_us,
      samples_per_second: samples_scored / max(elapsed_us / 1_000_000, 0.000001),
      average_call_us: duration_total_us / calls,
      max_call_us: List.last(durations),
      p99_call_us: percentile(durations, 0.99),
      state_digest: :erlang.phash2(states)
    }
  end

  defp measure_memory(series_count, state, _params) do
    encoded_bytes = byte_size(state) * series_count

    %{
      bytes_per_series: byte_size(state),
      total_bytes: encoded_bytes,
      term_bytes_per_series: :erts_debug.size(state) * :erlang.system_info(:wordsize)
    }
  end

  defp measure_parsing_share(series_count, state, params) do
    body = exposition_body(series_count)
    {parse_ms, {:ok, families}} = runtime_tc(fn -> Exposition.parse(body) end)
    samples = families |> Enum.flat_map(& &1.samples) |> Enum.map(&{&1.timestamp, &1.value, :gauge})

    {score_ms, _states} =
      runtime_tc(fn ->
        Enum.map(samples, fn sample ->
          {new_state, _results} = score(state, [sample], params)
          new_state
        end)
      end)

    total_ms = parse_ms + score_ms

    %{
      sample_count: length(samples),
      parse_ms: parse_ms,
      score_ms: score_ms,
      parse_cpu_share: parse_ms / max(total_ms, 1) * 100.0
    }
  end

  defp runtime_tc(function) do
    {runtime_before, _} = :erlang.statistics(:runtime)
    result = function.()
    {runtime_after, _} = :erlang.statistics(:runtime)
    {runtime_after - runtime_before, result}
  end

  defp timed_score(state, samples, params) do
    {duration_us, {new_state, _results}} = :timer.tc(fn -> score(state, samples, params) end)
    {duration_us, new_state}
  end

  defp score(state, samples, params) do
    case Scoring.Elixir.score(state, samples, params) do
      {:ok, new_state, results} -> {new_state, results}
      {:error, reason} -> Mix.raise("Elixir scoring benchmark failed: #{inspect(reason)}")
    end
  end

  defp samples(batch_size, series) do
    for offset <- 0..(batch_size - 1) do
      {series * 1_000_000 + offset * 1_000, 100.0 + rem(series + offset, 17) / 10.0, :gauge}
    end
  end

  defp samples_for_series(samples, series) do
    Enum.map(samples, fn {timestamp, value, kind} ->
      {timestamp + series, value, kind}
    end)
  end

  defp exposition_body(series_count) do
    lines =
      for series <- 1..series_count do
        "autoangel_benchmark_value{series=\"#{series}\"} #{100.0 + rem(series, 17) / 10.0} #{series}000"
      end

    Enum.join(["# TYPE autoangel_benchmark_value gauge" | lines], "\n") <> "\n"
  end

  defp percentile(values, percentile) do
    index = max(0, min(length(values) - 1, ceil(length(values) * percentile) - 1))
    Enum.at(values, index)
  end

  defp promotion_decision(results, memory) do
    passing_prefix =
      Enum.take_while(
        Enum.sort_by(results, & &1.batch_size),
        &(&1.max_call_us <= @nif_duration_target_us)
      )

    final_batch_size =
      case List.last(passing_prefix) do
        nil -> 0
        result -> result.batch_size
      end

    throughput_pass? = Enum.all?(results, &(&1.samples_per_second >= @throughput_target))

    %{
      promoted_kernels: [],
      selected_implementation: :elixir,
      max_batch_size: final_batch_size,
      duration_target_us: @nif_duration_target_us,
      duration_target_met?: final_batch_size > 0,
      throughput_target: @throughput_target,
      throughput_target_met?: throughput_pass?,
      rationale:
        if throughput_pass? do
          "Elixir scoring met the 2,000 Samples/s throughput gate and remains selected; no kernel is promoted to Rust. Calls above 1 ms require dirty scheduling or chunking under Requirement 47.3."
        else
          "Elixir scoring missed the 2,000 Samples/s throughput gate; scoring is a Rust candidate for task 4.2."
        end,
      memory_bytes_per_series: memory.bytes_per_series
    }
  end

  defp dependency_note do
    native_path = Path.expand("../../../../../native", __DIR__)

    cond do
      not File.dir?(native_path) ->
        "No Rust kernel crate is available at engine/native; only the Elixir reference kernel was measured."

      not Code.ensure_loaded?(Rustler) ->
        "The engine/native crate exists, but Rustler is unavailable in the current Mix environment; only the Elixir reference kernel was measured."

      true ->
        "Rustler is available, but no Rust kernel was invoked because task 3.3 benchmarks the Elixir reference before promotion."
    end
  end

  defp report(series_count, results, memory, parsing, decision, dependency_note) do
    batch_lines =
      Enum.map_join(results, "\n", fn result ->
        "- batch #{result.batch_size}: #{Float.round(result.samples_per_second, 2)} Samples/s, " <>
          "#{Float.round(result.average_call_us, 3)} us/call average, #{Float.round(result.max_call_us * 1.0, 2)} us max, " <>
          "#{Float.round(result.p99_call_us * 1.0, 2)} us p99 (#{result.elapsed_us} us total)"
      end)

    """
    ### NIF gate benchmark (#{DateTime.utc_now() |> DateTime.to_iso8601()})

    Machine: #{System.schedulers_online()} BEAM schedulers, #{machine_memory()}.
    Workload: warmed Elixir scoring kernel, #{series_count} Series, batch sizes #{Enum.map_join(results, ", ", &to_string(&1.batch_size))}.
    Throughput gate: #{decision.throughput_target} Samples/s (20,000 Series at one sample per 10 seconds).
    #{batch_lines}
    Memory: #{Float.round(memory.bytes_per_series * 1.0, 2)} encoded-state bytes/Series (#{memory.total_bytes} bytes for #{series_count} full-window states); BEAM term reference overhead #{Float.round(memory.term_bytes_per_series * 1.0, 2)} bytes/Series. No independent memory promotion target is defined by the requirements.
    Exposition parsing CPU share: #{Float.round(parsing.parse_cpu_share, 2)}% (#{parsing.parse_ms} ms parsing, #{parsing.score_ms} ms scoring, #{parsing.sample_count} Samples).
    Decision: selected #{decision.selected_implementation}; promoted kernels: #{if(decision.promoted_kernels == [], do: "none", else: Enum.map_join(decision.promoted_kernels, ", ", &Atom.to_string/1))}; final maximum batch size #{decision.max_batch_size}; Elixir call duration target #{decision.duration_target_us} us at maximum batch size. Duration target met by maximum observed call: #{decision.duration_target_met?}. Throughput target met: #{decision.throughput_target_met?}. #{decision.rationale}
    Dependency status: #{dependency_note}
    """
    |> String.trim()
  end

  defp append_decisions(report) do
    decisions = Path.expand("../../../../../../docs/decisions.md", __DIR__)
    document = File.read!(decisions)
    [before, after_section] = String.split(document, "## NIF gate", parts: 2)
    [_old_gate, after_gate] = String.split(after_section, "## Provisional defaults", parts: 2)
    updated = before <> "## NIF gate\n\n" <> report <> "\n\n## Provisional defaults" <> after_gate
    File.write!(decisions, updated)
  end

  defp machine_memory do
    case File.read("/proc/meminfo") do
      {:ok, contents} ->
        case Regex.run(~r/^MemTotal:\s+(\d+)\s+kB$/m, contents) do
          [_, kib] -> "#{kib} kB host memory"
          _ -> "unknown host memory"
        end

      _ ->
        "unknown host memory"
    end
  end
end

defmodule Autoangel.Workers.Property47ChunkingTest do
  @moduledoc """
  Property 47 (Requirements 47.7, 47.9).

  "For any pending Sample list for a Series and any maximum batch size, every
  kernel call receives at most the maximum batch size, chunks are in timestamp
  order, every Sample is scored, and the concatenated results and final state
  equal those of scoring the Samples one at a time."

  The real `Autoangel.Workers.Worker` folds pending Samples per Series through
  its scoring pipeline: `samples |> Enum.sort_by(timestamp) |>
  Enum.chunk_every(max_batch)`, then each chunk is scored against the kernel and
  threaded through `Decision`. Each chunk becomes exactly one kernel call to
  `Autoangel.Kernel.Selected.score/3`, which delegates to
  `Autoangel.Kernel.Scoring.Elixir.score/3`.

  A comparison of only the final `kernel_state`/`meta` across batch sizes is
  insufficient: the kernel folds the (already sorted) sample sequence
  internally, so the final state is identical regardless of how the sequence is
  grouped into calls. That observation hides the chunking behaviour itself —
  mutations that drop the `Enum.sort_by(timestamp)` or ignore `max_batch` would
  survive, because they do not change the folded end state.

  To observe the chunking directly, this test TRACES the real kernel calls the
  Worker makes. Using Erlang call tracing on the Worker process, it captures the
  argument list (the batch) and return value (the per-call results) of every
  `Autoangel.Kernel.Scoring.Elixir.score/3` invocation, then asserts:

    1. Batch bound (47.9): every traced call's sample batch has length
       <= max_batch.
    2. Real batching (47.7): when max_batch > 1 and the series has >= 2 samples,
       at least one traced call carries more than one sample (kills a mutation
       that always chunks size 1).
    3. Timestamp order: the concatenation of all traced calls' sample
       timestamps, in call order, is non-decreasing AND equals the sorted list
       of all input timestamps (kills dropping the sort).
    4. Every sample scored: the total number of per-sample results across all
       traced calls equals the number of input samples.
    5. Concatenated results equal one-at-a-time: the generated Worker's
       concatenated per-sample results list equals that of a second Worker run
       with max_batch = 1 (same per-sample result maps, in timestamp order).

  It additionally keeps the original end-state equivalence: the final
  `kernel_state` and normalized `meta` are identical across the generated
  `max_batch`, strictly one-at-a-time (`max_batch = 1`, the property's
  "scoring the Samples one at a time" reference), and a single whole-list chunk.

  Isolation notes: the Worker keeps a `:persistent_term` keyed by `index` and
  arms a missing-data sweep timer. Every Worker started here uses a UNIQUE index
  (derived from `System.unique_integer([:positive])`) so runs never collide, a
  very large `sweep_interval` so no sweep mutates state mid-test, and a fixed
  `now` so results are deterministic. The persistent_term keys are erased on
  exit. Tracing is torn down (and the mailbox drained) before the Worker is
  stopped so no stray trace messages leak between runs. `async: false` because
  of the shared persistent_term namespace and the sweep timer.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  alias Autoangel.Kernel.Params
  alias Autoangel.Kernel.Scoring.Elixir, as: KernelElixir
  alias Autoangel.SeriesRegistry
  alias Autoangel.Workers.Worker

  # Fixed wall clock so `received_at`/`now` are deterministic across Workers.
  @fixed_now 1_700_000_000_000
  # Large enough that the sweep never fires during a single check iteration.
  @huge_sweep 100_000_000
  @decision_cfg %{
    learning: %{duration: 0},
    persistence: %{n: 1, m: 5},
    missing_data: %{multiple: 3.0}
  }

  # A single series key keeps the property focused on the chunking invariant.
  @name "property47_metric"
  @labels %{"instance" => "a", "job" => "svc"}

  # Idle timeout (ms) for draining the trace mailbox after a synchronous run.
  @trace_idle_timeout 200

  defp canonical_key, do: SeriesRegistry.canonical_key(@name, @labels)

  # Each started anomaly is stamped with a fresh random `id`
  # (`:crypto.strong_rand_bytes/1` in `Decision.make_anomaly/6`), so two
  # otherwise-identical runs produce different ids. That randomness is unrelated
  # to chunking, so compare `meta` with every open-anomaly id blanked. Every
  # other field — statuses, timestamps, scores, run bookkeeping — is fully
  # determined by the (sorted) sample sequence and must match exactly.
  defp normalize_meta(nil), do: nil

  defp normalize_meta(%{} = meta) do
    meta
    |> blank_anomaly_id(:open_value_anomaly)
    |> blank_anomaly_id(:open_missing)
    |> blank_anomaly_id(:open_drift)
  end

  defp blank_anomaly_id(meta, field) do
    case Map.get(meta, field) do
      %{} = anomaly -> Map.put(meta, field, %{anomaly | id: nil})
      _ -> meta
    end
  end

  # Build a 4-tuple Sample `{{name, labels}, value, ts, :gauge}`. Using tuples
  # keeps kind handling trivial (always :gauge) and avoids counter-rate ordering
  # subtleties while remaining a faithful Worker input shape.
  defp sample(value, ts), do: {canonical_key(), value, ts, :gauge}

  # Start an isolated Worker with a unique index and a suppressed sweep. Returns
  # the pid; the caller stops the Worker and erases its persistent_term keys.
  defp start_worker(max_batch, index) do
    {:ok, pid} =
      Worker.start_link(
        index: index,
        name: nil,
        max_batch: max_batch,
        kernel_params: %Params{},
        sweep_interval: @huge_sweep,
        now: fn -> @fixed_now end,
        queue_counter: :atomics.new(1, signed: true),
        queue_capacity: 1_000_000,
        decision_cfg: @decision_cfg,
        scrape_status_table: nil,
        state_store: nil
      )

    pid
  end

  defp unique_index, do: System.unique_integer([:positive])

  defp stop_worker(pid, index) do
    GenServer.stop(pid)
    :persistent_term.erase({Worker, index})
    :persistent_term.erase({Worker, :capacity, index})
  end

  # Enqueue all samples in ONE cast so `process_pending` runs once over the full
  # sorted list, then read back the settled series via the synchronous call,
  # which also forces the preceding async cast to be fully processed first.
  defp score(max_batch, samples) do
    index = unique_index()
    pid = start_worker(max_batch, index)

    Worker.enqueue(pid, samples)
    result = Worker.series(pid, canonical_key())

    stop_worker(pid, index)
    result
  end

  # Like `score/2`, but also traces every `Autoangel.Kernel.Scoring.Elixir.score/3`
  # call the Worker makes while draining the pending samples. Returns
  # `{series_state, calls}` where `calls` is the list — in call order — of
  # `%{batch_ts: [ts, ...], results: [map, ...]}`, one entry per kernel call.
  #
  # The scoring runs inside the Worker process while it handles the `:samples`
  # cast. The subsequent synchronous `series/2` call is processed strictly after
  # that cast, so by the time `series/2` returns every trace message for this
  # run is already queued in this process' mailbox. We drain it with a bounded
  # idle-timeout loop, then tear tracing down before stopping the Worker.
  defp trace_score(max_batch, samples) do
    index = unique_index()
    pid = start_worker(max_batch, index)

    # `trace_pattern` only matches loaded modules; force the kernel module in.
    Code.ensure_loaded!(KernelElixir)
    :erlang.trace(pid, true, [:call])

    :erlang.trace_pattern(
      {KernelElixir, :score, 3},
      [{:_, [], [{:return_trace}]}],
      [:local]
    )

    Worker.enqueue(pid, samples)
    result = Worker.series(pid, canonical_key())

    calls = drain_trace(pid, [], [])

    :erlang.trace(pid, false, [:call])
    flush_trace_mailbox(pid)
    stop_worker(pid, index)

    {result, calls}
  end

  # Collect trace messages for `pid` until the mailbox is idle for
  # `@trace_idle_timeout` ms. `:call` messages carry the batch argument; the
  # matching `:return_from` carries the per-call results. We pair them in call
  # order: each `:call` pushes a pending batch, each `:return_from` completes the
  # oldest still-open batch into a finished call entry. Trace messages for any
  # other pid (there should be none) are ignored defensively.
  defp drain_trace(pid, pending, done) do
    receive do
      {:trace, ^pid, :call, {_mod, :score, [_state, batch, _params]}} ->
        batch_ts = Enum.map(batch, fn {ts, _value, _kind} -> ts end)
        drain_trace(pid, pending ++ [batch_ts], done)

      {:trace, ^pid, :return_from, {_mod, :score, 3}, return} ->
        results = results_from_return(return)

        case pending do
          [batch_ts | rest] ->
            drain_trace(pid, rest, done ++ [%{batch_ts: batch_ts, results: results}])

          [] ->
            drain_trace(pid, pending, done)
        end

      {:trace, ^pid, _event, _arg} ->
        drain_trace(pid, pending, done)

      {:trace, ^pid, _event, _arg1, _arg2} ->
        drain_trace(pid, pending, done)
    after
      @trace_idle_timeout -> done
    end
  end

  defp results_from_return({:ok, _new_state, results}) when is_list(results), do: results
  defp results_from_return(_other), do: []

  # Defensive: discard any remaining trace messages for `pid` so they cannot
  # bleed into a later run started from the same test process.
  defp flush_trace_mailbox(pid) do
    receive do
      {:trace, ^pid, _event, _arg} -> flush_trace_mailbox(pid)
      {:trace, ^pid, _event, _arg1, _arg2} -> flush_trace_mailbox(pid)
    after
      0 -> :ok
    end
  end

  # Generates a non-empty list of gauge samples (finite float values, integer
  # timestamps that may duplicate) plus a max_batch in 1..8.
  defp scenario do
    gen all(
          values <- list_of(float(min: -1.0e6, max: 1.0e6), min_length: 1, max_length: 40),
          timestamps <- list_of(integer(0..5_000), length: length(values)),
          max_batch <- integer(1..8)
        ) do
      samples = Enum.zip_with(values, timestamps, &sample/2)
      {samples, max_batch}
    end
  end

  property "chunking by any max_batch matches one-at-a-time in batching, order, coverage, and results" do
    check all({samples, max_batch} <- scenario(), max_runs: 50) do
      sample_count = length(samples)
      single_batch = sample_count + 1
      input_ts = Enum.map(samples, fn {_key, _value, ts, _kind} -> ts end)

      {generated, generated_calls} = trace_score(max_batch, samples)
      {one_at_a_time, one_calls} = trace_score(1, samples)
      whole = score(single_batch, samples)

      # Every Sample is for the one series, so the series must exist.
      assert generated != nil
      assert one_at_a_time != nil
      assert whole != nil

      # The Worker made at least one kernel call for the series.
      assert generated_calls != []

      # 1. Batch bound (47.9): no kernel call exceeds max_batch.
      Enum.each(generated_calls, fn call ->
        assert length(call.batch_ts) <= max_batch,
               "a kernel call received #{length(call.batch_ts)} samples, over max_batch=#{max_batch}"
      end)

      # 2. Real batching (47.7): with room to batch and enough samples, at least
      # one call must carry more than one sample.
      if max_batch > 1 and sample_count >= 2 do
        assert Enum.any?(generated_calls, &(length(&1.batch_ts) > 1)),
               "max_batch=#{max_batch} with #{sample_count} samples produced only size-1 chunks"
      end

      # 3. Timestamp order: concatenated call timestamps, in call order, are
      # non-decreasing and equal the sorted input timestamps.
      concatenated_ts = Enum.flat_map(generated_calls, & &1.batch_ts)

      assert concatenated_ts == Enum.sort(concatenated_ts),
             "kernel saw timestamps out of order: #{inspect(concatenated_ts)}"

      assert concatenated_ts == Enum.sort(input_ts),
             "kernel timestamps #{inspect(concatenated_ts)} != sorted inputs #{inspect(Enum.sort(input_ts))}"

      # 4. Every sample scored: one result per input sample across all calls.
      generated_results = Enum.flat_map(generated_calls, & &1.results)

      assert length(generated_results) == sample_count,
             "kernel produced #{length(generated_results)} results for #{sample_count} samples"

      # 5. Concatenated per-sample results equal strictly one-at-a-time scoring.
      one_results = Enum.flat_map(one_calls, & &1.results)

      assert generated_results == one_results,
             "concatenated results differed between max_batch=#{max_batch} and max_batch=1"

      # End-state equivalence across max_batch, one-at-a-time, and single chunk
      # (final folded state and normalized meta are batch-size independent).
      assert generated.kernel_state == one_at_a_time.kernel_state,
             "kernel_state differed between max_batch=#{max_batch} and max_batch=1"

      assert normalize_meta(generated.meta) == normalize_meta(one_at_a_time.meta),
             "meta differed between max_batch=#{max_batch} and max_batch=1"

      assert generated.kernel_state == whole.kernel_state,
             "kernel_state differed between max_batch=#{max_batch} and a single chunk"

      assert normalize_meta(generated.meta) == normalize_meta(whole.meta),
             "meta differed between max_batch=#{max_batch} and a single chunk"
    end
  end

  # Deterministic example: the Worker sorts by timestamp before chunking, so
  # out-of-order enqueued Samples scored with a tiny batch must be seen by the
  # kernel in timestamp order and settle identically to a single batch. This
  # pins the "chunks are in timestamp order" and "concatenated results equal
  # one-at-a-time" invariants to concrete input.
  test "out-of-order samples are chunked in timestamp order and settle identically" do
    samples = [
      sample(3.0, 300),
      sample(1.0, 100),
      sample(2.0, 200),
      sample(5.0, 500),
      sample(4.0, 400)
    ]

    {one, one_calls} = trace_score(1, samples)
    {two, two_calls} = trace_score(2, samples)
    big = score(99, samples)

    assert one != nil

    # The kernel sees the sorted timestamp sequence regardless of batch size.
    assert Enum.flat_map(two_calls, & &1.batch_ts) == [100, 200, 300, 400, 500]
    # max_batch=2 over five samples must produce at least one multi-sample call.
    assert Enum.any?(two_calls, &(length(&1.batch_ts) > 1))
    assert Enum.all?(two_calls, &(length(&1.batch_ts) <= 2))

    # Concatenated per-sample results match strictly one-at-a-time scoring.
    assert Enum.flat_map(two_calls, & &1.results) == Enum.flat_map(one_calls, & &1.results)

    assert one.kernel_state == two.kernel_state
    assert one.kernel_state == big.kernel_state
    assert normalize_meta(one.meta) == normalize_meta(two.meta)
    assert normalize_meta(one.meta) == normalize_meta(big.meta)
  end
end

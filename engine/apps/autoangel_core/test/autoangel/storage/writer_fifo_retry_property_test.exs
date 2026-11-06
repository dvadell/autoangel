defmodule Autoangel.Storage.WriterFifoRetryPropertyBackend do
  @moduledoc """
  A scripted storage backend used only by the FIFO/retry property test.

  `init/1` receives a controller Agent (holding an ordered list of outcomes)
  via the backend config and returns it as the opaque handle. Every call to
  `append_samples/2` pops the NEXT outcome from the controller. When the script
  is exhausted the backend returns `:ok` so the writer always drains cleanly and
  the test can terminate.

  The backend deliberately does NOT record attempts: the writer's
  `batch_observer` hook is the single source of truth for what was written and
  with which `uuid`, which keeps attempt capture independent of the outcome
  scripting.
  """
  @behaviour Autoangel.Storage

  @impl true
  def init(config), do: {:ok, Keyword.fetch!(config, :controller)}

  @impl true
  def append_samples(controller, _samples), do: next_outcome(controller)

  @impl true
  def write_anomalies(_handle, _items), do: :ok

  @impl true
  def write_series(_handle, _items), do: :ok

  @impl true
  def write_bands(_handle, _items), do: :ok

  @impl true
  def read_range(_handle, _ids, _from, _to), do: {:ok, []}

  @impl true
  def read_aggregates(_handle, _ids, _from, _to, _resolution), do: {:ok, []}

  # Pops the next scripted outcome, defaulting to :ok once the script is empty.
  defp next_outcome(controller) do
    Agent.get_and_update(controller, fn
      [outcome | rest] -> {outcome, rest}
      [] -> {:ok, []}
    end)
  end
end

defmodule Autoangel.Storage.WriterFifoRetryPropertyTest do
  @moduledoc """
  Property 38 (Requirements 34.2, 34.3, 34.4, 34.8, 36.6).

  "For any queue content and any sequence of backend outcomes from a mock
  backend, batches are written in FIFO order, a retryable batch is retried
  unchanged with each delay at least twice the previous one, a permanent batch
  is not retried and the next batch follows, and the success, retryable,
  permanent and dropped counters equal the counts in the reference model."

  Strategy
  --------
  The real `Autoangel.Storage.Writer` is driven with a SCRIPTED backend whose
  `append_samples/2` returns outcomes popped from a controller Agent, and with a
  `batch_observer` that forwards every write attempt (`{:attempt, uuid, samples}`)
  to the test process. Because the writer calls the observer immediately before
  each backend call, the observer captures EVERY attempt — including each retry —
  together with the stable per-batch `uuid`.

  An independent reference model splits the queued items into FIFO batches of
  `batch_size` and consumes the SAME outcome script to predict, for each batch:
  how many attempts it takes (one per outcome until `:ok`/`:permanent`, with the
  script defaulting to `:ok` when exhausted), whether it succeeds or is dropped,
  and the running success/retryable/permanent counters. The predicted attempt
  sequence and counters are asserted against what the writer actually did.

  The retry backoff doubling (34.3) cannot be timed deterministically under the
  fast timers used for draining, so the property asserts the STRUCTURAL retry
  guarantee (same uuid + same samples, repeated the scripted number of times).
  A separate deterministic example uses a large `retry_ms` and `:sys.get_state/1`
  to observe `retry_ms` at least doubling between attempts.
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Autoangel.Storage.Writer
  alias Autoangel.Storage.WriterFifoRetryPropertyBackend, as: Backend

  # Small timers so the property drains quickly; retries fire within a few ms.
  @retry_ms 5
  @max_retry_ms 40
  @drain_timeout 500

  # ----------------------------------------------------------------------------
  # Reference model
  #
  # Splits `items` into FIFO batches of `batch_size`, then walks each batch in
  # order consuming `script` (a flat list of outcomes shared across all batches,
  # exactly as the writer consumes them one backend call at a time). A batch is
  # attempted repeatedly: `:retryable` counts a retryable and re-attempts the
  # SAME batch; `:ok` counts a success and advances; `{:permanent, _}` counts a
  # permanent, drops the batch and advances. When the script is exhausted the
  # outcome defaults to `:ok`, matching the backend.
  #
  # Returns:
  #   * batches   — the FIFO list of batches (each a list of items), in order
  #   * attempts  — the expected flat attempt sequence as {batch_index, samples}
  #   * counters  — %{successful:, retryable:, permanent:}
  # ----------------------------------------------------------------------------

  defmodule Model do
    @moduledoc false

    def run(items, batch_size, script) do
      batches = chunk(items, batch_size)

      {attempts, counters, _script} =
        batches
        |> Enum.with_index()
        |> Enum.reduce({[], %{successful: 0, retryable: 0, permanent: 0}, script}, fn
          {batch, index}, {attempts, counters, script} ->
            {batch_attempts, counters, script} = run_batch(batch, index, counters, script)
            {attempts ++ batch_attempts, counters, script}
        end)

      %{batches: batches, attempts: attempts, counters: counters}
    end

    # The writer's `take_batch/3` pulls up to `batch_size` contiguous same-kind
    # items from the front; with a single `:sample` kind this is a plain chunk.
    defp chunk(items, batch_size), do: Enum.chunk_every(items, batch_size)

    defp run_batch(batch, index, counters, script) do
      case next(script) do
        {:ok, rest} ->
          {[{index, batch}], bump(counters, :successful), rest}

        {{:permanent, _} = _outcome, rest} ->
          {[{index, batch}], bump(counters, :permanent), rest}

        {{:retryable, _} = _outcome, rest} ->
          {attempts, counters, rest} = run_batch(batch, index, bump(counters, :retryable), rest)
          {[{index, batch} | attempts], counters, rest}
      end
    end

    # Pops the next outcome, defaulting to :ok when the script is exhausted.
    defp next([outcome | rest]), do: {outcome, rest}
    defp next([]), do: {:ok, []}

    defp bump(counters, key), do: Map.update!(counters, key, &(&1 + 1))
  end

  # ----------------------------------------------------------------------------
  # Generators
  # ----------------------------------------------------------------------------

  # A scripted outcome. Retryable/permanent carry an opaque reason term so the
  # writer's reason-threading paths are exercised.
  defp outcome_gen do
    one_of([
      constant(:ok),
      map(integer(0..9), fn r -> {:retryable, {:try, r}} end),
      map(integer(0..9), fn r -> {:permanent, {:perm, r}} end)
    ])
  end

  # ----------------------------------------------------------------------------
  # Property
  # ----------------------------------------------------------------------------

  property "batches are written FIFO, retried unchanged, and counters match the model" do
    check all(
            n_items <- integer(1..24),
            batch_size <- integer(1..6),
            script <- list_of(outcome_gen(), max_length: 30),
            max_runs: 50
          ) do
      items = Enum.to_list(1..n_items)
      model = Model.run(items, batch_size, script)

      test_pid = self()
      {:ok, controller} = Agent.start_link(fn -> script end)

      {:ok, writer} =
        Writer.start_link(
          backend: Backend,
          backend_config: [controller: controller],
          batch_size: batch_size,
          retry_ms: @retry_ms,
          max_retry_ms: @max_retry_ms,
          batch_observer: fn uuid, samples -> send(test_pid, {:attempt, uuid, samples}) end
        )

      try do
        # Enqueue everything while NOT ready, then release the migration gate so
        # the whole queue drains under the scripted outcomes.
        assert :ok = Writer.enqueue_samples(writer, items)
        assert Writer.migration_succeeded(writer) == :ok

        actual = collect_attempts(length(model.attempts))

        # 1. FIFO order: the sequence of DISTINCT batches, by first-attempt
        #    order, equals the FIFO split of the queue.
        assert first_attempt_batches(actual) == model.batches

        # 2. Retried unchanged: every attempt of a given uuid carries identical
        #    samples, and the attempt multiplicity per uuid matches the model
        #    (one attempt per model attempt row).
        assert_unchanged_retries(actual, model)

        # 3 & 4 (structural): the full attempt sequence — including each retry
        #    and each permanent-then-advance — equals the model's prediction.
        assert attempt_samples(actual) == Enum.map(model.attempts, fn {_i, b} -> b end)

        # 5. Final counters equal the reference model. `dropped` counts OVERFLOW
        #    drops only (none here, capacity is the large default), so it is 0.
        stats = Writer.stats(writer)
        assert stats.successful == model.counters.successful
        assert stats.retryable == model.counters.retryable
        assert stats.permanent == model.counters.permanent
        assert stats.dropped == 0
        assert stats.queued == 0
      after
        GenServer.stop(writer)
        Agent.stop(controller)
      end
    end
  end

  # ----------------------------------------------------------------------------
  # Attempt collection + helpers
  # ----------------------------------------------------------------------------

  # Drains exactly `count` {:attempt, uuid, samples} messages in arrival order.
  # Counting attempts (rather than timing delays) keeps the drain deterministic.
  defp collect_attempts(count), do: collect_attempts(count, [])

  defp collect_attempts(0, acc), do: Enum.reverse(acc)

  defp collect_attempts(count, acc) do
    receive do
      {:attempt, uuid, samples} -> collect_attempts(count - 1, [{uuid, samples} | acc])
    after
      @drain_timeout ->
        flunk("timed out waiting for #{count} more write attempt(s); got #{length(acc)} so far")
    end
  end

  # The batches as written, deduplicated by uuid, in first-attempt order. Each
  # distinct batch should appear once here even if retried multiple times.
  defp first_attempt_batches(attempts) do
    {batches, _seen} =
      Enum.reduce(attempts, {[], MapSet.new()}, fn {uuid, samples}, {acc, seen} ->
        if MapSet.member?(seen, uuid),
          do: {acc, seen},
          else: {[samples | acc], MapSet.put(seen, uuid)}
      end)

    Enum.reverse(batches)
  end

  defp attempt_samples(attempts), do: Enum.map(attempts, fn {_uuid, samples} -> samples end)

  # Every attempt sharing a uuid must carry identical samples (retried UNCHANGED),
  # and the number of attempts for each uuid must match the model's attempt rows
  # for the corresponding batch position.
  defp assert_unchanged_retries(attempts, model) do
    groups = Enum.chunk_by(attempts, fn {uuid, _samples} -> uuid end)

    # Attempts for one batch are contiguous (a batch finishes before the next
    # starts), so chunk_by on uuid yields one group per attempted batch.
    Enum.each(groups, fn group ->
      samples = Enum.map(group, fn {_uuid, s} -> s end)
      assert Enum.uniq(samples) |> length() == 1, "a retried batch must reuse the same samples"
    end)

    # Model attempts grouped by batch index give the expected per-batch attempt
    # counts; compare against the writer's per-uuid group sizes in order.
    expected_counts =
      model.attempts
      |> Enum.chunk_by(fn {index, _batch} -> index end)
      |> Enum.map(&length/1)

    actual_counts = Enum.map(groups, &length/1)
    assert actual_counts == expected_counts
  end

  # ----------------------------------------------------------------------------
  # Deterministic examples
  # ----------------------------------------------------------------------------

  test "a permanent batch is dropped and the next FIFO batch follows (34.4, 34.8)" do
    test_pid = self()
    # Batch A ({1,2}) hits a permanent failure; batch B ({3,4}) then succeeds.
    script = [{:permanent, :boom}, :ok]
    {:ok, controller} = Agent.start_link(fn -> script end)

    {:ok, writer} =
      Writer.start_link(
        backend: Backend,
        backend_config: [controller: controller],
        batch_size: 2,
        retry_ms: @retry_ms,
        max_retry_ms: @max_retry_ms,
        batch_observer: fn uuid, samples -> send(test_pid, {:attempt, uuid, samples}) end
      )

    try do
      assert :ok = Writer.enqueue_samples(writer, [1, 2, 3, 4])
      assert Writer.migration_succeeded(writer) == :ok

      attempts = collect_attempts(2)

      # The permanent batch is attempted exactly once (no retry), then the next
      # FIFO batch follows immediately.
      assert attempt_samples(attempts) == [[1, 2], [3, 4]]
      # Two distinct uuids: the permanent batch is NOT re-attempted.
      assert attempts |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> length() == 2

      stats = Writer.stats(writer)
      assert stats.permanent == 1
      assert stats.successful == 1
      assert stats.retryable == 0
      assert stats.queued == 0
    after
      GenServer.stop(writer)
      Agent.stop(controller)
    end
  end

  test "a retryable batch is retried unchanged and the retry delay at least doubles (34.3)" do
    test_pid = self()
    # Two retryables then success for a SINGLE batch. A large retry_ms lets us
    # read retry_ms via :sys.get_state between attempts before the timer fires.
    initial = 1_000
    script = [{:retryable, :a}, {:retryable, :b}, :ok]
    {:ok, controller} = Agent.start_link(fn -> script end)

    {:ok, writer} =
      Writer.start_link(
        backend: Backend,
        backend_config: [controller: controller],
        batch_size: 3,
        retry_ms: initial,
        max_retry_ms: 60_000,
        batch_observer: fn uuid, samples -> send(test_pid, {:attempt, uuid, samples}) end
      )

    try do
      assert :ok = Writer.enqueue_samples(writer, [:x, :y, :z])
      assert Writer.migration_succeeded(writer) == :ok

      # First attempt fires immediately and returns {:retryable, :a}; the writer
      # schedules the retry 1_000 ms out, so we can read state before it fires.
      assert_receive {:attempt, uuid1, [:x, :y, :z]}, @drain_timeout
      retry_ms_after_first = :sys.get_state(writer).retry_ms

      # Shorten the pending timer so the retry fires promptly without waiting a
      # full second, then observe the SECOND attempt (same uuid, same samples).
      hurry_retry(writer)
      assert_receive {:attempt, uuid2, [:x, :y, :z]}, @drain_timeout
      retry_ms_after_second = :sys.get_state(writer).retry_ms

      # Retried UNCHANGED: identical uuid and samples across attempts.
      assert uuid1 == uuid2

      # The scheduled delay at least doubles between successive retries.
      assert retry_ms_after_first >= 2 * initial
      assert retry_ms_after_second >= 2 * retry_ms_after_first

      # Let the final :ok drain the batch.
      hurry_retry(writer)
      assert_receive {:attempt, ^uuid1, [:x, :y, :z]}, @drain_timeout

      stats = Writer.stats(writer)
      assert stats.retryable == 2
      assert stats.successful == 1
      assert stats.permanent == 0
      assert stats.queued == 0
    after
      GenServer.stop(writer)
      Agent.stop(controller)
    end
  end

  test "overflow drops the oldest samples at capacity and the dropped counter matches (34.2, 36.6)" do
    test_pid = self()
    # Empty script => every drain attempt defaults to :ok once ready.
    {:ok, controller} = Agent.start_link(fn -> [] end)

    capacity = 3
    batch_size = capacity

    {:ok, writer} =
      Writer.start_link(
        backend: Backend,
        backend_config: [controller: controller],
        capacity: capacity,
        batch_size: batch_size,
        retry_ms: @retry_ms,
        max_retry_ms: @max_retry_ms,
        batch_observer: fn uuid, samples -> send(test_pid, {:attempt, uuid, samples}) end
      )

    try do
      # The writer starts NOT ready, so nothing drains: a single enqueue of more
      # items than `capacity` overflows, dropping the OLDEST samples first.
      total = [1, 2, 3, 4, 5, 6, 7]
      assert :ok = Writer.enqueue_samples(writer, total)

      # Drop-oldest-at-capacity: the first `capacity` fill the queue, then each
      # later item evicts the oldest, so dropped = max(0, total - capacity) and
      # the SURVIVORS are the newest `capacity` items, in FIFO order.
      expected_dropped = max(0, length(total) - capacity)
      survivors = Enum.take(total, -capacity)

      stats_before = Writer.stats(writer)
      assert stats_before.dropped == expected_dropped
      assert stats_before.queued == capacity
      assert stats_before.ready? == false

      # Release the gate; the single surviving batch drains under the default :ok.
      assert Writer.migration_succeeded(writer) == :ok

      assert_receive {:attempt, _uuid, written}, @drain_timeout
      # The newest samples are the ones written; the dropped oldest never appear.
      assert written == survivors

      stats_after = Writer.stats(writer)
      assert stats_after.dropped == expected_dropped
      assert stats_after.successful == 1
      assert stats_after.queued == 0
    after
      GenServer.stop(writer)
      Agent.stop(controller)
    end
  end

  test "mixed kinds batch same-kind at FIFO boundaries and sample batch_uuid is stamped (34.2)" do
    test_pid = self()
    {:ok, controller} = Agent.start_link(fn -> [] end)

    # A large batch_size means take_batch is bounded only by the kind-change
    # rule, so batch boundaries fall exactly where the queue's kind changes.
    {:ok, writer} =
      Writer.start_link(
        backend: Backend,
        backend_config: [controller: controller],
        batch_size: 10,
        retry_ms: @retry_ms,
        max_retry_ms: @max_retry_ms,
        batch_observer: fn uuid, samples -> send(test_pid, {:attempt, uuid, samples}) end
      )

    # Map samples carry a :batch_uuid key so with_batch_uuid stamps the batch
    # uuid; :kind lets the assertions confirm a batch never mixes kinds.
    s1 = %{kind: :sample, n: 1, batch_uuid: nil}
    s2 = %{kind: :sample, n: 2, batch_uuid: nil}
    s3 = %{kind: :sample, n: 3, batch_uuid: nil}
    s4 = %{kind: :sample, n: 4, batch_uuid: nil}
    series1 = %{kind: :series, n: 1}

    try do
      # Enqueue interleaved kinds in FIFO order while NOT ready:
      #   [s1, s2, series1, s3, s4]
      assert :ok = Writer.enqueue_samples(writer, [s1, s2])
      assert :ok = Writer.enqueue(writer, :series, series1)
      assert :ok = Writer.enqueue_samples(writer, [s3, s4])

      assert Writer.migration_succeeded(writer) == :ok

      # take_batch stops a batch when the kind changes, so three same-kind
      # batches drain in FIFO order: [s1,s2] (sample), [series1] (series),
      # [s3,s4] (sample).
      batches = collect_attempts(3)
      written = attempt_samples(batches)

      # Every batch is same-kind (no batch mixes :sample and :series).
      Enum.each(written, fn batch ->
        kinds = batch |> Enum.map(& &1.kind) |> Enum.uniq()
        assert length(kinds) == 1, "a batch must not mix item kinds"
      end)

      # Boundaries fall exactly where the kind changes, FIFO order preserved.
      assert Enum.map(written, fn batch -> hd(batch).kind end) == [:sample, :series, :sample]
      assert Enum.map(written, fn batch -> Enum.map(batch, & &1.n) end) == [[1, 2], [1], [3, 4]]

      # The sample batches had their :batch_uuid replaced with the batch's uuid;
      # the series batch (no :batch_uuid key) is left untouched.
      [
        {sample_uuid_a, sample_batch_a},
        {_series_uuid, series_batch},
        {sample_uuid_b, sample_batch_b}
      ] =
        batches

      Enum.each(sample_batch_a, fn s -> assert s.batch_uuid == sample_uuid_a end)
      Enum.each(sample_batch_b, fn s -> assert s.batch_uuid == sample_uuid_b end)
      Enum.each(series_batch, fn item -> refute Map.has_key?(item, :batch_uuid) end)

      stats = Writer.stats(writer)
      assert stats.successful == 3
      assert stats.queued == 0
      assert stats.dropped == 0
    after
      GenServer.stop(writer)
      Agent.stop(controller)
    end
  end

  # Cancels the writer's pending retry timer and re-sends :write so the scheduled
  # retry fires immediately, letting the deterministic example advance without
  # waiting for the full (large) retry delay. Reading/clearing retry_timer via
  # :sys.replace_state keeps the writer's own state consistent.
  defp hurry_retry(writer) do
    :sys.replace_state(writer, fn state ->
      if is_reference(state.retry_timer), do: Process.cancel_timer(state.retry_timer)
      %{state | retry_timer: nil}
    end)

    send(writer, :write)
    :ok
  end
end

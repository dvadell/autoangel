defmodule Autoangel.Storage.QueueCapacityPropertyBackend do
  @moduledoc false
  @behaviour Autoangel.Storage

  @impl true
  def init(config), do: {:ok, config[:recipient]}

  @impl true
  def append_samples(pid, samples), do: send(pid, {:written, samples}) && :ok

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
end

defmodule Autoangel.Storage.QueueCapacityRetryBackend do
  @moduledoc """
  A backend whose `append_samples/2` always fails with a `:retryable` outcome.

  Used by the dequeue example: once the writer becomes ready it takes exactly
  one batch off the queue front, attempts to write it, and — because the write
  is retryable — holds that batch in `retry_batch` while scheduling a retry far
  in the future. The batch therefore LEAVES the queue (the queue shrinks by one
  batch) without being re-enqueued, giving us a deterministic dequeue to observe.
  """
  @behaviour Autoangel.Storage

  @impl true
  def init(config), do: {:ok, config[:recipient]}

  @impl true
  def append_samples(pid, samples) do
    send(pid, {:attempted, samples})
    {:retryable, :forced}
  end

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
end

defmodule Autoangel.Storage.QueueCapacityPropertyTest do
  @moduledoc """
  Property 37 (Requirements 34.4, 34.6, 34.7).

  For any capacity and any interleaving of enqueues (Samples and other items)
  and dequeues, the item count never exceeds capacity, the dropped items are
  the oldest Sample items, and the dropped-Samples counter equals the number
  of Sample items dropped.

  The writer is kept NOT ready (migration never signalled) so `dispatch/1` is a
  no-op and nothing drains. Items accumulate in the queue, letting us observe
  the capacity/drop policy deterministically through `Writer.stats/1`.

  An independent reference model (a plain FIFO list replicating the exact
  drop-oldest-sample rule) is run in lock-step with the real writer and the two
  are asserted equal on both `queued` size and `dropped` count.
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Autoangel.Storage.QueueCapacityPropertyBackend, as: Backend
  alias Autoangel.Storage.QueueCapacityRetryBackend, as: RetryBackend
  alias Autoangel.Storage.Writer

  # ----------------------------------------------------------------------------
  # Reference model: a FIFO list of {kind, item} with the writer's drop policy.
  #
  #   * While size < capacity: append any item (sample or not).
  #   * At capacity, a new :sample evicts the OLDEST :sample in the queue, is
  #     itself enqueued, and the dropped-sample counter is incremented.
  #   * At capacity, a new NON-sample evicts the OLDEST :sample (if one exists),
  #     is itself enqueued, and the dropped-sample counter is incremented. Only
  #     when NO sample remains to drop is the non-sample item itself dropped,
  #     and in that case the dropped-sample counter is NOT incremented (it
  #     counts dropped SAMPLES only). This mirrors the corrected writer policy
  #     for Requirements 34.6/34.7 and design.md: "Anomaly, series and band
  #     items are dropped only when no Sample item is left to drop."
  #
  # The list head is the oldest element (FIFO front).
  # ----------------------------------------------------------------------------

  defmodule Model do
    @moduledoc false
    defstruct items: [], capacity: 0, dropped: 0

    def new(capacity), do: %__MODULE__{items: [], capacity: capacity, dropped: 0}

    def put(%__MODULE__{} = model, _kind, []), do: model

    def put(%__MODULE__{} = model, kind, [item | rest]) do
      model |> put_one(kind, item) |> put(kind, rest)
    end

    defp put_one(%__MODULE__{items: items, capacity: capacity} = model, kind, item)
         when length(items) < capacity do
      %{model | items: items ++ [{kind, item}]}
    end

    # At capacity: evict the oldest sample and enqueue the new item. When no
    # sample is left to evict, the new item is dropped. A dropped :sample still
    # increments the dropped-SAMPLE counter; a dropped non-sample does not.
    defp put_one(%__MODULE__{} = model, kind, item) do
      case drop_oldest_sample(model.items) do
        {:ok, remaining} ->
          %{model | items: remaining ++ [{kind, item}], dropped: model.dropped + 1}

        :none ->
          drop_without_sample(model, kind)
      end
    end

    # No sample remained to evict, so the new item is dropped and the queue is
    # unchanged. Dropping a :sample counts; dropping a non-sample does not.
    defp drop_without_sample(%__MODULE__{} = model, :sample),
      do: %{model | dropped: model.dropped + 1}

    defp drop_without_sample(%__MODULE__{} = model, _kind), do: model

    # Removes the first (oldest) :sample entry, preserving the order of the rest.
    defp drop_oldest_sample(items), do: drop_oldest_sample(items, [])

    defp drop_oldest_sample([], _prefix), do: :none

    defp drop_oldest_sample([{:sample, _} | rest], prefix),
      do: {:ok, Enum.reverse(prefix) ++ rest}

    defp drop_oldest_sample([other | rest], prefix),
      do: drop_oldest_sample(rest, [other | prefix])

    def size(%__MODULE__{items: items}), do: length(items)

    # The full ordered queue contents as {kind, item} tuples (oldest first).
    def items(%__MODULE__{items: items}), do: items

    # The surviving :sample terms in FIFO order (oldest first).
    def surviving_samples(%__MODULE__{items: items}) do
      for {:sample, item} <- items, do: item
    end

    # Removes the head batch the writer would dequeue: up to `limit` leading
    # entries that all share the kind of the current head. Mirrors the writer's
    # `take_batch/3` (single-kind contiguous run from the front).
    def drop_head_batch(%__MODULE__{items: []} = model, _limit), do: model

    def drop_head_batch(%__MODULE__{items: [{kind, _} | _] = items} = model, limit) do
      %{model | items: drop_head_run(items, kind, limit)}
    end

    defp drop_head_run(items, _kind, 0), do: items
    defp drop_head_run([{kind, _} | rest], kind, limit), do: drop_head_run(rest, kind, limit - 1)
    defp drop_head_run(items, _kind, _limit), do: items
  end

  # ----------------------------------------------------------------------------
  # Generators
  # ----------------------------------------------------------------------------

  defp op_gen do
    one_of([
      # A batch of n distinct sample terms, tagged so we never collide across ops.
      map(integer(1..5), fn n -> {:sample, n} end),
      map(member_of([:series, :band]), fn kind -> {:other, kind} end)
    ])
  end

  # ----------------------------------------------------------------------------
  # Property
  # ----------------------------------------------------------------------------

  property "queued never exceeds capacity and matches the reference model" do
    check all(
            capacity <- integer(1..20),
            ops <- list_of(op_gen(), max_length: 40)
          ) do
      {:ok, writer} =
        Writer.start_link(
          backend: Backend,
          backend_config: [recipient: self()],
          capacity: capacity
        )

      try do
        # Keep the writer NOT ready: never call migration_succeeded, so dispatch
        # is a no-op and the queue simply accumulates.
        refute Writer.stats(writer).ready?

        {final_model, _seq} =
          Enum.reduce(ops, {Model.new(capacity), 0}, fn op, {model, seq} ->
            {model, seq} = apply_op(writer, model, op, seq)

            stats = Writer.stats(writer)

            # 1. Never exceeds capacity.
            assert stats.queued <= capacity

            # 2. Size/counter match against the independent reference model.
            assert stats.queued == Model.size(model)
            assert stats.dropped == model.dropped

            # 3. CONTENTS match: the real queue, in FIFO order, equals the
            #    model's ordered {kind, item} list. This proves the dropped
            #    items are exactly the oldest SAMPLES and that the survivors
            #    retain FIFO order — not merely that the counts agree.
            assert queue_contents(writer) == Model.items(model)

            {model, seq}
          end)

        # Final cross-check.
        final = Writer.stats(writer)
        assert final.queued == Model.size(final_model)
        assert final.dropped == final_model.dropped
        assert final.queued <= capacity
        assert queue_contents(writer) == Model.items(final_model)
      after
        GenServer.stop(writer)
      end
    end
  end

  # Applies one op to BOTH the real writer and the reference model. `seq` keeps
  # sample terms globally distinct so no two enqueued samples ever collide.
  defp apply_op(writer, model, {:sample, n}, seq) do
    samples = for i <- 0..(n - 1), do: {:s, seq + i}
    assert :ok = Writer.enqueue_samples(writer, samples)
    {Model.put(model, :sample, samples), seq + n}
  end

  defp apply_op(writer, model, {:other, kind}, seq) do
    item = {:o, seq}
    assert :ok = Writer.enqueue(writer, kind, item)
    {Model.put(model, kind, [item]), seq + 1}
  end

  # Reads the writer's real queue (an Erlang :queue of {kind, item} tuples) in
  # FIFO order (oldest first) via introspection, for direct comparison with the
  # reference model's ordered item list.
  defp queue_contents(writer) do
    :queue.to_list(:sys.get_state(writer).queue)
  end

  # ----------------------------------------------------------------------------
  # Deterministic example: the SURVIVING samples are the NEWEST ones; the oldest
  # samples are the ones dropped. Proven by draining the queue once ready and
  # inspecting the written batches.
  # ----------------------------------------------------------------------------

  test "dropped samples are the oldest; the newest samples survive" do
    capacity = 3

    {:ok, writer} =
      Writer.start_link(
        backend: Backend,
        backend_config: [recipient: self()],
        capacity: capacity,
        batch_size: capacity
      )

    try do
      # Fill to capacity with identifiable samples, then overflow with two more.
      # Oldest (:a, :b) must be evicted; (:c, :d, :e) must survive in FIFO order.
      assert :ok = Writer.enqueue_samples(writer, [:a, :b, :c, :d, :e])

      stats = Writer.stats(writer)
      assert stats.ready? == false
      assert stats.queued == capacity
      assert stats.dropped == 2

      # Model agreement before draining.
      model = Model.put(Model.new(capacity), :sample, [:a, :b, :c, :d, :e])
      assert Model.size(model) == stats.queued
      assert model.dropped == stats.dropped
      assert Model.surviving_samples(model) == [:c, :d, :e]

      # Drain and confirm the survivors are exactly the newest samples in order.
      Writer.migration_succeeded(writer)
      assert_receive {:written, [:c, :d, :e]}, 200
      assert Writer.stats(writer).queued == 0
    after
      GenServer.stop(writer)
    end
  end

  test "a non-sample at capacity with no sample to evict is dropped but not counted" do
    capacity = 2

    {:ok, writer} =
      Writer.start_link(
        backend: Backend,
        backend_config: [recipient: self()],
        capacity: capacity
      )

    try do
      # Fill capacity entirely with non-sample items, then push one more.
      assert :ok = Writer.enqueue(writer, :series, {:o, 1})
      assert :ok = Writer.enqueue(writer, :band, {:o, 2})
      assert :ok = Writer.enqueue(writer, :series, {:o, 3})

      stats = Writer.stats(writer)
      assert stats.queued == capacity
      # The overflowing non-sample is dropped, but the dropped counter tracks
      # SAMPLES only, so it stays at zero.
      assert stats.dropped == 0

      model =
        Model.new(capacity)
        |> Model.put(:series, [{:o, 1}])
        |> Model.put(:band, [{:o, 2}])
        |> Model.put(:series, [{:o, 3}])

      assert Model.size(model) == stats.queued
      assert model.dropped == stats.dropped
      assert queue_contents(writer) == Model.items(model)
    after
      GenServer.stop(writer)
    end
  end

  # ----------------------------------------------------------------------------
  # Dequeue coverage (Property 37: "any interleaving of enqueues AND dequeues").
  #
  # Fill the queue while NOT ready, then flip to ready with a backend whose
  # append_samples returns {:retryable, _}. The writer takes exactly ONE batch
  # off the queue front and holds it in `retry_batch` (scheduling a retry far in
  # the future via a large retry_ms), so that batch LEAVES the queue without
  # being re-enqueued. The reference model removes the same head batch, and the
  # real queue contents + stats are asserted to match.
  # ----------------------------------------------------------------------------

  test "a dequeued batch leaves the queue and the remainder stays FIFO" do
    capacity = 6
    batch_size = 2

    {:ok, writer} =
      Writer.start_link(
        backend: RetryBackend,
        backend_config: [recipient: self()],
        capacity: capacity,
        batch_size: batch_size,
        # Large retry delay: the held batch is NOT retried within the test, so
        # the dequeue is observable and deterministic.
        retry_ms: 3_600_000
      )

    try do
      samples = for i <- 1..capacity, do: {:s, i}
      assert :ok = Writer.enqueue_samples(writer, samples)

      before = Writer.stats(writer)
      assert before.ready? == false
      assert before.queued == capacity

      model = Model.put(Model.new(capacity), :sample, samples)
      assert queue_contents(writer) == Model.items(model)

      # Flip to ready: dispatch takes one batch off the front, the retryable
      # write parks it in retry_batch, and a far-future retry is scheduled.
      Writer.migration_succeeded(writer)

      # The front batch was attempted exactly once.
      assert_receive {:attempted, attempted}, 200
      assert length(attempted) == batch_size

      # Model loses the same head batch.
      model = Model.drop_head_batch(model, batch_size)

      after_stats = Writer.stats(writer)
      # One batch dequeued: queued dropped by batch_size.
      assert after_stats.queued == capacity - batch_size
      assert after_stats.queued == Model.size(model)
      assert after_stats.retryable == 1
      # No samples were dropped by the dequeue (overflow never occurred here).
      assert after_stats.dropped == model.dropped

      # The remaining queue equals the model after removing the head batch,
      # preserving FIFO order of the survivors.
      assert queue_contents(writer) == Model.items(model)
      assert queue_contents(writer) == for(i <- (batch_size + 1)..capacity, do: {:sample, {:s, i}})
    after
      GenServer.stop(writer)
    end
  end
end

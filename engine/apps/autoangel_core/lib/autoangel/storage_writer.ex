defmodule Autoangel.Storage.Writer do
  @moduledoc """
  Bounded, asynchronous writer for the configured storage backend.

  Samples are kept as individual queue entries. This makes overflow policy
  explicit: the oldest sample is discarded before any newer sample, while
  non-sample work is retained whenever possible.
  """

  use GenServer
  require Logger

  @default_capacity 200_000
  @default_batch_size 5_000
  @default_retry_ms 500
  @default_max_retry_ms 60_000

  @type counter_name :: :successful | :retryable | :permanent | :dropped
  @type stats :: %{
          counter_name() => non_neg_integer(),
          queued: non_neg_integer(),
          ready?: boolean()
        }

  defstruct backend: nil,
            handle: nil,
            queue: :queue.new(),
            size: 0,
            capacity: @default_capacity,
            batch_size: @default_batch_size,
            retry_ms: @default_retry_ms,
            initial_retry_ms: @default_retry_ms,
            max_retry_ms: @default_max_retry_ms,
            retry_timer: nil,
            retry_batch: nil,
            ready?: false,
            counters: %{successful: 0, retryable: 0, permanent: 0, dropped: 0},
            self_monitor: nil,
            batch_observer: nil

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))

  @spec enqueue_samples(GenServer.server(), [term()]) :: :ok | :not_ready | :full
  def enqueue_samples(server, samples) when is_list(samples) do
    GenServer.call(server, {:enqueue, :sample, samples})
  end

  @spec enqueue(GenServer.server(), atom(), term()) :: :ok | :not_ready | :full
  def enqueue(server, kind, item), do: GenServer.call(server, {:enqueue, kind, [item]})

  @spec stats(GenServer.server()) :: stats()
  def stats(server), do: GenServer.call(server, :stats)

  @spec migration_succeeded(GenServer.server()) :: :ok
  def migration_succeeded(server) do
    send(server, :migration_succeeded)
    :ok
  end

  @spec migration_failed(GenServer.server()) :: :ok
  def migration_failed(server) do
    send(server, :migration_failed)
    :ok
  end

  @impl true
  def init(opts) do
    backend = Keyword.get(opts, :backend)
    config = Keyword.get(opts, :backend_config, Keyword.get(opts, :config, %{}))

    case init_backend(backend, config) do
      {:ok, handle} ->
        ready = Keyword.get(opts, :migration_ready, false)

        state = %__MODULE__{
          backend: backend,
          handle: handle,
          capacity: positive_option(opts, :capacity, @default_capacity),
          batch_size: positive_option(opts, :batch_size, @default_batch_size),
          retry_ms: positive_option(opts, :retry_ms, @default_retry_ms),
          initial_retry_ms: positive_option(opts, :retry_ms, @default_retry_ms),
          max_retry_ms: positive_option(opts, :max_retry_ms, @default_max_retry_ms),
          ready?: ready,
          self_monitor: Keyword.get(opts, :self_monitor),
          batch_observer: Keyword.get(opts, :batch_observer)
        }

        {:ok, state}

      {:error, reason} ->
        {:stop, {:backend_init_failed, reason}}
    end
  end

  @impl true
  def handle_info(:migration_succeeded, state),
    do: {:noreply, %{state | ready?: true} |> dispatch()}

  def handle_info(:migration_failed, state), do: {:noreply, %{state | ready?: false}}

  def handle_info(:write, state), do: {:noreply, dispatch(%{state | retry_timer: nil})}

  def handle_info({:migration, :succeeded}, state), do: handle_info(:migration_succeeded, state)
  def handle_info({:migration, :failed}, state), do: handle_info(:migration_failed, state)

  def handle_info({:samples, samples}, state) when is_list(samples),
    do: {:noreply, enqueue_items(state, :sample, samples) |> dispatch()}

  def handle_info(_message, state), do: {:noreply, state}

  @impl true
  def handle_cast({:samples, samples}, state) when is_list(samples) do
    {:noreply, enqueue_items(state, :sample, samples) |> dispatch()}
  end

  def handle_cast({:enqueue, kind, items}, state) when is_list(items) do
    {:noreply, enqueue_items(state, kind, items) |> dispatch()}
  end

  @impl true
  def handle_call(:stats, _from, state),
    do: {:reply, Map.merge(state.counters, %{queued: state.size, ready?: state.ready?}), state}

  def handle_call({:enqueue, kind, items}, _from, state) when is_list(items) do
    state = enqueue_items(state, kind, items)
    {:reply, :ok, dispatch(state)}
  end

  defp enqueue_items(state, _kind, []), do: state

  defp enqueue_items(state, kind, items) do
    {queue, size, dropped, _accepted} =
      put_items(state.queue, state.size, state.capacity, kind, items)

    state = %{state | queue: queue, size: size} |> increment(:dropped, dropped)
    if dropped > 0, do: Autoangel.Metrics.increment(:samples_dropped, dropped)
    Autoangel.Metrics.set(:storage_queue_depth, size)
    notify_drops(state, dropped)
    notify(state, {:storage_enqueued, dropped == 0})
    state
  end

  defp init_backend(nil, _config), do: {:ok, nil}

  defp init_backend(backend, config) do
    if function_exported?(backend, :init, 1),
      do: backend.init(config),
      else: {:error, :invalid_backend}
  end

  defp put_items(queue, size, capacity, kind, items) do
    Enum.reduce(items, {queue, size, 0, 0}, fn item, acc ->
      put_item(acc, capacity, kind, item)
    end)
  end

  defp put_item({queue, size, dropped, accepted}, capacity, kind, item) when size < capacity,
    do: {:queue.in({kind, item}, queue), size + 1, dropped, accepted + 1}

  defp put_item({queue, size, dropped, accepted}, _capacity, :sample, item) do
    case drop_oldest_sample(queue) do
      {:ok, queue} ->
        {:queue.in({:sample, item}, queue), size, dropped + 1, accepted + 1}

      :none ->
        {queue, size, dropped + 1, accepted}
    end
  end

  # A non-Sample item at capacity still enqueues by evicting the oldest Sample to
  # keep the count at or below capacity (Requirements 34.6, 34.7). Only when no
  # Sample remains to drop is the new item itself dropped.
  defp put_item({queue, size, dropped, accepted}, _capacity, kind, item) do
    case drop_oldest_sample(queue) do
      {:ok, queue} ->
        {:queue.in({kind, item}, queue), size, dropped + 1, accepted + 1}

      :none ->
        {queue, size, dropped, accepted}
    end
  end

  defp drop_oldest_sample(queue), do: drop_oldest_sample(queue, :queue.out(queue), [])

  defp drop_oldest_sample(_queue, {:empty, _}, _prefix), do: :none

  defp drop_oldest_sample(_queue, {{:value, {:sample, _}}, rest}, prefix) do
    {:ok, Enum.reduce(prefix, rest, &:queue.in_r/2)}
  end

  defp drop_oldest_sample(queue, {{:value, item}, rest}, prefix),
    do: drop_oldest_sample(queue, :queue.out(rest), [item | prefix])

  defp dispatch(%{ready?: false} = state), do: state
  defp dispatch(%{retry_timer: timer} = state) when is_reference(timer), do: state

  defp dispatch(%{retry_batch: {uuid, batch}} = state),
    do: finish_batch(state, write_batch(state, batch, uuid), batch, state.queue, state.size, uuid)

  defp dispatch(%{size: 0} = state), do: state

  defp dispatch(state) do
    {batch, queue, size} = take_batch(state.queue, state.size, state.batch_size)
    uuid = uuid()
    result = write_batch(state, batch, uuid)
    finish_batch(state, result, batch, queue, size, uuid)
  end

  defp take_batch(queue, size, limit), do: take_batch(queue, size, limit, [], nil)
  defp take_batch(queue, size, 0, acc, _kind), do: {Enum.reverse(acc), queue, size}

  defp take_batch(queue, size, limit, acc, kind) do
    case :queue.out(queue) do
      {{:value, {item_kind, item}}, queue} when is_nil(kind) or kind == item_kind ->
        take_batch(queue, size - 1, limit - 1, [{item_kind, item} | acc], item_kind)

      _ when acc != [] ->
        {Enum.reverse(acc), queue, size}

      {:empty, queue} ->
        {Enum.reverse(acc), queue, 0}
    end
  end

  defp write_batch(%{backend: nil}, _batch, _uuid), do: :ok

  defp write_batch(%{backend: backend, handle: handle, batch_observer: observer}, batch, uuid) do
    {kind, _} = List.first(batch)
    samples = Enum.map(batch, fn {_kind, item} -> with_batch_uuid(item, uuid) end)
    if is_function(observer, 2), do: observer.(uuid, samples)

    if kind == :sample,
      do: backend.append_samples(handle, samples),
      else: write_other(backend, handle, kind, samples)
  rescue
    exception -> {:retryable, {:exception, exception}}
  catch
    _kind, reason -> {:retryable, {:throw, reason}}
  end

  defp with_batch_uuid(item, uuid) when is_map(item) do
    if Map.has_key?(item, :batch_uuid), do: Map.put(item, :batch_uuid, uuid), else: item
  end

  defp with_batch_uuid(item, _uuid), do: item

  defp write_other(backend, handle, :series, items), do: backend.write_series(handle, items)
  defp write_other(backend, handle, :band, items), do: backend.write_bands(handle, items)
  defp write_other(_backend, _handle, kind, _items), do: {:permanent, {:unknown_kind, kind}}

  defp finish_batch(state, result, batch, queue, size, uuid) do
    finish_batch(state, result, batch, queue, size, uuid, true)
  end

  defp finish_batch(state, :ok, _batch, queue, size, _uuid, _new_batch) do
    notify(state, :database_success)

    Autoangel.Metrics.increment({:storage, :successful})
    Autoangel.Metrics.set(:storage_queue_depth, size)

    state
    |> increment(:successful, 1)
    |> Map.merge(%{queue: queue, size: size, retry_ms: state.initial_retry_ms, retry_batch: nil})
    |> dispatch()
  end

  defp finish_batch(state, {:retryable, reason}, batch, queue, size, uuid, _new_batch) do
    notify(state, {:storage_retryable, reason, length(batch)})
    retry = min(max(state.retry_ms * 2, state.retry_ms + 1), state.max_retry_ms)
    timer = Process.send_after(self(), :write, state.retry_ms)

    Autoangel.Metrics.increment({:storage, :retryable})
    Autoangel.Metrics.set(:storage_queue_depth, size)

    state
    |> increment(:retryable, 1)
    |> Map.merge(%{
      queue: queue,
      size: size,
      retry_ms: retry,
      retry_timer: timer,
      retry_batch: {uuid, batch}
    })
  end

  defp finish_batch(state, {:permanent, reason}, batch, queue, size, _uuid, _new_batch) do
    Logger.error("storage permanent failure: #{inspect(reason)}; dropping #{length(batch)} items")
    notify(state, {:storage_permanent, reason, length(batch)})

    Autoangel.Metrics.increment({:storage, :permanent})
    Autoangel.Metrics.set(:storage_queue_depth, size)

    state
    |> increment(:permanent, 1)
    |> Map.merge(%{queue: queue, size: size, retry_ms: state.initial_retry_ms, retry_batch: nil})
    |> dispatch()
  end

  defp finish_batch(state, other, batch, queue, size, uuid, new_batch),
    do:
      finish_batch(
        state,
        {:permanent, {:invalid_outcome, other}},
        batch,
        queue,
        size,
        uuid,
        new_batch
      )

  defp increment(state, key, amount),
    do: %{state | counters: Map.update!(state.counters, key, &(&1 + amount))}

  defp notify_drops(_state, 0), do: :ok
  defp notify_drops(state, count), do: notify(state, {:storage_dropped, count})
  defp notify(%{self_monitor: pid}, event) when is_pid(pid), do: send(pid, event)

  defp notify(%{self_monitor: name}, event) when is_atom(name) do
    if Process.whereis(name), do: send(name, event), else: :ok
  end

  defp notify(_, _), do: :ok

  defp positive_option(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> value
      _ -> default
    end
  end

  defp uuid, do: :crypto.strong_rand_bytes(16)
end

defmodule Autoangel.Storage.Supervisor do
  @moduledoc "Supervises the isolated Storage_Writer subtree."
  use Supervisor

  def start_link(opts \\ []),
    do: Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @impl true
  def init(opts) do
    writer_name = Keyword.get(opts, :writer_name, Autoangel.Storage.Writer)
    children = [{Autoangel.Storage.Writer, Keyword.put(opts, :name, writer_name)}]
    Supervisor.init(children, strategy: :one_for_one)
  end
end

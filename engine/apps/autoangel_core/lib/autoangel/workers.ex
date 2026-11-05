defmodule Autoangel.Workers do
  @moduledoc "Supervised fixed-size pool of isolated detection workers."
  use Supervisor

  def start_link(opts \\ []),
    do: Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @impl Supervisor
  def init(opts) do
    size = Keyword.get(opts, :pool_size, System.schedulers_online())
    worker_opts = Keyword.drop(opts, [:name, :pool_size, :queue_counter])

    children =
      for index <- 0..(max(size, 1) - 1) do
        Supervisor.child_spec({Autoangel.Workers.Worker, Keyword.put(worker_opts, :index, index)},
          id: {Autoangel.Workers.Worker, index}
        )
      end

    Supervisor.init(children, strategy: :one_for_one)
  end

  def worker(index, supervisor \\ __MODULE__),
    do:
      Supervisor.which_children(supervisor)
      |> Enum.find_value(fn
        {{_, ^index}, pid, _, _} -> pid
        _ -> nil
      end)
end

defmodule Autoangel.Workers.Worker do
  @moduledoc "Owns Series state, scores bounded batches, and runs missing-data sweeps."
  use GenServer
  require Logger
  alias Autoangel.{Detect.Decision, Kernel, Kernel.Params, Kernel.State}
  alias Autoangel.Exposition.Sample

  defstruct index: 0,
            series: %{},
            pending: %{},
            queue_counter: nil,
            queue_capacity: 20_000,
            max_batch: 2,
            kernel_params: %Params{},
            sweep_ref: nil,
            sweep_interval: 15_000,
            scrape_status_table: nil,
            now: nil,
            state_store: nil,
            decision_cfg: %{}

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))

  def enqueue(pid, samples), do: GenServer.cast(pid, {:samples, samples})
  def reserve(pid, count), do: GenServer.call(pid, {:reserve, count})
  def queue_counter(index), do: :persistent_term.get({__MODULE__, index})
  def release(pid, count), do: GenServer.cast(pid, {:release, count})
  def snapshot(pid), do: GenServer.call(pid, :snapshot)

  def put_series(pid, key, kernel_state, meta),
    do: GenServer.call(pid, {:put_series, key, kernel_state, meta})

  def series(pid, key), do: GenServer.call(pid, {:series, key})

  @impl GenServer
  def init(opts) do
    interval = Keyword.get(opts, :sweep_interval, 15_000)

    state = %__MODULE__{
      index: Keyword.get(opts, :index, 0),
      queue_counter: Keyword.get(opts, :queue_counter) || :atomics.new(1, signed: true),
      queue_capacity: Keyword.get(opts, :queue_capacity, 20_000),
      max_batch: Keyword.get(opts, :max_batch, 2),
      kernel_params: Keyword.get(opts, :kernel_params, %Params{}),
      sweep_interval: interval,
      scrape_status_table: Keyword.get(opts, :scrape_status_table),
      now: Keyword.get(opts, :now, fn -> System.system_time(:millisecond) end),
      state_store: Keyword.get(opts, :state_store),
      decision_cfg: Keyword.get(opts, :decision_cfg, %{})
    }

    :persistent_term.put({__MODULE__, state.index}, state.queue_counter)
    {:ok, restore_entries(schedule_sweep(state))}
  end

  @impl GenServer
  def handle_call({:reserve, count}, _from, state) when is_integer(count) and count >= 0 do
    if reserve_counter(state.queue_counter, count, state.queue_capacity),
      do: {:reply, :ok, state},
      else: {:reply, {:error, :full}, state}
  end

  def handle_call({:put_series, key, kernel_state, meta}, _from, state)
      when is_binary(kernel_state) do
    series =
      Map.put(state.series, key, %{
        key: key,
        kind: Map.get(meta, :kind, :gauge) || :gauge,
        kernel_state: kernel_state,
        meta: meta
      })

    {:reply, :ok, %{state | series: series}}
  end

  def handle_call(:snapshot, _from, state) do
    entries =
      Enum.map(state.series, fn {key, series} ->
        meta = Map.put(series.meta, :worker_index, state.index)
        {key, {1, series.kernel_state, :erlang.term_to_binary(meta, [:deterministic])}}
      end)

    {:reply, {:ok, entries}, state}
  end

  def handle_call({:series, key}, _from, state), do: {:reply, Map.get(state.series, key), state}

  @impl GenServer
  def handle_cast({:release, count}, state) when is_integer(count) and count >= 0 do
    decrement(state.queue_counter, count)
    {:noreply, state}
  end

  def handle_cast({:samples, samples}, state) when is_list(samples) do
    decrement(state.queue_counter, length(samples))
    samples = Enum.filter(samples, &valid_sample?/1)
    received_at = state.now.()
    samples = Enum.map(samples, &stamp_sample(&1, received_at))

    pending =
      Enum.reduce(samples, state.pending, fn sample, acc ->
        key = key(sample)
        Map.update(acc, key, [sample], &[sample | &1])
      end)

    {:noreply, process_pending(%{state | pending: pending})}
  end

  def handle_cast(_message, state), do: {:noreply, state}

  @impl GenServer
  def handle_info(:sweep, state) do
    now = state.now.()

    failing_targets = failing_targets(state.scrape_status_table)

    series =
      Enum.reduce(state.series, state.series, fn {key, series_state}, acc ->
        cfg =
          Map.merge(state.decision_cfg, %{
            missing_data: Map.get(state.decision_cfg, :missing_data, %{multiple: 3.0}),
            failing_targets: failing_targets
          })

        {meta, _events} = Decision.step(series_state.meta, :missing_sweep, now, cfg)
        Map.put(acc, key, %{series_state | meta: meta})
      end)

    {:noreply, schedule_sweep(%{state | series: series})}
  end

  defp restore_entries(%{state_store: nil} = state), do: state

  defp restore_entries(%{state_store: store, index: index} = state) do
    case Autoangel.StateStore.restore_worker(store, index) do
      :none ->
        state

      {:error, reason} ->
        Logger.error("checkpoint restore failed for worker #{index}: #{inspect(reason)}")
        state

      {:ok, entries} ->
        restored = Enum.reduce(entries, state.series, &restore_entry/2)
        %{state | series: restored}
    end
  end

  defp restore_entry({key, {1, state_bin, meta_bin}}, series)
       when is_binary(state_bin) and is_binary(meta_bin) do
    with {:ok, decoded} <- State.decode(state_bin),
         {:ok, meta} <- safe_term(meta_bin) do
      Map.put(series, key, %{key: key, kind: decoded.kind, kernel_state: state_bin, meta: meta})
    else
      _ ->
        Logger.error("unreadable checkpoint data for series #{inspect(key)}; starting fresh")
        series
    end
  end

  defp restore_entry({key, _value}, series) do
    Logger.error("unreadable checkpoint data for series #{inspect(key)}; starting fresh")
    series
  end

  defp safe_term(binary) do
    {:ok, :erlang.binary_to_term(binary, [:safe])}
  rescue
    _ -> {:error, :bad_metadata}
  end

  defp process_pending(state) do
    {series, _} =
      Enum.reduce(state.pending, {state.series, []}, fn {key, samples}, {all, _} ->
        samples = samples |> Enum.sort_by(&timestamp/1) |> Enum.chunk_every(state.max_batch)
        current = Map.get(all, key, new_series(key, hd(samples) |> hd()))
        updated = Enum.reduce(samples, current, &score_chunk(&2, &1, state))
        {Map.put(all, key, updated), []}
      end)

    %{state | series: series, pending: %{}}
  end

  defp score_chunk(series, samples, state) do
    tuples = Enum.map(samples, &kernel_sample(&1, series.kind))
    effective_kind = samples |> List.last() |> sample_kind(series.kind)

    case score_kernel(series, tuples, state.kernel_params) do
      {:ok, kernel_state, results} ->
        meta =
          results
          |> Enum.zip(samples)
          |> Enum.map(fn {result, sample} ->
            {Map.merge(result, %{
               kind: sample_kind(sample, effective_kind),
               source: sample_source(sample),
               recv_ms: receive_time(sample, state)
             }), sample}
          end)
          |> Enum.reduce(series.meta, &apply_result(&1, &2, state))

        %{series | kernel_state: kernel_state, meta: meta}

      {:error, reason} ->
        Logger.warning("kernel error for worker series #{inspect(series.key)}: #{inspect(reason)}")

        if reason == :invalid_samples do
          series
        else
          %{series | kernel_state: State.encode(State.new(kind: series.kind))}
        end
    end
  end

  defp score_kernel(series, tuples, params) do
    Kernel.Selected.score(series.kernel_state, tuples, params)
  rescue
    exception -> {:error, {:kernel_exception, exception}}
  catch
    kind, reason -> {:error, {:kernel_throw, kind, reason}}
  end

  defp apply_result({result, sample}, meta, state),
    do: Decision.step(meta, result, receive_time(sample, state), decision_config(state)) |> elem(0)

  defp decision_config(state), do: state.decision_cfg

  defp new_series(key, sample) do
    kind = sample_kind(sample)

    %{
      key: key,
      kind: kind,
      kernel_state: State.encode(State.new(kind: kind)),
      meta: %Decision.Meta{key: key, kind: kind}
    }
  end

  defp sample_kind(%Sample{series_kind: kind}, _fallback) when kind in [:gauge, :counter], do: kind
  defp sample_kind(%{series_kind: kind}, _fallback) when kind in [:gauge, :counter], do: kind
  defp sample_kind({_, _, _, kind}, _fallback) when kind in [:gauge, :counter], do: kind
  defp sample_kind(_, fallback), do: fallback

  defp sample_kind(%Sample{series_kind: kind}) when kind in [:gauge, :counter], do: kind
  defp sample_kind(%{series_kind: kind}) when kind in [:gauge, :counter], do: kind
  defp sample_kind({_, _, _, kind}) when kind in [:gauge, :counter], do: kind
  defp sample_kind(_), do: :gauge

  defp stamp_sample(%Sample{timestamp: nil, received_at: nil} = sample, timestamp),
    do: %{sample | timestamp: timestamp, received_at: timestamp}

  defp stamp_sample(%Sample{timestamp: nil, received_at: received_at} = sample, _now)
       when is_integer(received_at),
       do: %{sample | timestamp: received_at}

  defp stamp_sample(%Sample{received_at: nil} = sample, received_at),
    do: %{sample | received_at: received_at}

  defp stamp_sample(%Sample{} = sample, _received_at), do: sample

  defp stamp_sample(%{timestamp: nil, received_at: nil} = sample, timestamp),
    do: Map.merge(sample, %{timestamp: timestamp, received_at: timestamp})

  defp stamp_sample(%{timestamp: nil, received_at: received_at} = sample, _now)
       when is_integer(received_at),
       do: Map.put(sample, :timestamp, received_at)

  defp stamp_sample(%{received_at: nil} = sample, received_at),
    do: Map.put(sample, :received_at, received_at)

  defp stamp_sample(%{} = sample, _received_at), do: sample

  defp stamp_sample({key, value, nil, kind}, timestamp), do: {key, value, timestamp, kind}
  defp stamp_sample(sample, _timestamp), do: sample

  defp sample_source(%Sample{} = sample), do: Map.get(sample, :source)
  defp sample_source(%{} = sample), do: Map.get(sample, :source)
  defp sample_source(_), do: nil

  defp valid_sample?(%Sample{name: name, labels: labels, value: value, timestamp: timestamp})
       when is_binary(name) and (is_map(labels) or is_list(labels)) and
              (is_number(value) or value in [:nan, :inf, :neg_inf]) and
              (is_integer(timestamp) or is_nil(timestamp)),
       do: true

  defp valid_sample?(%Sample{}), do: false

  defp valid_sample?(%{name: name, labels: labels, value: value, timestamp: timestamp})
       when is_binary(name) and is_map(labels) and
              (is_number(value) or value in [:nan, :inf, :neg_inf]) and
              (is_integer(timestamp) or is_nil(timestamp)),
       do: true

  defp valid_sample?(%{name: name, labels: labels, value: value})
       when is_binary(name) and is_map(labels) and
              (is_number(value) or value in [:nan, :inf, :neg_inf]),
       do: true

  defp valid_sample?({{_name, _labels}, value, ts, kind})
       when (is_number(value) or value in [:nan, :inf, :neg_inf]) and
              (is_integer(ts) or is_nil(ts)) and kind in [:gauge, :counter, nil],
       do: true

  defp valid_sample?(_), do: false

  defp key(%Sample{name: name, labels: labels}),
    do: Autoangel.SeriesRegistry.canonical_key(name, labels)

  defp key(%{name: name, labels: labels}), do: Autoangel.SeriesRegistry.canonical_key(name, labels)
  defp key({key, _value, _ts, _kind}), do: key
  defp timestamp(%Sample{timestamp: ts}) when is_integer(ts), do: ts
  defp timestamp(%{timestamp: ts}) when is_integer(ts), do: ts
  defp timestamp({_, _, ts, _}), do: ts
  defp timestamp(_), do: 0

  defp receive_time(%Sample{received_at: received_at}, _state) when is_integer(received_at),
    do: received_at

  defp receive_time(%{received_at: received_at}, _state) when is_integer(received_at),
    do: received_at

  defp receive_time(_sample, state), do: state.now.()

  defp kernel_sample(%Sample{timestamp: ts, value: value, series_kind: kind}, fallback),
    do: {ts || System.system_time(:millisecond), value, kind || fallback}

  defp kernel_sample(%{timestamp: ts, value: value, series_kind: kind}, fallback),
    do: {ts || System.system_time(:millisecond), value, kind || fallback}

  defp kernel_sample(%{timestamp: ts, value: value}, fallback),
    do: {ts || System.system_time(:millisecond), value, fallback}

  defp kernel_sample({_, value, ts, nil}, fallback), do: {ts, value, fallback}
  defp kernel_sample({_, value, ts, kind}, _), do: {ts, value, kind}

  defp schedule_sweep(state),
    do: %{state | sweep_ref: Process.send_after(self(), :sweep, state.sweep_interval)}

  defp reserve_counter(nil, _count, _capacity), do: true

  defp reserve_counter(counter, count, capacity) do
    current = :atomics.get(counter, 1)

    cond do
      current + count > capacity -> false
      :atomics.compare_exchange(counter, 1, current, current + count) == :ok -> true
      true -> reserve_counter(counter, count, capacity)
    end
  end

  defp decrement(nil, _), do: :ok
  defp decrement(counter, count), do: :atomics.add(counter, 1, -count)

  defp failing_targets(nil), do: %{}

  defp failing_targets(table) do
    table
    |> :ets.tab2list()
    |> Enum.reduce(%{}, fn
      {target, status}, acc -> Map.put(acc, target, status in [:failing, true])
      _, acc -> acc
    end)
  catch
    :error, :badarg -> %{}
  end
end

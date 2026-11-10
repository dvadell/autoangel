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
  alias Autoangel.{Alert.Dispatcher, Detect.Decision, Kernel, Kernel.Params, Kernel.State}
  alias Autoangel.Exposition.Sample
  alias Autoangel.Storage.{Rows, Writer}

  defstruct index: 0,
            series: %{},
            pending: %{},
            queue_counter: nil,
            queue_capacity: 20_000,
            max_batch: 2,
            kernel_params: %Params{},
            sweep_ref: nil,
            sweep_timer?: true,
            sweep_interval: 15_000,
            scrape_status_table: nil,
            now: nil,
            state_store: nil,
            decision_cfg: %{},
            dispatcher: Dispatcher,
            correlator: Autoangel.Correlate,
            storage_writer: nil

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))

  def enqueue(pid, samples), do: GenServer.cast(pid, {:samples, samples})

  @spec try_reserve(non_neg_integer(), non_neg_integer(), pos_integer()) :: :ok | {:error, :full}
  def try_reserve(index, count, capacity)
      when is_integer(index) and index >= 0 and is_integer(count) and count >= 0 and
             is_integer(capacity) and capacity > 0 do
    case queue_counter(index) do
      nil ->
        :ok

      counter ->
        limit = :persistent_term.get({__MODULE__, :capacity, index}, capacity)
        if reserve_counter(counter, count, limit), do: :ok, else: {:error, :full}
    end
  end

  @doc "Releases capacity previously reserved for a Worker queue."
  @spec release_reserved(non_neg_integer(), non_neg_integer()) :: :ok
  def release_reserved(index, count) when is_integer(index) and count >= 0 do
    case queue_counter(index) do
      nil -> :ok
      counter -> decrement(counter, count)
    end

    :ok
  end

  @spec queue_counter(non_neg_integer()) :: :atomics.atomics_ref() | nil
  def queue_counter(index), do: :persistent_term.get({__MODULE__, index}, nil)

  def reserve(pid, count), do: GenServer.call(pid, {:reserve, count})
  def release(pid, count), do: GenServer.cast(pid, {:release, count})
  def snapshot(pid), do: GenServer.call(pid, :snapshot)

  def put_series(pid, key, kernel_state, meta),
    do: GenServer.call(pid, {:put_series, key, kernel_state, meta})

  def series(pid, key), do: GenServer.call(pid, {:series, key})

  @doc """
  Sets the per-Series sensitivity override used by `Decision` (Requirement 30.2).
  Returns `:error` when this Worker does not hold the Series.
  """
  @spec set_sensitivity(GenServer.server(), term(), number()) :: :ok | :error
  def set_sensitivity(pid, key, value) when is_number(value),
    do: GenServer.call(pid, {:set_sensitivity, key, value})

  @doc "Returns and clears the latest baseline band recorded per Series."
  @spec take_bands(GenServer.server()) :: [map()]
  def take_bands(pid), do: GenServer.call(pid, :take_bands)

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
      sweep_timer?: Keyword.get(opts, :sweep_timer, true),
      scrape_status_table: Keyword.get(opts, :scrape_status_table),
      now: Keyword.get(opts, :now, fn -> System.system_time(:millisecond) end),
      state_store: Keyword.get(opts, :state_store),
      decision_cfg: Keyword.get(opts, :decision_cfg, %{}),
      dispatcher: Keyword.get(opts, :dispatcher, Dispatcher),
      correlator: Keyword.get(opts, :correlator, Autoangel.Correlate),
      storage_writer: Keyword.get(opts, :storage_writer)
    }

    :persistent_term.put({__MODULE__, state.index}, state.queue_counter)
    :persistent_term.put({__MODULE__, :capacity, state.index}, state.queue_capacity)
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

  def handle_call({:set_sensitivity, key, value}, _from, state) do
    case Map.fetch(state.series, key) do
      {:ok, entry} ->
        meta = Map.put(entry.meta, :sensitivity_override, value)
        {:reply, :ok, %{state | series: Map.put(state.series, key, %{entry | meta: meta})}}

      :error ->
        {:reply, :error, state}
    end
  end

  def handle_call(:take_bands, _from, state) do
    {bands, series} =
      Enum.reduce(state.series, {[], state.series}, fn
        {key, %{band: band} = entry}, {bands, acc} when not is_nil(band) ->
          {[band | bands], Map.put(acc, key, %{entry | band: nil})}

        _other, acc ->
          acc
      end)

    {:reply, bands, %{state | series: series}}
  end

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

        series_state = ensure_stored(series_state, state)
        {meta, events} = Decision.step(series_state.meta, :missing_sweep, now, cfg)
        forward_events(state, events)
        store_status_changes(state, meta, events)
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
        restore_sensitivities(%{state | series: restored})
    end
  end

  # Per-Series overrides are persisted synchronously by Feedback, possibly after
  # the last checkpoint; the State_Store record is authoritative on restore.
  defp restore_sensitivities(%{state_store: store} = state) do
    case Autoangel.StateStore.records(store, :sensitivity) do
      [] ->
        state

      records when is_list(records) ->
        overrides = Map.new(records)
        %{state | series: Map.new(state.series, &with_override(&1, overrides))}

      {:error, reason} ->
        Logger.error("sensitivity override restore failed: #{inspect(reason)}")
        state
    end
  end

  defp with_override({key, entry}, overrides) do
    case Map.fetch(overrides, series_id(key)) do
      {:ok, value} when is_number(value) ->
        {key, %{entry | meta: Map.put(entry.meta, :sensitivity_override, value)}}

      _ ->
        {key, entry}
    end
  end

  defp series_id(key) do
    Autoangel.SeriesRegistry.series_id(key)
  rescue
    FunctionClauseError -> nil
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

        current =
          all
          |> Map.get_lazy(key, fn -> new_series(key, hd(samples) |> hd(), state) end)
          |> ensure_stored(state)

        updated = Enum.reduce(samples, current, &score_chunk(&2, &1, state))
        {Map.put(all, key, updated), []}
      end)

    %{state | series: series, pending: %{}}
  end

  defp score_chunk(series, samples, state) do
    tuples = Enum.map(samples, &kernel_sample(&1, series.kind))
    effective_kind = samples |> List.last() |> sample_kind(series.kind)

    started = System.monotonic_time(:microsecond)

    result = score_kernel(series, tuples, state.kernel_params)
    duration = System.monotonic_time(:microsecond) - started
    Autoangel.Metrics.observe(:nif_call_duration_seconds, duration / 1_000_000)

    case result do
      {:ok, kernel_state, results} ->
        Enum.each(samples, fn sample ->
          accepted = receive_time(sample, state)
          latency = max(System.system_time(:millisecond) - accepted, 0) / 1_000
          Autoangel.Metrics.observe(:detection_latency_seconds, latency)
        end)

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
        |> Map.put(:band, latest_band(series, results, samples))

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

  defp apply_result({result, sample}, meta, state) do
    previous_kind = Map.get(meta, :kind)

    {meta, events} =
      Decision.step(meta, result, receive_time(sample, state), decision_config(state))

    forward_events(state, events)

    events =
      if meta.kind != previous_kind,
        do: events ++ [{:status, meta.status}],
        else: events

    store_status_changes(state, meta, events)
    meta
  end

  # Status changes update the stored Series row (Requirement 38.2).
  defp store_status_changes(state, meta, events) do
    Enum.each(events, fn
      {:status, status} -> store(state, :series, Rows.series(meta.key, meta.kind, status))
      _ -> :ok
    end)
  end

  # The most recent band from a scored chunk, kept until the next checkpoint
  # flush; earlier bands of the same interval are superseded (Requirement 53.1).
  defp latest_band(series, results, samples) do
    results
    |> Enum.zip(samples)
    |> Enum.filter(fn {r, _} -> is_number(Map.get(r, :lower)) and is_number(Map.get(r, :upper)) end)
    |> List.last()
    |> case do
      nil ->
        Map.get(series, :band)

      {r, sample} ->
        case Map.get(r, :ts) || timestamp(sample) do
          ts when is_integer(ts) and ts > 0 ->
            Rows.band(series.key, ts, r.lower, r.upper, Map.get(r, :baseline_type) || :robust)

          _ ->
            Map.get(series, :band)
        end
    end
  end

  defp decision_config(state), do: state.decision_cfg

  # Forwards detection lifecycle events toward alert delivery. An
  # `:anomaly_started` event is routed to the Correlator, which enriches it with
  # correlation Candidates, gates on the learning flag and forwards to the
  # Dispatcher; `:anomaly_resolved` is relayed through the Correlator too, to keep start/resolve order. Other events
  # are ignored. Forwarding is a pure side-effect: it never touches `meta` or
  # `kernel_state`, so detection state is identical whether or not the Correlator
  # or Dispatcher is wired. When a target process is not running (e.g. unit tests
  # or a Topology without the alert tree) this is a no-op, and a transient
  # `:exit` during the cast is swallowed.
  defp forward_events(state, events) do
    Enum.each(events, fn
      {:anomaly_started, anomaly} -> forward_started(state, anomaly)
      {:anomaly_resolved, anomaly} -> forward_resolved(state, anomaly)
      _ -> :ok
    end)

    :ok
  end

  # An :anomaly_started is enriched with correlation Candidates by the
  # Correlator, which also gates on the learning flag before forwarding to the
  # Dispatcher. When no Correlator runs, fall back to forwarding the bare
  # anomaly directly so an alert can still reach delivery.
  defp forward_started(%{correlator: correlator} = state, anomaly)
       when not is_nil(correlator) do
    if process_alive?(correlator) do
      try do
        Autoangel.Correlate.anomaly_started(correlator, anomaly)
      catch
        :exit, _reason -> :ok
      end
    else
      dispatch_direct(state, {:anomaly_started, anomaly})
    end
  end

  defp forward_started(state, anomaly),
    do: dispatch_direct(state, {:anomaly_started, anomaly})

  # An :anomaly_resolved is routed through the Correlator as well (when it runs)
  # so that, for a given Series, a start and its later resolution reach the
  # Dispatcher in the order the Worker produced them. Routing resolutions
  # directly while starts go via the Correlator would let a resolution overtake
  # its start and strand an Incident open. Falls back to the direct path.
  defp forward_resolved(%{correlator: correlator} = state, anomaly)
       when not is_nil(correlator) do
    if process_alive?(correlator) do
      try do
        Autoangel.Correlate.anomaly_resolved(correlator, anomaly)
      catch
        :exit, _reason -> :ok
      end
    else
      dispatch_direct(state, {:anomaly_resolved, anomaly})
    end
  end

  defp forward_resolved(state, anomaly),
    do: dispatch_direct(state, {:anomaly_resolved, anomaly})

  defp dispatch_direct(%{dispatcher: nil}, _event), do: :ok

  defp dispatch_direct(%{dispatcher: dispatcher}, event) do
    if process_alive?(dispatcher), do: send_event(dispatcher, event)
    :ok
  end

  defp process_alive?(pid) when is_pid(pid), do: Process.alive?(pid)
  defp process_alive?(name) when is_atom(name), do: is_pid(Process.whereis(name))
  defp process_alive?(_), do: false

  defp send_event(dispatcher, event) do
    Dispatcher.event(dispatcher, event)
  catch
    :exit, _reason -> :ok
  end

  defp new_series(key, sample, state) do
    kind = sample_kind(sample)

    meta = %Decision.Meta{
      key: key,
      kind: kind,
      sensitivity_override: persisted_override(state, key)
    }

    # The Series row is enqueued at creation, ahead of every dependent item.
    store(state, :series, Rows.series(key, kind, meta.status))

    %{
      key: key,
      kind: kind,
      kernel_state: State.encode(State.new(kind: kind)),
      meta: meta,
      band: nil,
      stored?: true
    }
  end

  # A Series restored from a checkpoint (or held across a Writer restart) may
  # have no stored row. Enqueue it once, before its first dependent item (band,
  # anomaly), so foreign keys always resolve. Idempotent upsert on the backend.
  defp ensure_stored(%{stored?: true} = series, _state), do: series
  defp ensure_stored(series, %{storage_writer: nil}), do: series

  defp ensure_stored(series, state) do
    meta = series.meta

    store(
      state,
      :series,
      Rows.series(
        series.key,
        Map.get(meta, :kind) || series.kind,
        Map.get(meta, :status) || :learning
      )
    )

    Map.put(series, :stored?, true)
  end

  # A "this was normal" override persisted before this Worker created the Series.
  defp persisted_override(%{state_store: nil}, _key), do: nil

  defp persisted_override(%{state_store: store}, key) do
    case series_id(key) && Autoangel.StateStore.get_record(store, {:sensitivity, series_id(key)}) do
      {:ok, value} when is_number(value) -> value
      _ -> nil
    end
  catch
    :exit, _ -> nil
  end

  # Enqueue a storage item when a Storage_Writer is configured (never blocks).
  defp store(%{storage_writer: nil}, _kind, _item), do: :ok

  defp store(%{storage_writer: writer}, kind, item) do
    if process_alive?(writer), do: Writer.enqueue_async(writer, kind, item)
    :ok
  catch
    :exit, _reason -> :ok
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

  # One sweep timer at a time: any pending timer is cancelled before a new one
  # is set. With `sweep_timer: false` (simulated time) no timer runs and the
  # driver of simulated time sends `:sweep` itself.
  defp schedule_sweep(%{sweep_timer?: false} = state), do: state

  defp schedule_sweep(state) do
    if state.sweep_ref, do: Process.cancel_timer(state.sweep_ref)
    %{state | sweep_ref: Process.send_after(self(), :sweep, state.sweep_interval)}
  end

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

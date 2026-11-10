defmodule Autoangel.Correlate do
  @moduledoc """
  In-memory co-anomaly Correlator (Requirements 18, 19, 20).

  A GenServer owning two ETS tables:

    * an ordered-set Anomaly_Event_Index keyed `{start_ts, series_key, anomaly_id}`
      whose value is the Anomaly_Score, pruned when older than
      `now - 2 * Correlation_Window`; and
    * a pair co-occurrence table keyed `{min_key, max_key}` whose value is a
      `{count, last_tick}` pair, with LRU eviction once it exceeds the configured
      cap (default 100,000 pairs).

  On an `anomaly_started` event the Correlator inserts the event, selects one
  Candidate per other Series inside the inclusive Correlation_Window, computes
  the ranking factors, ranks the Candidates deterministically, computes evidence
  and the "weak evidence" marker, increments the pair co-occurrence counts, then
  forwards `{:anomaly_started, anomaly, candidates}` to the Dispatcher only when
  the Anomaly is not learning, and — whenever storage is configured — enqueues
  the Anomaly together with every Candidate to the Storage_Writer.

  The ranking and marking logic is split into pure functions
  (`select_candidates/4`, `rank/1`, `mark_weak/3`) so it can be property-tested
  without ETS or any process. The Correlator never reads TimescaleDB
  (Requirement 18.6): candidate lists are produced in every topology.
  """

  use GenServer

  alias Autoangel.Alert.Dispatcher
  alias Autoangel.SeriesRegistry
  alias Autoangel.Storage.Writer

  @default_window_ms 10 * 60 * 1_000
  @default_pool_limit 20
  @default_support_threshold 2
  @default_pair_cap 100_000

  # Ranking-factor weights (design §Correlator). Every weight is strictly
  # positive so a Candidate better in exactly one factor and equal in the rest
  # has strictly higher strength (Requirement 18.5).
  @w_prox 0.35
  @w_before 0.15
  @w_mag 0.20
  @w_shared 0.15
  @w_hist 0.15

  @shared_labels ["host", "job", "service"]
  @hist_denominator_offset 5

  @type series_key :: {String.t(), [{String.t(), String.t()}]}

  @typedoc "A selected, ranked Candidate for an Anomaly."
  @type candidate :: %{
          series_key: series_key(),
          rank: pos_integer(),
          evidence: map(),
          weak?: boolean()
        }

  @typedoc "An index event the Correlator reasons over."
  @type event :: %{
          series_key: series_key(),
          start_ms: integer(),
          score: number(),
          event_like?: boolean()
        }

  defstruct index: nil,
            pairs: nil,
            window_ms: @default_window_ms,
            pool_limit: @default_pool_limit,
            support_threshold: @default_support_threshold,
            pair_cap: @default_pair_cap,
            pair_tick: 0,
            dispatcher: Autoangel.Alert.Dispatcher,
            storage_writer: nil,
            now: &__MODULE__.current_time/0

  @type option ::
          {:name, GenServer.server()}
          | {:window_ms, pos_integer()}
          | {:pool_limit, pos_integer()}
          | {:support_threshold, non_neg_integer()}
          | {:pair_cap, pos_integer()}
          | {:dispatcher, GenServer.server() | nil}
          | {:storage_writer, GenServer.server() | nil}
          | {:now, (-> integer())}

  @spec current_time() :: integer()
  def current_time, do: System.system_time(:millisecond)

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @doc """
  Handle an Anomaly start asynchronously (hot path): index it, compute the
  Candidate list, forward to the Dispatcher and Storage_Writer.
  """
  @spec anomaly_started(GenServer.server(), map()) :: :ok
  def anomaly_started(server \\ __MODULE__, anomaly) when is_map(anomaly),
    do: GenServer.cast(server, {:anomaly_started, anomaly})

  @doc """
  Synchronously index an Anomaly start and return the computed Candidate list.
  Used by tests and callers that need the result; has the same effects as
  `anomaly_started/2`.
  """
  @spec correlate(GenServer.server(), map()) :: [candidate()]
  def correlate(server \\ __MODULE__, anomaly) when is_map(anomaly),
    do: GenServer.call(server, {:correlate, anomaly})

  @doc """
  Relay an Anomaly resolution to the Dispatcher.

  Resolutions carry no Candidates, but they are routed through the Correlator so
  that for a given Series the start and its later resolution reach the Dispatcher
  in the order the Worker produced them (both travel Worker → Correlator →
  Dispatcher). Routing a resolution directly to the Dispatcher while starts go
  via the Correlator would allow a resolution to overtake its start.
  """
  @spec anomaly_resolved(GenServer.server(), map()) :: :ok
  def anomaly_resolved(server \\ __MODULE__, anomaly) when is_map(anomaly),
    do: GenServer.cast(server, {:anomaly_resolved, anomaly})

  @impl true
  def init(opts) do
    with {:ok, pool_limit} <-
           validate_pool_limit(Keyword.get(opts, :pool_limit, @default_pool_limit)),
         {:ok, support} <-
           validate_support(Keyword.get(opts, :support_threshold, @default_support_threshold)) do
      state = %__MODULE__{
        index: :ets.new(:autoangel_correlate_events, [:ordered_set, :private]),
        pairs: :ets.new(:autoangel_correlate_pairs, [:set, :private]),
        window_ms: positive_opt(opts, :window_ms, @default_window_ms),
        pool_limit: pool_limit,
        support_threshold: support,
        pair_cap: positive_opt(opts, :pair_cap, @default_pair_cap),
        dispatcher: Keyword.get(opts, :dispatcher, Autoangel.Alert.Dispatcher),
        storage_writer: Keyword.get(opts, :storage_writer),
        now: Keyword.get(opts, :now, &__MODULE__.current_time/0)
      }

      {:ok, state}
    else
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_cast({:anomaly_started, anomaly}, state) do
    {_candidates, state} = process(anomaly, state)
    {:noreply, state}
  end

  def handle_cast({:anomaly_resolved, anomaly}, state) do
    relay_resolved(anomaly, state)
    {:noreply, state}
  end

  @impl true
  def handle_call({:correlate, anomaly}, _from, state) do
    {candidates, state} = process(anomaly, state)
    {:reply, candidates, state}
  end

  # --- core processing ------------------------------------------------------

  defp process(anomaly, state) do
    event = to_event(anomaly)
    state = prune(state, event.start_ms)
    insert_event(state.index, event)

    window = state.window_ms

    selected =
      state.index
      |> events_in_window(event.start_ms, window)
      |> select_candidates(event.series_key, event.start_ms, window)

    candidates =
      selected
      |> Enum.map(fn candidate_event ->
        build_candidate(candidate_event, event, state)
      end)
      |> rank()
      |> mark_weak(length(selected), state)

    # Increment pair co-occurrence AFTER evidence has been computed, so the
    # support recorded for this Anomaly counts only earlier co-occurrences
    # (design §Correlator step 5).
    state =
      Enum.reduce(selected, bump_tick(state), fn candidate_event, acc ->
        increment_pair(acc, event.series_key, candidate_event.series_key)
      end)

    forward(anomaly, candidates, state)
    {candidates, state}
  end

  # --- candidate selection (pure) -------------------------------------------

  @doc """
  Select one Candidate event per other Series within the inclusive window.

  Given the events whose start lies in `[target_start - w, target_start + w]`,
  drop the target Series, group by Series and keep for each the event whose
  start is closest to `target_start`, breaking ties by earlier start and then by
  higher score (Requirements 18.3, 18.7). The result is deterministic and
  contains each other Series at most once.
  """
  @spec select_candidates([event()], series_key(), integer(), pos_integer()) :: [event()]
  def select_candidates(events, target_key, target_start, window) do
    events
    |> Enum.filter(fn e ->
      e.series_key != target_key and abs(e.start_ms - target_start) <= window
    end)
    |> Enum.group_by(& &1.series_key)
    |> Enum.map(fn {_key, series_events} ->
      Enum.min_by(
        series_events,
        fn e -> {abs(e.start_ms - target_start), e.start_ms, -score_number(e.score)} end
      )
    end)
  end

  # --- ranking (pure) -------------------------------------------------------

  @doc """
  Rank Candidates by descending strength, assigning ranks 1..n.

  Candidates are maps carrying a `:factors` tuple `{prox, before, mag, shared,
  hist}` and a `:series_id`. Sorting is by strength descending, then by the
  factor tuple descending (guarding against floating-point rounding hiding a
  one-factor difference), then by `series_id` ascending for determinism
  (Requirements 18.4, 18.5).
  """
  @spec rank([map()]) :: [map()]
  def rank(candidates) do
    candidates
    |> Enum.sort_by(&rank_sort_key/1)
    |> Enum.with_index(1)
    |> Enum.map(fn {candidate, rank} -> Map.put(candidate, :rank, rank) end)
  end

  # Sort key: Erlang term order sorts ascending, so negate the strength and
  # factor components to get descending order, and keep series_id ascending.
  defp rank_sort_key(%{strength: strength, factors: {prox, before, mag, shared, hist}} = c) do
    {-strength, -prox, -before, -mag, -shared, -hist, Map.get(c, :series_id, <<>>)}
  end

  # --- weak marking (pure) --------------------------------------------------

  @doc """
  Mark Candidates as "weak evidence" when the selected pool exceeds the pool
  limit and the Candidate's support is below the support threshold; otherwise
  mark none (Requirements 19.1, 19.2, 19.3). Applies identically to event-like
  and non-event-like Candidates (Requirement 20.4).
  """
  @spec mark_weak(
          [map()],
          non_neg_integer(),
          map() | %{pool_limit: pos_integer(), support_threshold: non_neg_integer()}
        ) ::
          [candidate()]
  def mark_weak(candidates, selected_count, %{
        pool_limit: pool_limit,
        support_threshold: support_threshold
      }) do
    over_pool? = selected_count > pool_limit

    Enum.map(candidates, fn candidate ->
      support = get_in(candidate, [:evidence, :support]) || 0
      weak? = over_pool? and support < support_threshold

      candidate
      |> Map.put(:weak?, weak?)
      |> Map.update!(:evidence, &Map.put(&1, :weak?, weak?))
      |> Map.take([:series_key, :rank, :evidence, :weak?])
    end)
  end

  # --- candidate construction -----------------------------------------------

  defp build_candidate(candidate_event, target, state) do
    window = state.window_ms
    delta_ms = candidate_event.start_ms - target.start_ms
    before? = delta_ms < 0
    support = pair_count(state, target.series_key, candidate_event.series_key)
    shared = shared_label_values(target.series_key, candidate_event.series_key)
    event_like? = candidate_event.event_like? or target.event_like?

    prox = 1.0 - abs(delta_ms) / window
    before = if before?, do: 1.0, else: 0.0
    mag = score_number(candidate_event.score) / 100.0
    shared_factor = length(shared) / length(@shared_labels)
    hist = support / (support + @hist_denominator_offset)

    strength =
      @w_prox * prox + @w_before * before + @w_mag * mag + @w_shared * shared_factor +
        @w_hist * hist

    evidence = %{
      delta_ms: delta_ms,
      before?: before?,
      score: score_number(candidate_event.score),
      shared_labels: shared,
      support: support,
      co_occurrence_window_ms: window,
      event_like?: event_like?,
      weak?: false
    }

    %{
      series_key: candidate_event.series_key,
      series_id: SeriesRegistry.series_id(candidate_event.series_key),
      strength: strength,
      factors: {prox, before, mag, shared_factor, hist},
      evidence: evidence
    }
  end

  defp shared_label_values({_, target_labels}, {_, candidate_labels}) do
    target = Map.new(target_labels)
    candidate = Map.new(candidate_labels)

    Enum.filter(@shared_labels, fn label ->
      case {Map.fetch(target, label), Map.fetch(candidate, label)} do
        {{:ok, value}, {:ok, value}} -> true
        _ -> false
      end
    end)
  end

  # --- ETS index ------------------------------------------------------------

  defp insert_event(index, event) do
    :ets.insert(index, {{event.start_ms, event.series_key, event.anomaly_id}, event})
  end

  # Select all events whose start lies in the inclusive window. The ordered-set
  # key begins with start_ms, so a bounded select on the first tuple element is
  # a range scan.
  defp events_in_window(index, start_ms, window) do
    low = start_ms - window
    high = start_ms + window

    :ets.select(index, [
      {{{:"$1", :_, :_}, :"$2"}, [{:andalso, {:>=, :"$1", low}, {:"=<", :"$1", high}}], [:"$2"]}
    ])
  end

  # Prune events older than now - 2 * window (Requirement 18.8: kept for at least
  # one window after their start; we keep two, so a target at the far edge of the
  # window still sees them). "now" is the wall clock, bounded below by the
  # incoming event's start so a single skewed future timestamp cannot wipe the
  # index of still-relevant neighbours.
  defp prune(state, event_start_ms) do
    now = min(state.now.(), event_start_ms)
    cutoff = now - 2 * state.window_ms

    :ets.select_delete(state.index, [
      {{{:"$1", :_, :_}, :_}, [{:<, :"$1", cutoff}], [true]}
    ])

    state
  end

  # --- pair co-occurrence table (LRU) ---------------------------------------

  defp pair_count(state, a, b) do
    case :ets.lookup(state.pairs, pair_key(a, b)) do
      [{_key, {count, _tick}}] -> count
      [] -> 0
    end
  end

  defp increment_pair(state, a, b) do
    key = pair_key(a, b)
    tick = state.pair_tick

    case :ets.lookup(state.pairs, key) do
      [{^key, {count, _old_tick}}] ->
        :ets.insert(state.pairs, {key, {count + 1, tick}})
        state

      [] ->
        :ets.insert(state.pairs, {key, {1, tick}})
        evict_if_over_cap(state)
    end
  end

  defp evict_if_over_cap(state) do
    if :ets.info(state.pairs, :size) > state.pair_cap do
      case oldest_pair(state.pairs) do
        nil -> state
        key -> :ets.delete(state.pairs, key)
      end
    end

    state
  end

  # Find the least-recently-touched pair (minimum tick). O(n) but only runs when
  # the table is over cap, which the 100,000-pair default makes rare.
  defp oldest_pair(table) do
    :ets.foldl(
      fn {key, {_count, tick}}, {best_key, best_tick} ->
        if is_nil(best_tick) or tick < best_tick, do: {key, tick}, else: {best_key, best_tick}
      end,
      {nil, nil},
      table
    )
    |> elem(0)
  end

  defp pair_key(a, b) when a <= b, do: {a, b}
  defp pair_key(a, b), do: {b, a}

  defp bump_tick(state), do: %{state | pair_tick: state.pair_tick + 1}

  # --- forwarding -----------------------------------------------------------

  # Forward to the Dispatcher only when not learning (design step 7, Req 35.2);
  # to the Storage_Writer whenever storage is configured, in every case.
  defp forward(anomaly, candidates, state) do
    unless Map.get(anomaly, :learning?, false) do
      forward_to_dispatcher(state.dispatcher, anomaly, candidates)
    end

    forward_to_storage(state.storage_writer, anomaly, candidates)
    :ok
  end

  defp forward_to_dispatcher(nil, _anomaly, _candidates), do: :ok

  defp forward_to_dispatcher(dispatcher, anomaly, candidates) do
    if alive?(dispatcher) do
      try do
        Dispatcher.event(dispatcher, {:anomaly_started, anomaly, candidates})
      catch
        :exit, _reason -> :ok
      end
    end

    :ok
  end

  defp forward_to_storage(nil, _anomaly, _candidates), do: :ok

  defp forward_to_storage(writer, anomaly, candidates) do
    if alive?(writer) do
      try do
        Writer.enqueue_async(writer, :anomaly, %{
          anomaly: anomaly,
          candidates: candidates
        })
      catch
        :exit, _reason -> :ok
      end
    end

    :ok
  end

  defp alive?(pid) when is_pid(pid), do: Process.alive?(pid)
  defp alive?(name) when is_atom(name), do: is_pid(Process.whereis(name))
  defp alive?(_), do: false

  # Relay an anomaly resolution to the Dispatcher, preserving Worker-produced
  # ordering relative to the corresponding start.
  defp relay_resolved(anomaly, %{dispatcher: dispatcher}) do
    if alive?(dispatcher) do
      try do
        Dispatcher.event(dispatcher, {:anomaly_resolved, anomaly})
      catch
        :exit, _reason -> :ok
      end
    end

    :ok
  end

  # --- helpers --------------------------------------------------------------

  defp to_event(anomaly) do
    %{
      series_key: Map.fetch!(anomaly, :series_key),
      anomaly_id: Map.get(anomaly, :id) || Map.get(anomaly, :anomaly_id),
      start_ms: Map.fetch!(anomaly, :start_ms),
      score: Map.get(anomaly, :score, 0.0),
      event_like?: Map.get(anomaly, :event_like?, false)
    }
  end

  defp score_number(score) when is_number(score), do: score
  defp score_number(_), do: 0.0

  defp validate_pool_limit(value) when is_integer(value) and value > 0, do: {:ok, value}

  defp validate_pool_limit(value),
    do: {:error, {:invalid_setting, :candidate_pool_size_limit, value}}

  defp validate_support(value) when is_integer(value) and value >= 0, do: {:ok, value}
  defp validate_support(value), do: {:error, {:invalid_setting, :support_threshold, value}}

  defp positive_opt(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> value
      _ -> default
    end
  end
end

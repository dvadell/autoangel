defmodule Autoangel.Alert.Policy do
  @moduledoc "Pure Incident and alert policy."

  alias Autoangel.Alert.Render

  @default_cooldown_ms 30 * 60 * 1_000

  @type incident :: map()
  @type state :: %{
          incidents: %{optional(term()) => incident()},
          series_incident: map(),
          cooldowns: map(),
          next_id: pos_integer(),
          cooldown_ms: pos_integer(),
          grouping_window_ms: non_neg_integer(),
          plugins: list(),
          render: keyword(),
          mutes: MapSet.t()
        }

  @doc "Returns an empty policy state, optionally with persisted incidents."
  @spec new(keyword()) :: state()
  def new(opts \\ []) do
    incidents = Keyword.get(opts, :incidents, %{})
    incidents = if is_list(incidents), do: Map.new(incidents), else: incidents

    open_incidents =
      incidents
      |> Enum.filter(fn {_id, incident} -> Map.get(incident, :open?, false) end)
      |> Map.new()

    series_incident =
      Enum.reduce(open_incidents, %{}, fn {id, incident}, acc ->
        Enum.reduce(Map.get(incident, :series_keys, [Map.get(incident, :series_key)]), acc, fn
          nil, acc -> acc
          key, acc -> Map.put(acc, key, id)
        end)
      end)

    next_id =
      incidents
      |> Map.keys()
      |> Enum.map(fn
        {:incident, id} when is_integer(id) -> id
        _ -> 0
      end)
      |> Enum.max(fn -> 0 end)
      |> Kernel.+(1)

    cooldown_ms = Keyword.get(opts, :cooldown_ms, @default_cooldown_ms)
    now_ms = Keyword.get(opts, :now_ms, 0)

    cooldowns = Enum.reduce(incidents, %{}, &restore_cooldown(&1, &2, cooldown_ms, now_ms))

    %{
      incidents: open_incidents,
      series_incident: series_incident,
      cooldowns: cooldowns,
      next_id: next_id,
      cooldown_ms: Keyword.get(opts, :cooldown_ms, @default_cooldown_ms),
      grouping_window_ms: Keyword.get(opts, :grouping_window_ms, 0),
      plugins: Keyword.get(opts, :plugins, []),
      render: Keyword.get(opts, :render, []),
      mutes: MapSet.new(Keyword.get(opts, :mutes, []))
    }
  end

  @doc "Records a mute for `{:series, key}` or `{:incident, id}`; idempotent."
  @spec mute(state(), {:series, term()} | {:incident, term()}) :: state()
  def mute(state, {kind, _} = target) when kind in [:series, :incident] do
    state = normalize_state(state)
    state = %{state | mutes: MapSet.put(state.mutes, target)}
    mark_incident_muted(state, target)
  end

  # Muting an Incident flags it so no further notification is sent for it
  # (Req 30.1); the flag is checked wherever Incident notifications are emitted.
  # An Incident's id is itself `{:incident, n}`, the same value used as its
  # mute target.
  defp mark_incident_muted(state, {:incident, _} = id) do
    case Map.fetch(state.incidents, id) do
      {:ok, incident} ->
        %{state | incidents: Map.put(state.incidents, id, Map.put(incident, :muted?, true))}

      :error ->
        state
    end
  end

  defp mark_incident_muted(state, _target), do: state

  @doc """
  True when notifications for this Incident must be suppressed: the Incident is
  muted, or the Series it belongs to is muted.
  """
  @spec notifications_muted?(state(), map()) :: boolean()
  def notifications_muted?(state, incident) do
    Map.get(incident, :muted?, false) or muted?(state, Map.get(incident, :id)) or
      Enum.any?(Map.get(incident, :series_keys, []), &muted?(state, {:series, &1}))
  end

  @doc "True when the target is muted."
  @spec muted?(state(), {:series, term()} | {:incident, term()}) :: boolean()
  def muted?(state, target), do: MapSet.member?(Map.get(state, :mutes, MapSet.new()), target)

  @spec on_event(state(), tuple(), integer()) :: {state(), [tuple()]}
  def on_event(state, event, now_ms) when is_map(state) and is_integer(now_ms) do
    state = normalize_state(state)

    case event do
      {:anomaly_started, anomaly, candidates} ->
        start(state, anomaly, candidates, now_ms)

      {:anomaly_started, anomaly} ->
        start(state, anomaly, [], now_ms)

      {:anomaly_with_candidates, anomaly, candidates} ->
        start(state, anomaly, candidates, now_ms)

      {:anomaly_resolved, anomaly} ->
        resolve(state, anomaly, now_ms)

      {:delivered, incident_id, plugin} ->
        delivered(state, incident_id, plugin)

      {:feedback, _feedback} ->
        {state, []}

      _ ->
        {state, []}
    end
  end

  defp start(state, anomaly, _candidates, _now) when not is_map(anomaly), do: {state, []}
  defp start(state, %{learning?: true}, _candidates, _now), do: {state, []}

  defp start(state, anomaly, candidates, now_ms) do
    series_key = Map.get(anomaly, :series_key)

    if is_nil(series_key) do
      {state, []}
    else
      start_for_series(state, anomaly, candidates, series_key, now_ms)
    end
  end

  defp start_for_series(state, anomaly, candidates, series_key, now_ms) do
    cond do
      Map.has_key?(state.series_incident, series_key) ->
        {state, []}

      cooldown_active?(state, series_key, now_ms) ->
        {state, []}

      true ->
        case join_target(state, anomaly) do
          nil -> open_incident(state, anomaly, candidates, series_key, now_ms)
          id -> join_incident(state, id, anomaly, series_key)
        end
    end
  end

  defp open_incident(state, anomaly, candidates, series_key, now_ms) do
    id = {:incident, state.next_id}

    incident = %{
      id: id,
      series_key: series_key,
      series_keys: [series_key],
      anomalies: %{Map.get(anomaly, :id) => anomaly},
      opened_at: Map.get(anomaly, :start_ms, now_ms),
      closed_at: nil,
      open?: true,
      muted?: muted?(state, {:series, series_key}),
      delivered_to: [],
      resolution_sent_to: []
    }

    alert = %{build_alert(anomaly, candidates, state) | incident_id: id_number(id)}

    next = %{
      state
      | incidents: Map.put(state.incidents, id, incident),
        series_incident: Map.put(state.series_incident, series_key, id),
        next_id: state.next_id + 1
    }

    # A muted Series still opens (and dedupes into) an Incident so detection
    # and the Incident record continue, but no alert is sent (Req 30.1).
    deliveries =
      if incident.muted?, do: [], else: Enum.map(state.plugins, &{:deliver, &1, alert})

    {next, [{:open_incident, incident}, {:persist, {:open, incident}} | deliveries]}
  end

  # --- grouping (Requirements 28.1 to 28.4) -------------------------------------
  #
  # An Anomaly whose start is within the grouping window of an open Incident joins
  # the closest one, with no separate alert. Ties go to the lowest Incident number.
  defp join_target(state, anomaly) do
    start = Map.get(anomaly, :start_ms)

    if is_integer(start) and Map.get(state, :grouping_window_ms, 0) > 0 do
      state.incidents
      |> Enum.filter(fn {_id, incident} -> Map.get(incident, :open?, false) end)
      |> Enum.map(fn {id, incident} -> {distance(incident, start), id_number(id), id} end)
      |> Enum.filter(fn {distance, _n, _id} -> distance <= state.grouping_window_ms end)
      |> Enum.min(fn -> nil end)
      |> case do
        nil -> nil
        {_distance, _n, id} -> id
      end
    end
  end

  defp distance(incident, start) do
    incident
    |> Map.get(:anomalies, %{})
    |> Map.values()
    |> Enum.map(&Map.get(&1, :start_ms))
    |> Enum.filter(&is_integer/1)
    |> Enum.map(&abs(&1 - start))
    |> Enum.min(fn -> :infinity end)
  end

  defp id_number({:incident, n}) when is_integer(n), do: n
  defp id_number(_id), do: 0

  defp join_incident(state, id, anomaly, series_key) do
    incident = Map.fetch!(state.incidents, id)

    incident = %{
      incident
      | series_keys: Enum.uniq(incident.series_keys ++ [series_key]),
        anomalies: Map.put(incident.anomalies, Map.get(anomaly, :id), anomaly)
    }

    next = %{
      state
      | incidents: Map.put(state.incidents, id, incident),
        series_incident: Map.put(state.series_incident, series_key, id)
    }

    {next, [{:join_incident, id, series_key}, {:persist, {:open, incident}}]}
  end

  defp resolve(state, anomaly, now_ms) when is_map(anomaly) do
    series_key = Map.get(anomaly, :series_key)

    case Map.get(state.series_incident, series_key) do
      nil ->
        {state, []}

      id ->
        incident = Map.fetch!(state.incidents, id)
        anomalies = Map.put(incident.anomalies, Map.get(anomaly, :id), anomaly)
        incident = %{incident | anomalies: anomalies}

        if Enum.all?(Map.values(anomalies), &(not is_nil(Map.get(&1, :end_ms)))),
          do: close_incident(state, id, incident, now_ms),
          else: {%{state | incidents: Map.put(state.incidents, id, incident)}, []}
    end
  end

  defp resolve(state, _anomaly, _now), do: {state, []}

  # The Incident stays open until its last member resolves. On close, one
  # resolution goes to each plugin whose original delivery succeeded
  # (Requirements 29.1 to 29.3).
  defp close_incident(state, id, incident, now_ms) do
    delivered = Map.get(incident, :delivered_to, [])
    sent = if notifications_muted?(state, incident), do: [], else: delivered
    closed = %{incident | open?: false, closed_at: now_ms, resolution_sent_to: sent}
    cooldown = now_ms + cooldown_ms(state)

    next = %{
      state
      | incidents: Map.put(state.incidents, id, closed),
        series_incident: remove_members(state.series_incident, incident.series_keys),
        cooldowns: Enum.reduce(incident.series_keys, state.cooldowns, &Map.put(&2, &1, cooldown))
    }

    resolution = Render.resolution(closed, Map.get(state, :render, []))
    deliveries = Enum.map(sent, &{:deliver, &1, resolution})
    {next, [{:close_incident, closed}, {:persist, {:close, closed}} | deliveries]}
  end

  # A plugin reports that the original alert of an Incident arrived. When the
  # Incident has already closed, its resolution goes out now (Requirement 29.1).
  defp delivered(state, id, plugin) do
    id = if is_integer(id), do: {:incident, id}, else: id

    case Map.fetch(state.incidents, id) do
      {:ok, incident} -> mark_delivered(state, id, incident, plugin)
      :error -> {state, []}
    end
  end

  defp mark_delivered(state, id, incident, plugin) do
    delivered = Enum.uniq(Map.get(incident, :delivered_to, []) ++ [plugin])
    incident = Map.put(incident, :delivered_to, delivered)
    state = %{state | incidents: Map.put(state.incidents, id, incident)}

    if not Map.get(incident, :open?, true) and
         plugin not in Map.get(incident, :resolution_sent_to, []) and
         not notifications_muted?(state, incident) do
      sent = Map.get(incident, :resolution_sent_to, []) ++ [plugin]
      incident = Map.put(incident, :resolution_sent_to, sent)
      resolution = Render.resolution(incident, Map.get(state, :render, []))

      {%{state | incidents: Map.put(state.incidents, id, incident)},
       [{:deliver, plugin, resolution}]}
    else
      {state, []}
    end
  end

  defp build_alert(anomaly, candidates, state),
    do: Render.build(anomaly, candidates, Map.get(state, :render, []))

  defp restore_cooldown({_id, incident}, acc, cooldown_ms, now_ms) do
    closed = Map.get(incident, :closed_at)

    case {Map.get(incident, :open?, false), closed} do
      {false, closed} when is_integer(closed) and closed + cooldown_ms > now_ms ->
        Map.put(acc, Map.get(incident, :series_key), closed + cooldown_ms)

      _ ->
        acc
    end
  end

  defp cooldown_active?(state, key, now), do: Map.get(state.cooldowns, key, 0) > now
  defp cooldown_ms(state), do: Map.get(state, :cooldown_ms, @default_cooldown_ms)
  defp remove_members(map, keys), do: Enum.reduce(keys, map, &Map.delete(&2, &1))

  defp normalize_state(state) do
    Map.merge(new(cooldown_ms: @default_cooldown_ms), state)
  end
end

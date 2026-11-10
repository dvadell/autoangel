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
      plugins: Keyword.get(opts, :plugins, []),
      render: Keyword.get(opts, :render, []),
      mutes: MapSet.new(Keyword.get(opts, :mutes, []))
    }
  end

  @doc "Records a mute for `{:series, key}` or `{:incident, id}`; idempotent."
  @spec mute(state(), {:series, term()} | {:incident, term()}) :: state()
  def mute(state, {kind, _} = target) when kind in [:series, :incident] do
    state = normalize_state(state)
    %{state | mutes: MapSet.put(state.mutes, target)}
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
        id = {:incident, state.next_id}

        incident = %{
          id: id,
          series_key: series_key,
          series_keys: [series_key],
          anomalies: %{Map.get(anomaly, :id) => anomaly},
          opened_at: Map.get(anomaly, :start_ms, now_ms),
          closed_at: nil,
          open?: true
        }

        alert = build_alert(anomaly, candidates, state)

        next = %{
          state
          | incidents: Map.put(state.incidents, id, incident),
            series_incident: Map.put(state.series_incident, series_key, id),
            next_id: state.next_id + 1
        }

        deliveries = Enum.map(state.plugins, &{:deliver, &1, alert})
        {next, [{:open_incident, incident}, {:persist, {:open, incident}} | deliveries]}
    end
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

        if Enum.all?(Map.values(anomalies), &(not is_nil(Map.get(&1, :end_ms)))) do
          closed = %{incident | open?: false, closed_at: now_ms}
          cooldown = now_ms + cooldown_ms(state)

          next = %{
            state
            | incidents: Map.put(state.incidents, id, closed),
              series_incident: remove_members(state.series_incident, incident.series_keys),
              cooldowns: Map.put(state.cooldowns, series_key, cooldown)
          }

          {next, [{:close_incident, closed}, {:persist, {:close, closed}}]}
        else
          {%{state | incidents: Map.put(state.incidents, id, incident)}, []}
        end
    end
  end

  defp resolve(state, _anomaly, _now), do: {state, []}

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

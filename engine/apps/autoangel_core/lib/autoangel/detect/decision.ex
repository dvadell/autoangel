defmodule Autoangel.Detect.Decision do
  @moduledoc "Pure detector state transitions for scored samples and missing-data sweeps."

  defmodule Anomaly do
    @moduledoc false
    @type t :: %__MODULE__{}

    defstruct id: nil,
              series_key: nil,
              kind: :value,
              classification: nil,
              start_ms: nil,
              end_ms: nil,
              score: 0.0,
              observed: nil,
              lower: nil,
              upper: nil,
              baseline_type: nil,
              learning?: false
  end

  defmodule Meta do
    @moduledoc false
    @type t :: %__MODULE__{}

    defstruct key: nil,
              id: nil,
              kind: :gauge,
              status: :learning,
              first_ts: nil,
              latest_ts: nil,
              last_recv_ms: nil,
              last_source: nil,
              typical_interval_ms: nil,
              event_like?: false,
              run: nil,
              calm_run: nil,
              open_value_anomaly: nil,
              open_missing: nil,
              open_drift: nil,
              seasonal: nil,
              direction_override: nil,
              sensitivity_override: nil,
              warming?: false
  end

  @type config :: map() | keyword()
  @type event ::
          {:anomaly_started, Anomaly.t()} | {:anomaly_resolved, Anomaly.t()} | {:status, atom()}

  @spec step(Meta.t() | map(), map() | atom(), integer(), config()) :: {map(), [event()]}
  def step(meta, result, now_recv, cfg \\ %{}) when is_map(meta) and is_integer(now_recv) do
    meta = normalize_meta(meta)
    old_status = meta.status

    output =
      if missing_sweep?(result) do
        missing_sweep(meta, now_recv, cfg, result)
      else
        sample_step(meta, result, now_recv, cfg)
      end

    add_status_event(old_status, output)
  end

  defp sample_step(meta, result, now_recv, cfg) when is_map(result) do
    ts = Map.get(result, :ts, now_recv)
    recv_ms = Map.get(result, :recv_ms, now_recv)
    kind = Map.get(result, :kind, meta.kind)
    {meta, _kind_changed?} = apply_kind(meta, kind)
    {meta, events} = resolve_missing(meta, recv_ms)
    meta = update_sample_metadata(meta, result, ts, recv_ms)
    meta = maybe_start_learning(meta, ts)
    meta = maybe_finish_learning(meta, ts, cfg)

    case Map.get(result, :score) do
      nil -> {set_status(meta), events ++ status_event(meta, meta.status)}
      score -> scored_sample(meta, result, ts, score, cfg, events)
    end
  end

  defp sample_step(meta, _result, _now_recv, _cfg), do: {meta, []}

  defp scored_sample(meta, result, ts, score, cfg, events) do
    anomalous? = anomalous?(meta, result, score, cfg)
    open? = not is_nil(meta.open_value_anomaly)

    {meta, events} =
      cond do
        anomalous? and not open? ->
          run = extend_run(meta.run, ts)
          meta = %{meta | run: run, calm_run: nil}

          if persists?(run, cfg) do
            anomaly = make_anomaly(meta, result, run.first_ts, :value, nil, learning?(meta))

            {%{meta | open_value_anomaly: anomaly, run: nil},
             events ++ [{:anomaly_started, anomaly}]}
          else
            {meta, events}
          end

        anomalous? ->
          {%{meta | run: nil, calm_run: nil}, events}

        true ->
          resolve_value_if_needed(meta, result, ts, cfg, events)
      end

    meta = set_status(meta)
    {meta, events ++ status_event(meta, meta.status)}
  end

  defp resolve_value_if_needed(%{open_value_anomaly: nil} = meta, _result, _ts, _cfg, events),
    do: {%{meta | run: nil}, events}

  defp resolve_value_if_needed(meta, _result, ts, cfg, events) do
    calm = extend_run(meta.calm_run, ts)

    if persists?(calm, cfg) do
      anomaly = %{meta.open_value_anomaly | end_ms: calm.first_ts}

      {%{meta | open_value_anomaly: nil, calm_run: nil, run: nil},
       events ++ [{:anomaly_resolved, anomaly}]}
    else
      {%{meta | calm_run: calm, run: nil}, events}
    end
  end

  defp missing_sweep(meta, now_recv, cfg, result) do
    multiple = number(config_get(cfg, [:missing_data, :multiple], 3.0), 3.0)
    interval = meta.typical_interval_ms
    threshold = if is_number(interval), do: multiple * interval, else: :infinity
    qualifies? = is_number(meta.last_recv_ms) and now_recv - meta.last_recv_ms > threshold

    if qualifies? and is_nil(meta.open_missing) and meta.status != :silent do
      classification = missing_classification(meta, cfg, result)
      anomaly = make_anomaly(meta, %{}, now_recv, :missing, classification, learning?(meta))

      meta = %{
        meta
        | open_missing: anomaly,
          status: if(learning?(meta), do: :learning, else: :silent)
      }

      {meta, [{:anomaly_started, anomaly} | status_event(meta, meta.status)]}
    else
      {meta, []}
    end
  end

  defp resolve_missing(meta, recv_ms) do
    case meta.open_missing do
      nil ->
        {meta, []}

      anomaly ->
        anomaly = %{anomaly | end_ms: recv_ms}
        meta = %{meta | open_missing: nil}
        {meta, [{:anomaly_resolved, anomaly}]}
    end
  end

  defp anomalous?(meta, result, score, cfg) do
    threshold = effective_threshold(meta, cfg)
    direction = meta.direction_override || :both
    observed = Map.get(result, :observed)
    lower = Map.get(result, :lower)
    upper = Map.get(result, :upper)

    direction_ok? =
      case {direction, observed, lower, upper} do
        {:high, value, _, high} when is_number(value) and is_number(high) -> value > high
        {:low, value, low, _} when is_number(value) and is_number(low) -> value < low
        _ -> true
      end

    is_number(score) and score >= threshold and direction_ok?
  end

  defp effective_threshold(meta, cfg) do
    base =
      number(config_get(cfg, [:score_threshold], config_get(cfg, [:anomaly_threshold], 75)), 75)

    sensitivity = config_get(cfg, [:sensitivity], 1.0)
    sensitivity = number(sensitivity, 1.0)
    override = Map.get(meta, :sensitivity_override)
    sensitivity = number(override, sensitivity)
    clamp(100.0 - (100.0 - base) * sensitivity, 1.0, 99.9)
  end

  defp persists?(run, cfg) do
    n = config_get(cfg, [:persistence, :n], config_get(cfg, [:persistence_n], 3))
    minutes = config_get(cfg, [:persistence, :m], config_get(cfg, [:persistence_minutes], 5))
    run.count >= number(n, 3) or run.last_ts - run.first_ts >= number(minutes, 5) * 60_000
  end

  defp extend_run(nil, ts), do: %{count: 1, first_ts: ts, last_ts: ts}

  defp extend_run(%{count: 0}, ts), do: %{count: 1, first_ts: ts, last_ts: ts}
  defp extend_run(%{first_ts: nil}, ts), do: %{count: 1, first_ts: ts, last_ts: ts}
  defp extend_run(run, ts), do: %{run | count: run.count + 1, last_ts: ts}

  defp maybe_start_learning(%{first_ts: nil} = meta, ts), do: %{meta | first_ts: ts}
  defp maybe_start_learning(meta, _ts), do: meta

  defp maybe_finish_learning(
         %{status: :learning, first_ts: first, seasonal: {_, period}} = meta,
         _ts,
         cfg
       )
       when is_integer(first) and is_integer(period) do
    cycles = config_get(cfg, [:learning, :cycles], 2)
    if meta.latest_ts - first >= cycles * period, do: %{meta | status: :healthy}, else: meta
  end

  defp maybe_finish_learning(%{status: :learning, first_ts: first} = meta, _ts, cfg)
       when is_integer(first) do
    duration = config_get(cfg, [:learning, :duration], 86_400)
    if meta.latest_ts - first >= duration * 1_000, do: %{meta | status: :healthy}, else: meta
  end

  defp maybe_finish_learning(meta, _ts, _cfg), do: meta

  defp update_sample_metadata(meta, result, ts, recv_ms) do
    %{
      meta
      | latest_ts: max_ts(meta.latest_ts, ts),
        last_recv_ms: recv_ms,
        last_source: Map.get(result, :source, meta.last_source),
        typical_interval_ms: Map.get(result, :typical_interval_ms, meta.typical_interval_ms)
    }
  end

  defp apply_kind(meta, kind) when kind in [:gauge, :counter] and meta.kind != kind do
    {%{meta | kind: kind, run: nil, calm_run: nil}, true}
  end

  defp apply_kind(meta, _), do: {meta, false}

  defp set_status(%{status: :learning} = meta), do: meta

  defp set_status(%{open_missing: anomaly} = meta) when not is_nil(anomaly),
    do: %{meta | status: :silent}

  defp set_status(%{open_value_anomaly: anomaly} = meta) when not is_nil(anomaly),
    do: %{meta | status: :anomalous}

  defp set_status(meta), do: %{meta | status: :healthy}

  defp status_event(_meta, _status), do: []

  defp add_status_event(old_status, {meta, events}) when old_status == meta.status,
    do: {meta, events}

  defp add_status_event(_old_status, {meta, events}), do: {meta, events ++ [{:status, meta.status}]}

  defp make_anomaly(meta, result, ts, kind, classification, learning?) do
    %Anomaly{
      id: anomaly_id(),
      series_key: meta.key,
      kind: kind,
      classification: classification,
      start_ms: ts,
      score: Map.get(result, :score, 0.0),
      observed: Map.get(result, :observed),
      lower: Map.get(result, :lower),
      upper: Map.get(result, :upper),
      baseline_type: Map.get(result, :baseline_type),
      learning?: learning?
    }
  end

  defp missing_classification(%{last_source: {:scrape, target}}, cfg, result) do
    failing? = target_failing?(target, cfg, result)
    if failing?, do: :target_down, else: :metric_silent
  end

  defp missing_classification(_meta, _cfg, _result), do: :metric_silent

  defp target_failing?(target, cfg, result) when is_map(result) do
    case Map.fetch(result, :target_failing?) do
      {:ok, value} -> value
      :error -> target_failing?(target, cfg)
    end
  end

  defp target_failing?(target, cfg, _result), do: target_failing?(target, cfg)

  defp target_failing?(target, cfg) do
    case config_get(cfg, [:failing_targets], %{}) do
      failing_targets when is_map(failing_targets) ->
        Map.get(failing_targets, target, false)

      failing_targets when is_list(failing_targets) ->
        case List.keyfind(failing_targets, target, 0) do
          {_key, value} -> value
          nil -> false
        end

      _ ->
        false
    end
  end

  defp learning?(meta), do: meta.status == :learning
  defp missing_sweep?(:missing_sweep), do: true
  defp missing_sweep?(%{sweep?: true}), do: true
  defp missing_sweep?(_), do: false

  defp normalize_meta(%Meta{} = meta), do: meta

  defp normalize_meta(meta) do
    fields = Map.keys(Meta.__struct__()) -- [:__struct__]
    struct(Meta, Map.take(meta, fields))
  end

  defp max_ts(nil, ts), do: ts
  defp max_ts(old, ts), do: max(old, ts)
  defp number(value, _fallback) when is_number(value), do: value
  defp number(_, fallback), do: fallback
  defp clamp(value, low, high), do: min(max(value, low), high)

  defp config_get(cfg, [key | rest], default) when is_list(cfg) do
    case Keyword.fetch(cfg, key) do
      {:ok, value} when rest == [] -> value
      {:ok, value} -> config_get(value, rest, default)
      :error -> default
    end
  end

  defp config_get(cfg, path, default) when is_map(cfg), do: get_in(cfg, path) || default
  defp config_get(_, _, default), do: default
  defp anomaly_id, do: :crypto.strong_rand_bytes(16) |> Base.encode16(case: :lower)
end

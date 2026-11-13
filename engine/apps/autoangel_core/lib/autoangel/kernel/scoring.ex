defmodule Autoangel.Kernel.Scoring do
  @moduledoc "Pure scoring-kernel contract and Elixir reference implementation."

  @callback score(
              binary(),
              [{integer(), number() | :nan | :inf | :neg_inf, :gauge | :counter}],
              Autoangel.Kernel.Params.t()
            ) ::
              {:ok, binary(), [map()]} | {:error, term()}

  defmodule Elixir do
    @moduledoc "Reference implementation of the robust rolling scoring kernel."
    @behaviour Autoangel.Kernel.Scoring

    alias Autoangel.Kernel.{Params, Sketch, State}

    @ln2 :math.log(2.0)
    @max_float 1.797_693_134_862_315_7e308
    @spacing_capacity 16
    @max_i64 9_223_372_036_854_775_807

    @spec score(
            binary(),
            [{integer(), number() | :nan | :inf | :neg_inf, :gauge | :counter}],
            Params.t()
          ) ::
            {:ok, binary(), [map()]} | {:error, term()}
    @impl Autoangel.Kernel.Scoring
    def score(state_bin, samples, params \\ %Params{})

    def score(state_bin, samples, params) when is_binary(state_bin) and is_list(samples) do
      with {:ok, state} <- State.decode(state_bin),
           {:ok, params} <- params(params),
           :ok <- validate_samples(samples) do
        {results, state} =
          Enum.map_reduce(samples, state, fn sample, current_state ->
            {new_state, result} = score_one(sample, current_state, params)
            {result, new_state}
          end)

        {:ok, State.encode(state), results}
      end
    end

    def score(_, _, _), do: {:error, :invalid_input}

    defp score_one({ts, value, kind}, state, params) do
      if finite?(value) do
        value = value * 1.0
        state = reset_for_kind(state, kind)
        previous = state.prev
        {observed, scored?} = observed_value({ts, value}, previous, kind)
        spacing = positive_spacing(previous, ts)
        spacing_values = add_spacing(state.spacing, state.spacing_head, spacing)
        typical = median_spacing(spacing_values)

        base_result = %{
          ts: ts,
          score: nil,
          observed: nil,
          lower: nil,
          upper: nil,
          baseline_type: nil,
          typical_interval_ms: typical,
          spacing_cv: spacing_cv(spacing_values),
          spacing_ok?: spacing != nil
        }

        if kind == :counter and not scored? do
          {%{
             state
             | kind: kind,
               head: 0,
               prev: latest_prev(previous, {ts, value}),
               spacing: spacing_values,
               spacing_head: next_head(state.spacing_head, spacing)
           }, base_result}
        else
          result =
            score_value(observed, base_result, state.window, seasonal_cell(state, ts), params)

          window = push_window(state.window, observed, params.window_size)

          {result, window, level, longterm} =
            track_changes(result, window, state, observed, ts, params)

          {%{
             state
             | kind: kind,
               window: window,
               level: level,
               longterm: longterm,
               seasonal: update_seasonal(state.seasonal, ts, observed),
               head: 0,
               capacity: params.window_size,
               prev: latest_prev(previous, {ts, value}),
               spacing: spacing_values,
               spacing_head: next_head(state.spacing_head, spacing)
           }, result}
        end
      else
        {state,
         %{
           ts: ts,
           score: nil,
           observed: nil,
           lower: nil,
           upper: nil,
           baseline_type: nil,
           typical_interval_ms: median_spacing(state.spacing),
           spacing_ok?: false
         }}
      end
    end

    defp reset_for_kind(%State{window: [_ | _], kind: old_kind} = state, kind)
         when old_kind != kind,
         do: %{state | window: [], prev: nil}

    defp reset_for_kind(state, _kind), do: state

    defp observed_value({_ts, _value}, nil, :counter), do: {nil, false}

    defp observed_value({ts, _value}, {previous_ts, _}, :counter) when ts <= previous_ts,
      do: {nil, false}

    defp observed_value({ts, value}, {previous_ts, previous_value}, :counter) do
      seconds = (ts - previous_ts) / 1000.0

      rate =
        if value < previous_value,
          do: safe_div(value, seconds),
          else: safe_div(safe_difference(value, previous_value), seconds)

      {max(rate, 0.0), true}
    end

    defp observed_value({_ts, value}, _previous, :gauge), do: {value, true}

    defp score_value(nil, result, _window, _cell, _params), do: result

    defp score_value(value, result, [], nil, _params) do
      %{result | score: 0.0, observed: value, lower: value, upper: value, baseline_type: :robust}
    end

    defp score_value(value, result, window, cell, params) do
      window_med = if window == [], do: 0.0, else: median(window)

      window_mad =
        if window == [], do: 0.0, else: median(Enum.map(window, &safe_abs_diff(&1, window_med)))

      # A per-phase Baseline replaces the rolling median; its spread is never
      # narrower than the recent raw spread (cells hold hourly means).
      {med, mad, baseline_type} =
        case cell do
          nil -> {window_med, window_mad, :robust}
          {cell, type} -> {cell.med, max(window_mad, cell.mad), type}
        end

      scale =
        max(
          safe_product(params.mad_scale, mad),
          max(params.scale_floor, safe_product(params.scale_fraction, abs(med)))
        )

      z = safe_div(safe_abs_diff(value, med), scale)
      score = clamp(100.0 * -expm1(-z * @ln2 / 3.0), 0.0, 100.0)
      score = if score == 0.0, do: 0.0, else: score
      z_t = threshold_z(params.score_threshold)
      distance = safe_product(z_t, scale)
      lower = finite_clamp(safe_difference(med, distance))
      upper = finite_clamp(safe_sum(med, distance))

      %{
        result
        | score: score,
          observed: value,
          lower: min(lower, upper),
          upper: max(lower, upper),
          baseline_type: baseline_type
      }
      |> Map.put(:z, safe_div(safe_difference(value, med), scale))
      |> Map.put(:scale, scale)
    end

    # --- level shift (CUSUM) and drift --------------------------------------------

    @reset_level %{
      cusum_pos: 0.0,
      cusum_neg: 0.0,
      pending: 0,
      pending_level: 0.0,
      pending_first_ts: 0
    }

    defp track_changes(%{score: nil} = result, window, state, _observed, _ts, _params),
      do: {result, window, state.level, state.longterm}

    defp track_changes(result, window, state, observed, ts, params) do
      {level, candidate} =
        if mature?(window, params),
          do: cusum(state.level, Map.get(result, :z, 0.0), observed, ts, params),
          else: {@reset_level, nil}

      if candidate && confirmed?(candidate, result, window, params) do
        # Rebuild the Baseline from the post-change Samples only and restart the
        # long-term reference from them (Requirements 13.3, 13.4).
        window = Enum.take(window, -candidate.count)
        sketch = Sketch.from_values(window, params.longterm_window)

        {result |> Map.put(:level_shift, Map.delete(candidate, :next)) |> Map.put(:drift, nil),
         window, @reset_level, to_longterm(sketch)}
      else
        level = if candidate, do: candidate.next, else: level
        sketch = Sketch.add(longterm_centroids(state.longterm), observed, params.longterm_window)
        {Map.put(result, :drift, drift(result, sketch, params)), window, level, to_longterm(sketch)}
      end
    end

    # A Baseline that is still forming cannot tell a level shift from noise: it
    # needs at least 32 Samples and twice the minimum shift length.
    defp mature?(window, params),
      do: length(window) > max(32, 2 * params.level_shift_min_samples)

    # A burst of outliers leaves the CUSUM high for a while after the values are
    # back to normal. A real level shift also shows in the latest Samples: their
    # median sits at least one robust sigma away from the old Baseline, on the
    # side of the shift.
    defp confirmed?(candidate, result, window, params) do
      recent = Enum.take(window, -params.level_shift_min_samples)
      center = (result.lower + result.upper) / 2
      offset = (median(recent) - center) / max(result.scale, params.scale_floor)

      if candidate.direction == :up, do: offset >= 1.0, else: offset <= -1.0
    end

    # Returns `{level, nil}` or `{level, %{start_ts, level, count, direction}}`.
    defp cusum(level, z, observed, ts, params) do
      z = clamp(z * 1.0, -params.cusum_clip, params.cusum_clip)
      pos = max(0.0, level.cusum_pos + z - params.cusum_k)
      neg = max(0.0, level.cusum_neg - z - params.cusum_k)

      if pos == 0.0 and neg == 0.0,
        do: {@reset_level, nil},
        else: advance(level, {pos, neg}, observed, ts, params)
    end

    defp advance(level, {pos, neg}, observed, ts, params) do
      pending = min(level.pending + 1, 65_535)
      first = if level.pending == 0, do: ts, else: level.pending_first_ts
      mean = (level.pending_level * level.pending + observed) / pending

      next = %{
        cusum_pos: pos,
        cusum_neg: neg,
        pending: pending,
        pending_level: mean,
        pending_first_ts: first
      }

      candidate = %{
        start_ts: first,
        level: mean,
        count: pending,
        direction: if(pos >= neg, do: :up, else: :down),
        next: next
      }

      if pending >= params.level_shift_min_samples and max(pos, neg) >= params.cusum_h,
        do: {next, candidate},
        else: {next, nil}
    end

    defp longterm_centroids(%{blocks: [%{centroids: centroids} | _]}), do: centroids
    defp longterm_centroids(_none), do: []

    defp to_longterm([]), do: nil
    defp to_longterm(centroids), do: %{blocks: [%{n: length(centroids), centroids: centroids}]}

    # Divergence of the rolling median from the long-term median, in long-term
    # scaled-MAD units. `nil` until the sketch holds twice the rolling window, and
    # for per-phase seasonal Baselines (their median moves by design).
    defp drift(%{baseline_type: :robust, lower: lower, upper: upper}, sketch, params) do
      with true <- Sketch.total(sketch) >= 2 * params.window_size,
           {median, mad} <- Sketch.median_mad(sketch) do
        scale =
          max(params.mad_scale * mad, max(params.scale_floor, params.scale_fraction * abs(median)))

        abs((lower + upper) / 2 - median) / scale
      else
        _ -> nil
      end
    end

    defp drift(_result, _sketch, _params), do: nil

    # --- per-phase seasonal Baseline -------------------------------------------

    @min_cell_samples 2

    defp seasonal_cell(%State{seasonal: nil}, _ts), do: nil

    defp seasonal_cell(%State{seasonal: %{period: period, cells: cells}}, ts) do
      cell = Enum.at(cells, phase(ts, period))

      if cell && cell.n >= @min_cell_samples,
        do: {cell, if(period == 24, do: :seasonal_daily, else: :seasonal_weekly)},
        else: nil
    end

    defp update_seasonal(nil, _ts, _value), do: nil
    defp update_seasonal(seasonal, _ts, nil), do: seasonal

    defp update_seasonal(%{period: period, cells: cells} = seasonal, ts, value) do
      index = phase(ts, period)

      cells =
        List.update_at(cells, index, fn cell ->
          alpha = 1.0 / min(cell.n + 1, 24)

          %{
            med: cell.med + alpha * (value - cell.med),
            mad: cell.mad + alpha * (abs(value - cell.med) - cell.mad),
            n: min(cell.n + 1, 4_294_967_295)
          }
        end)

      %{seasonal | cells: cells}
    end

    @doc false
    @spec phase(integer(), 24 | 168) :: non_neg_integer()
    def phase(ts_ms, period), do: Integer.mod(Integer.floor_div(ts_ms, 3_600_000), period)

    defp threshold_z(threshold) when threshold <= 0, do: 0.0
    defp threshold_z(threshold) when threshold >= 100, do: @max_float
    defp threshold_z(threshold), do: -3.0 * :math.log(1.0 - threshold / 100.0) / @ln2

    defp push_window(window, value, capacity) do
      values = window ++ [value]
      if length(values) > capacity, do: Enum.drop(values, length(values) - capacity), else: values
    end

    defp add_spacing(spacing, _head, nil), do: spacing

    defp add_spacing(spacing, _head, value) do
      values = spacing ++ [value]
      if length(values) > @spacing_capacity, do: Enum.drop(values, 1), else: values
    end

    defp next_head(_head, nil), do: 0
    defp next_head(_head, _), do: 0
    defp positive_spacing(nil, _), do: nil

    defp positive_spacing({previous_ts, _}, ts)
         when ts > previous_ts and ts - previous_ts <= @max_i64,
         do: ts - previous_ts

    defp positive_spacing(_, _), do: nil

    defp latest_prev(nil, {new_ts, new_value}), do: {new_ts, new_value}

    defp latest_prev({old_ts, _old_value}, {new_ts, new_value}) when new_ts > old_ts,
      do: {new_ts, new_value}

    defp latest_prev(old, _), do: old

    defp median([]), do: nil

    defp median(values) do
      sorted = Enum.sort(values)
      n = length(sorted)

      if rem(n, 2) == 1,
        do: Enum.at(sorted, div(n, 2)),
        else: Enum.at(sorted, div(n, 2) - 1) / 2 + Enum.at(sorted, div(n, 2)) / 2
    end

    # Coefficient of variation of the recent spacings; `nil` with fewer than four.
    defp spacing_cv(values) when length(values) < 4, do: nil

    defp spacing_cv(values) do
      mean = Enum.sum(values) / length(values)

      if mean <= 0 do
        nil
      else
        variance = Enum.sum(for v <- values, do: (v - mean) * (v - mean)) / length(values)
        :math.sqrt(variance) / mean
      end
    end

    defp median_spacing([]), do: nil

    defp median_spacing(values) do
      sorted = Enum.sort(values)
      n = length(sorted)

      if rem(n, 2) == 1 do
        Enum.at(sorted, div(n, 2))
      else
        left = Enum.at(sorted, div(n, 2) - 1)
        right = Enum.at(sorted, div(n, 2))
        left + div(right - left, 2)
      end
    end

    defp finite?(:nan), do: false
    defp finite?(:inf), do: false
    defp finite?(:neg_inf), do: false

    defp finite?(value) when is_integer(value), do: abs(value) <= trunc(@max_float)

    defp finite?(value) when is_float(value) do
      :erlang.float_to_binary(value) not in ["nan", "NaN", "inf", "Inf", "-inf", "-Inf"]
    end

    defp finite?(_), do: false

    defp expm1(value) do
      exponential = :math.exp(value)

      cond do
        exponential == 1.0 -> value
        exponential == 0.0 or exponential < 0.5 -> exponential - 1.0
        true -> (exponential - 1.0) * value / :math.log(exponential)
      end
    end

    defp safe_abs_diff(left, right), do: abs(safe_difference(left, right))

    defp safe_difference(left, right) when left >= 0 and right < 0,
      do: if(left > @max_float - abs(right), do: @max_float, else: left - right)

    defp safe_difference(left, right) when left < 0 and right >= 0,
      do: if(abs(left) > @max_float - right, do: -@max_float, else: left - right)

    defp safe_difference(left, right), do: left - right
    defp safe_sum(left, right), do: safe_difference(left, -right)

    defp safe_div(_left, right) when right == 0, do: @max_float

    defp safe_div(left, right) do
      absolute_right = abs(right)
      absolute_left = abs(left)

      overflow? = absolute_right < 1 and absolute_left >= @max_float * absolute_right

      if overflow?, do: if(left < 0, do: -@max_float, else: @max_float), else: left / right
    end

    defp safe_product(left, right) when left >= 0 and right >= 0 do
      cond do
        right <= 1.0 -> left * right
        left >= @max_float / right -> @max_float
        true -> left * right
      end
    end

    defp safe_product(left, right), do: safe_product(abs(left), abs(right))
    defp finite_clamp(value) when value > @max_float, do: @max_float
    defp finite_clamp(value) when value < -@max_float, do: -@max_float
    defp finite_clamp(value), do: value
    defp clamp(value, low, _high) when value < low, do: low
    defp clamp(value, _low, high) when value > high, do: high
    defp clamp(value, _low, _high), do: value

    defp params(%Params{} = params), do: validate_params(params)

    defp params(params) when is_map(params) and not is_struct(params),
      do: params |> then(&struct(Params, &1)) |> validate_params()

    defp params(_), do: {:error, :invalid_params}

    defp validate_params(%Params{window_size: size} = params)
         when is_integer(size) and size > 0 and size <= 65_535 do
      if valid_scales?(params) and valid_threshold?(params) and valid_change_params?(params),
        do: {:ok, params},
        else: {:error, :invalid_params}
    end

    defp validate_params(_), do: {:error, :invalid_params}

    defp valid_scales?(params) do
      Enum.all?([params.mad_scale, params.scale_floor, params.scale_fraction], fn value ->
        is_number(value) and finite?(value) and value > 0
      end)
    end

    defp valid_threshold?(params) do
      finite?(params.score_threshold) and params.score_threshold >= 0 and
        params.score_threshold <= 100
    end

    defp valid_change_params?(params) do
      positive_integer?(params.level_shift_min_samples) and
        positive_integer?(params.longterm_window) and
        Enum.all?([params.cusum_k, params.cusum_h, params.cusum_clip], &positive_number?/1)
    end

    defp positive_integer?(value), do: is_integer(value) and value > 0
    defp positive_number?(value), do: is_number(value) and value > 0

    defp validate_samples([]), do: :ok

    defp validate_samples([sample | rest]) do
      if valid_sample?(sample), do: validate_samples(rest), else: {:error, :invalid_samples}
    end

    defp validate_samples(_), do: {:error, :invalid_samples}

    defp valid_sample?({ts, value, kind}),
      do:
        is_integer(ts) and ts >= -9_223_372_036_854_775_808 and
          ts <= 9_223_372_036_854_775_807 and
          (finite?(value) or value in [:nan, :inf, :neg_inf]) and kind in [:gauge, :counter]

    defp valid_sample?(_), do: false
  end
end

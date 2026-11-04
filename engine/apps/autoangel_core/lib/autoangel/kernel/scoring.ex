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

    alias Autoangel.Kernel.{Params, State}

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
          result = score_value(observed, base_result, state.window, params)
          window = push_window(state.window, observed, params.window_size)

          {%{
             state
             | kind: kind,
               window: window,
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

    defp score_value(nil, result, _window, _params), do: result

    defp score_value(value, result, [], _params) do
      %{result | score: 0.0, observed: value, lower: value, upper: value, baseline_type: :robust}
    end

    defp score_value(value, result, window, params) do
      med = median(window)
      mad = median(Enum.map(window, &safe_abs_diff(&1, med)))

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
          baseline_type: :robust
      }
    end

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
      valid_numbers =
        Enum.all?([params.mad_scale, params.scale_floor, params.scale_fraction], fn value ->
          is_number(value) and finite?(value) and value > 0
        end)

      valid_threshold =
        finite?(params.score_threshold) and params.score_threshold >= 0 and
          params.score_threshold <= 100

      if valid_numbers and valid_threshold, do: {:ok, params}, else: {:error, :invalid_params}
    end

    defp validate_params(_), do: {:error, :invalid_params}

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

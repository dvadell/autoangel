defmodule Autoangel.Kernel.ScoringTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Autoangel.Kernel.{Params, State}
  alias Autoangel.Kernel.Scoring.Elixir, as: ScoringElixir

  defp initial_state, do: State.encode(State.new())

  test "scores against the previous window and returns an explanation" do
    assert {:ok, state, [first, second]} =
             ScoringElixir.score(initial_state(), [{1_000, 10.0, :gauge}, {2_000, 10.0, :gauge}])

    assert first.score == 0.0
    assert first.observed == 10.0
    assert {first.lower, first.upper} == {10.0, 10.0}
    assert first.baseline_type == :robust
    assert second.score == 0.0
    assert second.typical_interval_ms == 1_000
    assert {:ok, decoded} = State.decode(state)
    assert decoded.window == [10.0, 10.0]
  end

  test "maps an outlier to a bounded score and keeps the expected range finite" do
    samples = Enum.map(1..4, &{&1 * 1_000, 10.0, :gauge}) ++ [{5_000, 100.0, :gauge}]
    assert {:ok, _state, results} = ScoringElixir.score(initial_state(), samples)
    result = List.last(results)
    assert result.score > 0.0 and result.score <= 100.0
    assert is_float(result.lower) and is_float(result.upper)
    assert result.lower <= result.upper
  end

  test "a tiny deviation with zero MAD gets a positive score" do
    samples = [{1_000, 0.0, :gauge}, {2_000, 5.0e-324, :gauge}]
    assert {:ok, _state, [first, second]} = ScoringElixir.score(initial_state(), samples)
    assert first.score == 0.0
    assert second.score > 0.0
  end

  test "non-finite exposition values do not alter the rolling baseline" do
    assert {:ok, before, _} = ScoringElixir.score(initial_state(), [{1_000, 10.0, :gauge}])

    assert {:ok, after_state, [nan, finite]} =
             ScoringElixir.score(before, [{2_000, :nan, :gauge}, {3_000, 11.0, :gauge}])

    assert nan.score == nil
    assert finite.score > 0.0
    assert {:ok, decoded} = State.decode(after_state)
    assert decoded.window == [10.0, 11.0]
  end

  test "preserves finite large-magnitude divisions" do
    samples = [{1_000, 0.0, :gauge}, {2_000, 2.0e200, :gauge}, {3_000, 3.0e200, :gauge}]
    assert {:ok, _state, results} = ScoringElixir.score(initial_state(), samples)
    assert List.last(results).score > 20.0
    assert List.last(results).score < 40.0
  end

  test "computes non-negative counter rates and handles resets" do
    samples = [{1_000, 10.0, :counter}, {2_000, 12.0, :counter}, {3_000, 3.0, :counter}]
    assert {:ok, _state, [first, second, third]} = ScoringElixir.score(initial_state(), samples)
    assert first.score == nil
    assert second.observed == 2.0
    assert third.observed == 3.0
    assert third.observed >= 0.0
  end

  property "Feature: v1, Property 8: counter rates are never negative" do
    check all(
            samples <-
              list_of(tuple({integer(-100_000..100_000), integer(-100_000..100_000)}),
                min_length: 1,
                max_length: 100
              )
          ) do
      samples = Enum.map(samples, fn {timestamp, value} -> {timestamp, value, :counter} end)

      assert {:ok, _state, results} = ScoringElixir.score(initial_state(), samples)

      expected =
        Enum.map_reduce(samples, nil, fn {timestamp, value, _kind}, previous ->
          result =
            case previous do
              nil ->
                nil

              {previous_timestamp, _previous_value} when timestamp <= previous_timestamp ->
                nil

              {previous_timestamp, previous_value} ->
                seconds = (timestamp - previous_timestamp) / 1000.0

                if value < previous_value do
                  max(value / seconds, 0.0)
                else
                  max((value - previous_value) / seconds, 0.0)
                end
            end

          next_previous =
            case previous do
              nil -> {timestamp, value}
              {previous_timestamp, _} when timestamp > previous_timestamp -> {timestamp, value}
              previous -> previous
            end

          {{result, next_previous}, next_previous}
        end)
        |> elem(0)
        |> Enum.map(&elem(&1, 0))

      assert length(results) == length(samples)

      Enum.zip(results, expected)
      |> Enum.each(fn {result, expected_rate} ->
        if expected_rate do
          assert is_number(result.score)
          assert result.observed >= 0.0
          assert_in_delta result.observed, expected_rate, 1.0e-12
        else
          assert result.observed == nil
          assert result.score == nil
        end
      end)
    end
  end

  # **Validates: Requirements 9.5**
  # Feature: v1, Property 13: Score monotonicity
  property "Feature: v1, Property 13: score monotonicity" do
    check all(
            baseline <- list_of(integer(-1_000..1_000), min_length: 3, max_length: 32),
            side <- member_of([1, -1]),
            near_delta <- integer(1..1_000),
            additional_delta <- integer(1..1_000)
          ) do
      baseline = Enum.map(baseline, &(&1 * 1.0))
      median = property_13_median(baseline)
      near = median + side * near_delta
      far = near + side * additional_delta

      baseline_samples =
        Enum.with_index(baseline, 1)
        |> Enum.map(fn {value, index} -> {index * 1_000, value, :gauge} end)

      candidate_timestamp = (length(baseline) + 1) * 1_000

      assert {:ok, baseline_state, _results} =
               ScoringElixir.score(initial_state(), baseline_samples)

      assert {:ok, _near_state, [near_result]} =
               ScoringElixir.score(baseline_state, [{candidate_timestamp, near, :gauge}])

      assert {:ok, _far_state, [far_result]} =
               ScoringElixir.score(baseline_state, [{candidate_timestamp, far, :gauge}])

      assert near_result.score >= 0.0
      assert far_result.score >= near_result.score
    end
  end

  defp property_13_median(values) do
    sorted = Enum.sort(values)
    middle = div(length(sorted), 2)

    if rem(length(sorted), 2) == 1,
      do: Enum.at(sorted, middle),
      else: (Enum.at(sorted, middle - 1) + Enum.at(sorted, middle)) / 2
  end

  # **Validates: Requirements 6.1, 6.3, 6.4**
  property "Feature: v1, Property 10: Typical_Interval comes only from positive spacings" do
    check all(
            start <- integer(-1_000_000..1_000_000),
            positive_samples <-
              list_of(
                tuple({integer(1..1_000), integer(-1_000..1_000)}),
                min_length: 18,
                max_length: 50
              ),
            nonpositive_samples <-
              list_of(
                tuple({integer(-1_000..0), integer(-1_000..1_000)}),
                min_length: 1,
                max_length: 20
              )
          ) do
      steps = positive_samples ++ nonpositive_samples

      {samples, _timestamp} =
        Enum.map_reduce(steps, start, fn {step, value}, timestamp ->
          timestamp = timestamp + step
          {{timestamp, value, :gauge}, timestamp}
        end)

      assert {:ok, _state, results} = ScoringElixir.score(initial_state(), samples)
      assert length(results) == length(samples)
      assert Enum.map(results, & &1.observed) == Enum.map(samples, &(elem(&1, 1) * 1.0))

      {expected, _final_state} =
        Enum.map_reduce(samples, {nil, []}, fn {timestamp, _value, _kind}, {previous, spacings} ->
          spacing =
            case previous do
              {previous_timestamp, _} when timestamp > previous_timestamp ->
                timestamp - previous_timestamp

              _ ->
                nil
            end

          spacings =
            if spacing do
              Enum.take((spacings ++ [spacing]) |> Enum.reverse(), 16) |> Enum.reverse()
            else
              spacings
            end

          typical =
            case Enum.sort(spacings) do
              [] ->
                nil

              sorted when rem(length(sorted), 2) == 1 ->
                Enum.at(sorted, div(length(sorted), 2))

              sorted ->
                middle = div(length(sorted), 2)
                left = Enum.at(sorted, middle - 1)
                right = Enum.at(sorted, middle)
                left + div(right - left, 2)
            end

          next_previous =
            case previous do
              nil -> {timestamp, 0}
              {previous_timestamp, _} when timestamp > previous_timestamp -> {timestamp, 0}
              previous -> previous
            end

          {{typical, spacing != nil}, {next_previous, spacings}}
        end)

      {expected_intervals, expected_spacing_ok} =
        Enum.unzip(expected)

      assert Enum.map(results, & &1.typical_interval_ms) == expected_intervals
      assert Enum.map(results, & &1.spacing_ok?) == expected_spacing_ok
    end
  end

  # **Validates: Requirements 5.4, 6.2, 9.1, 9.2, 16.1**
  property "Feature: v1, Property 9: scoring kernel matches the reference model" do
    check all(
            kind <- member_of([:gauge, :counter]),
            window_size <- integer(1..8),
            samples <-
              list_of(
                tuple({integer(-10_000..10_000), integer(-1_000..1_000)}),
                min_length: 1,
                max_length: 40
              )
          ) do
      samples = Enum.map(samples, fn {timestamp, value} -> {timestamp, value, kind} end)
      params = %Params{window_size: window_size}

      assert {:ok, state_bin, actual} = ScoringElixir.score(initial_state(), samples, params)
      {expected, expected_window} = reference_results(samples, params)

      assert length(actual) == length(samples)
      assert length(expected_window) <= params.window_size

      Enum.zip(actual, expected)
      |> Enum.each(fn {result, expected_result} ->
        assert result.ts == expected_result.ts
        assert result.observed == expected_result.observed

        assert result.score == expected_result.score or
                 (is_float(result.score) and is_float(expected_result.score) and
                    abs(result.score - expected_result.score) < 1.0e-10)

        assert result.lower == expected_result.lower or
                 (is_float(result.lower) and is_float(expected_result.lower) and
                    abs(result.lower - expected_result.lower) < 1.0e-10)

        assert result.upper == expected_result.upper or
                 (is_float(result.upper) and is_float(expected_result.upper) and
                    abs(result.upper - expected_result.upper) < 1.0e-10)
      end)

      assert {:ok, decoded} = State.decode(state_bin)
      assert decoded.window == expected_window
      assert decoded.longterm == nil

      assert byte_size(state_bin) <=
               byte_size(
                 State.encode(%State{
                   capacity: 256,
                   window: List.duplicate(0.0, 256),
                   prev: {0, 0.0},
                   spacing: List.duplicate(1, 16)
                 })
               )
    end
  end

  defp reference_results(samples, params) do
    {results, model} =
      Enum.map_reduce(samples, %{window: [], previous: nil, kind: :gauge}, fn {ts, value, kind},
                                                                              model ->
        model =
          if model.kind != kind and model.window != [],
            do: %{model | window: [], previous: nil},
            else: model

        {observed, scored?} = reference_observed(ts, value, kind, model.previous)
        result = reference_result(ts, observed, scored?, model.window, params)

        next_window =
          if scored?,
            do: push_reference(model.window, observed, params.window_size),
            else: model.window

        next_previous = latest_reference(model.previous, {ts, value})
        {result, %{window: next_window, previous: next_previous, kind: kind}}
      end)

    {results, model.window}
  end

  defp reference_observed(_ts, _value, :counter, nil), do: {nil, false}

  defp reference_observed(ts, _value, :counter, {previous_ts, _}) when ts <= previous_ts,
    do: {nil, false}

  defp reference_observed(ts, value, :counter, {previous_ts, previous_value}) do
    seconds = (ts - previous_ts) / 1000.0
    rate = if value < previous_value, do: value / seconds, else: (value - previous_value) / seconds
    {max(rate, 0.0), true}
  end

  defp reference_observed(_ts, value, :gauge, _previous), do: {value * 1.0, true}

  defp reference_result(ts, nil, false, _window, _params),
    do: %{ts: ts, observed: nil, score: nil, lower: nil, upper: nil}

  defp reference_result(ts, value, true, [], _params),
    do: %{ts: ts, observed: value, score: 0.0, lower: value, upper: value}

  defp reference_result(ts, value, true, window, params) do
    median = reference_median(window)
    mad = reference_median(Enum.map(window, &abs(&1 - median)))

    scale =
      max(params.mad_scale * mad, max(params.scale_floor, params.scale_fraction * abs(median)))

    z = abs(value - median) / scale
    score = 100.0 * (1.0 - :math.exp(-z * :math.log(2.0) / 3.0))
    threshold_z = -3.0 * :math.log(1.0 - params.score_threshold / 100.0) / :math.log(2.0)
    distance = threshold_z * scale

    %{ts: ts, observed: value, score: score, lower: median - distance, upper: median + distance}
  end

  defp reference_median(values) do
    sorted = Enum.sort(values)
    middle = div(length(sorted), 2)

    if rem(length(sorted), 2) == 1,
      do: Enum.at(sorted, middle),
      else: (Enum.at(sorted, middle - 1) + Enum.at(sorted, middle)) / 2
  end

  defp push_reference(window, value, capacity) do
    values = window ++ [value]
    if length(values) > capacity, do: Enum.drop(values, length(values) - capacity), else: values
  end

  defp latest_reference(nil, sample), do: sample
  defp latest_reference({previous_ts, _}, {ts, _} = sample) when ts > previous_ts, do: sample
  defp latest_reference(previous, _sample), do: previous

  test "rejects malformed state, samples, and invalid parameters" do
    assert {:error, :bad_state} = ScoringElixir.score(<<0>>, [], %Params{})
    assert {:error, :invalid_samples} = ScoringElixir.score(initial_state(), [{1, 2}])
    assert {:error, :invalid_params} = ScoringElixir.score(initial_state(), [], %{window_size: 0})
  end

  # **Validates: Requirements 9.4, 12.3, 13.1, 17.1, 17.2**
  property "Feature: v1, Property 12: scores are bounded and always explained" do
    check all(values <- nonempty(list_of(integer(-1_000..1_000), max_length: 100))) do
      samples =
        values
        |> Enum.with_index(1)
        |> Enum.map(fn {value, index} -> {index * 1_000, value * 1.0, :gauge} end)

      assert {:ok, _state, results} = ScoringElixir.score(initial_state(), samples)
      assert length(results) == length(samples)

      Enum.each(results, fn result ->
        assert is_float(result.score)
        assert result.score >= 0.0 and result.score <= 100.0
        assert is_float(result.observed)
        assert is_float(result.lower) and is_float(result.upper)
        assert result.lower == result.lower
        assert result.upper == result.upper
        assert abs(result.lower) <= 1.797_693_134_862_315_7e308
        assert abs(result.upper) <= 1.797_693_134_862_315_7e308
        assert result.lower <= result.upper
        assert result.baseline_type == :robust
      end)
    end
  end
end

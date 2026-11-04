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

  test "rejects malformed state, samples, and invalid parameters" do
    assert {:error, :bad_state} = ScoringElixir.score(<<0>>, [], %Params{})
    assert {:error, :invalid_samples} = ScoringElixir.score(initial_state(), [{1, 2}])
    assert {:error, :invalid_params} = ScoringElixir.score(initial_state(), [], %{window_size: 0})
  end
end

defmodule Autoangel.Kernel.ScoringTest do
  use ExUnit.Case, async: true

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

  test "rejects malformed state, samples, and invalid parameters" do
    assert {:error, :bad_state} = ScoringElixir.score(<<0>>, [], %Params{})
    assert {:error, :invalid_samples} = ScoringElixir.score(initial_state(), [{1, 2}])
    assert {:error, :invalid_params} = ScoringElixir.score(initial_state(), [], %{window_size: 0})
  end
end

defmodule Autoangel.Detect.Property11ConstantGaugeTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Autoangel.Detect.Decision
  alias Autoangel.Detect.Decision.Meta
  alias Autoangel.Kernel.Scoring.Elixir, as: ScoringElixir
  alias Autoangel.Kernel.State

  # **Validates: Requirements 6.5**
  # Feature: v1, Property 11: constant gauges never raise an anomaly.
  #
  # For any constant value and any sequence of spacings that keeps every gap
  # below the missing-data multiple times the current Typical_Interval, feeding
  # the Series through learning and beyond raises no Anomaly of any kind.
  #
  # The learning duration is one second (1 * 1_000 ms). Spacings are generated
  # inside a band whose maximum is strictly less than three times its minimum,
  # so for any median spacing the gap stays below missing_multiple * typical and
  # no missing-data sweep could ever qualify. We drive BOTH anomaly kinds in
  # true chronological order: each sample at ts_i, then one or more missing-data
  # sweeps strictly inside the gap (ts_i, ts_{i+1}), then the next sample at
  # ts_{i+1}. Because every gap stays below missing_multiple * typical, and the
  # sweep offset from the last received sample is strictly smaller than the gap,
  # no sweep ever qualifies, so no missing anomaly can start. A constant value
  # scores 0.0 (well under the threshold of 75), so no value anomaly can start
  # either.
  @missing_multiple 3.0
  @learning_seconds 1
  @cfg %{
    learning: %{duration: @learning_seconds},
    persistence: %{n: 3, m: 5},
    missing_data: %{multiple: @missing_multiple}
  }

  # Minimum spacing floor; the band [spacing_floor, spacing_floor * 3 - 1]
  # guarantees max_gap < 3 * min_gap <= missing_multiple * typical_interval.
  @spacing_floor 1_000

  property "Feature: v1, Property 11: constant gauges never raise an anomaly" do
    check all(
            value <- float(min: -1.0e6, max: 1.0e6),
            base_ts <- integer(-1_000_000..1_000_000),
            spacings <-
              list_of(integer(@spacing_floor..(@spacing_floor * 3 - 1)),
                min_length: 30,
                max_length: 120
              )
          ) do
      timestamps = timestamps_from(base_ts, spacings)

      # Sanity: the generated spacings keep every gap strictly below
      # missing_multiple * typical_interval, so the precondition holds.
      typical = median_spacing(spacings)
      assert Enum.all?(spacings, &(&1 < @missing_multiple * typical))

      # The stream must cross the learning duration (1 second) and continue
      # well beyond it; the minimum spacing already guarantees this.
      total_span = List.last(timestamps) - List.first(timestamps)
      assert total_span > @learning_seconds * 1_000

      samples = Enum.map(timestamps, fn ts -> {ts, value, :gauge} end)

      assert {:ok, _state, results} =
               ScoringElixir.score(State.encode(State.new()), samples)

      # Constant gauge values always score exactly zero.
      assert Enum.all?(results, &(&1.score == 0.0))

      {final_meta, events} = run_pipeline(results)

      # No anomaly of any kind (value or missing) is ever started, even though
      # we drive a missing-data sweep strictly inside every gap.
      assert Enum.filter(events, &match?({:anomaly_started, _}, &1)) == []

      # The series reaches :healthy after learning and stays healthy.
      assert final_meta.status == :healthy
    end
  end

  # Fold every kernel result through Decision.step in chronological order,
  # interleaving missing-data sweeps strictly inside each gap between
  # consecutive samples. A sweep only qualifies when meta.last_recv_ms is a
  # number, so we drive sweeps only for gaps that follow the first processed
  # sample (last_recv_ms is set by that first sample step). All events from both
  # the sample steps and the sweep steps are collected.
  defp run_pipeline(results) do
    {final_meta, events} =
      Enum.reduce(results, {%Meta{}, [], nil}, fn result, {meta, events, prev_ts} ->
        ts = result.ts

        # Drive sweeps inside the gap (prev_ts, ts) once the first sample has
        # been processed (prev_ts is the previous sample's recv time, so
        # meta.last_recv_ms == prev_ts and is a number here).
        {meta, events} = drive_sweeps(meta, events, sweep_times(prev_ts, ts))

        # Then the sample at ts.
        recv_ms = ts
        result = Map.merge(result, %{recv_ms: recv_ms, kind: :gauge})
        {meta, sample_events} = Decision.step(meta, result, recv_ms, @cfg)
        {meta, events ++ sample_events, ts}
      end)
      |> then(fn {meta, events, _prev_ts} -> {meta, events} end)

    {final_meta, events}
  end

  # Drive each sweep time through Decision.step, threading meta and collecting
  # every emitted event.
  defp drive_sweeps(meta, events, sweep_times) do
    Enum.reduce(sweep_times, {meta, events}, fn sweep_time, {meta, events} ->
      {meta, sweep_events} = Decision.step(meta, :missing_sweep, sweep_time, @cfg)
      {meta, events ++ sweep_events}
    end)
  end

  # Sweep times strictly inside the gap (prev_ts, ts). Returns [] for the first
  # sample (prev_ts == nil) so no sweep runs before last_recv_ms is set, and []
  # for degenerate gaps with no interior integer point. Always includes the
  # point just before the next sample (ts - 1); adds the gap midpoint when it
  # lands strictly inside and differs from ts - 1.
  defp sweep_times(nil, _ts), do: []

  defp sweep_times(prev_ts, ts) when ts - prev_ts <= 1, do: []

  defp sweep_times(prev_ts, ts) do
    just_before = ts - 1
    mid = div(prev_ts + ts, 2)

    if mid > prev_ts and mid < just_before do
      [mid, just_before]
    else
      [just_before]
    end
  end

  # Cumulative-sum generated spacings into absolute timestamps.
  defp timestamps_from(base_ts, spacings) do
    {timestamps, _last} =
      Enum.map_reduce(spacings, base_ts, fn gap, ts ->
        next = ts + gap
        {next, next}
      end)

    [base_ts | timestamps]
  end

  defp median_spacing(spacings) do
    sorted = Enum.sort(spacings)
    n = length(sorted)

    if rem(n, 2) == 1 do
      Enum.at(sorted, div(n, 2))
    else
      left = Enum.at(sorted, div(n, 2) - 1)
      right = Enum.at(sorted, div(n, 2))
      (left + right) / 2
    end
  end
end

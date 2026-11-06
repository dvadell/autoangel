defmodule Autoangel.Detect.Property16LearningTest do
  @moduledoc """
  Property 16 (Requirements 10.1, 10.2, 10.3, 10.4, 11.8).

  "For any sequence of Samples and Anomalies for a Series, no alert reaches the
  Alert_Dispatcher while the Series is learning, and the status changes to
  healthy at the first Sample whose timestamp is at least the learning duration
  after the first Sample (or, with a seasonal period, when the Samples span the
  configured number of cycles), never earlier."

  `Autoangel.Detect.Decision` is a pure function and has no Alert_Dispatcher, so
  "no alert while learning" is expressed structurally: any anomaly STARTED while
  the Series is learning carries `learning?: true`, and the status stays
  `:learning` until the learning window has elapsed. These properties exercise
  `Decision.step/4` directly with generated integer timestamps.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Autoangel.Detect.Decision
  alias Decision.Meta

  # Scores below threshold (75) can never be anomalous; scores at/above can be.
  defp result(ts, score),
    do: %{ts: ts, recv_ms: ts, score: score, observed: 10.0, lower: 0.0, upper: 1.0}

  # Fold Decision.step across a sample sequence, threading meta and recording,
  # for each step, the index, the sample ts, the meta AFTER the step, and the
  # events emitted by that step.
  defp run_sequence(samples, cfg) do
    {_meta, steps} =
      samples
      |> Enum.with_index()
      |> Enum.reduce({%Meta{}, []}, fn {{ts, score}, index}, {meta, acc} ->
        {meta_after, events} = Decision.step(meta, result(ts, score), ts, cfg)
        {meta_after, [%{index: index, ts: ts, meta: meta_after, events: events} | acc]}
      end)

    Enum.reverse(steps)
  end

  # Generates a strictly increasing sequence of {ts, score} samples. The first
  # timestamp and the gaps are drawn so the sequence spans a range comparable to
  # the learning duration, ensuring some samples fall inside and some outside the
  # learning window. Scores straddle the default threshold of 75.
  defp sample_sequence(duration_seconds) do
    window_ms = duration_seconds * 1_000

    gen all(
          first_ts <- integer(0..1_000),
          gaps <- list_of(integer(1..max(1, div(window_ms, 3))), min_length: 3, max_length: 25),
          scores <- list_of(float(min: 0.0, max: 100.0), length: length(gaps) + 1)
        ) do
      {timestamps, _} =
        Enum.map_reduce(gaps, first_ts, fn gap, prev ->
          next = prev + gap
          {next, next}
        end)

      timestamps = [first_ts | timestamps]
      Enum.zip(timestamps, scores)
    end
  end

  property "status stays :learning until the duration elapses, then flips, never earlier" do
    check all(
            duration <- integer(1..10),
            samples <- sample_sequence(duration),
            max_runs: 200
          ) do
      window_ms = duration * 1_000
      n = 1
      cfg = %{learning: %{duration: duration}, persistence: %{n: n, m: 5}}

      steps = run_sequence(samples, cfg)
      [%{ts: first_ts} | _] = steps

      # Index of the first sample whose timestamp is at least `window_ms` after
      # the first sample. nil when the sequence never spans the window.
      threshold_index =
        Enum.find_value(steps, fn %{index: index, ts: ts} ->
          if ts - first_ts >= window_ms, do: index, else: nil
        end)

      Enum.each(steps, fn %{index: index, ts: ts, meta: meta, events: events} ->
        within_window? = ts - first_ts < window_ms

        if within_window? do
          # (1) Still inside the learning window: status is :learning.
          assert meta.status == :learning,
                 "expected :learning at index #{index} (ts #{ts}, first #{first_ts}), got #{meta.status}"

          # Any anomaly started while learning is flagged learning? == true (11.8).
          Enum.each(events, fn
            {:anomaly_started, anomaly} ->
              assert anomaly.learning? == true,
                     "anomaly started during learning at index #{index} must carry learning? == true"

            _ ->
              :ok
          end)

          # No health/anomalous transition should surface before the threshold.
          refute {:status, :healthy} in events,
                 "unexpected :healthy transition at index #{index} before learning elapsed"

          refute {:status, :anomalous} in events,
                 "unexpected :anomalous transition at index #{index} before learning elapsed"
        else
          # (2) At/after the first sample past the window: no longer :learning,
          # and the status is exactly what set_status/1 dictates. With no missing
          # sweep in this sequence open_missing is always nil, so the status is
          # :anomalous when a value anomaly is open and :healthy otherwise.
          expected_status = if meta.open_value_anomaly, do: :anomalous, else: :healthy

          assert meta.status == expected_status,
                 "expected status #{expected_status} at index #{index} (ts #{ts}, first #{first_ts}), got #{meta.status}"

          # Any anomaly started at/after the threshold is learning? == false (11.8).
          Enum.each(events, fn
            {:anomaly_started, anomaly} ->
              assert anomaly.learning? == false,
                     "anomaly started after learning at index #{index} must carry learning? == false"

            _ ->
              :ok
          end)
        end
      end)

      # (2 cont.) When a threshold sample exists, every strictly prior step is
      # still learning, and from the threshold step onward it never is.
      if threshold_index do
        Enum.each(steps, fn %{index: index, meta: meta} ->
          if index < threshold_index do
            assert meta.status == :learning,
                   "status must remain :learning before threshold index #{threshold_index}, index #{index}"
          else
            expected_status = if meta.open_value_anomaly, do: :anomalous, else: :healthy

            assert meta.status == expected_status,
                   "status must be #{expected_status} from threshold index #{threshold_index}, index #{index}, got #{meta.status}"
          end
        end)
      else
        # Never spanned the window: must have remained learning throughout.
        Enum.each(steps, fn %{meta: meta} -> assert meta.status == :learning end)
      end
    end
  end

  # Example-based coverage of the seasonal branch: with a seasonal period the
  # learning window is `cycles * period` (ms), not duration-based. Status must
  # stay :learning until the samples span two cycles, then flip to healthy.
  test "seasonal learning finishes only after samples span the configured cycles" do
    period = 1_000
    cfg = %{learning: %{cycles: 2}, persistence: %{n: 1, m: 5}}
    meta = %Meta{seasonal: {:daily, period}}

    {meta, _} = Decision.step(meta, result(0, 0.0), 0, cfg)
    assert meta.status == :learning
    assert meta.first_ts == 0

    # One cycle elapsed: still learning (needs two cycles = 2_000ms span).
    {meta, _} = Decision.step(meta, result(1_000, 0.0), 1_000, cfg)
    assert meta.status == :learning

    # Just short of two cycles: still learning.
    {meta, _} = Decision.step(meta, result(1_999, 0.0), 1_999, cfg)
    assert meta.status == :learning

    # Spans exactly two cycles: learning ends.
    {meta, _} = Decision.step(meta, result(2_000, 0.0), 2_000, cfg)
    assert meta.status == :healthy
  end

  # Deterministic boundary example for the NON-seasonal duration branch. This
  # pins the `>=` in maybe_finish_learning/3 (`latest_ts - first >= duration *
  # 1_000`): a sample one millisecond short of the window must still be
  # :learning, and the sample exactly at the window must leave :learning. With a
  # `>=`→`>` mutation the ts=1000 sample would remain :learning and this fails.
  test "non-seasonal learning finishes exactly at duration*1000 (boundary example)" do
    cfg = %{learning: %{duration: 1}, persistence: %{n: 1, m: 5}}

    # First sample at ts=0: first_ts becomes 0, series is learning.
    {meta, _} = Decision.step(%Meta{}, result(0, 0.0), 0, cfg)
    assert meta.first_ts == 0
    assert meta.status == :learning

    # ts=999 == duration*1000 - 1: still inside the window, still :learning.
    {meta, _} = Decision.step(meta, result(999, 0.0), 999, cfg)
    assert meta.status == :learning

    # ts=1000 == duration*1000: window elapsed, learning ends. The sample is
    # non-anomalous (score 0.0) with no open anomaly, so status is :healthy.
    {meta, _} = Decision.step(meta, result(1_000, 0.0), 1_000, cfg)
    assert meta.status == :healthy
  end
end

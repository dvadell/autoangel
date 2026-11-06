defmodule Autoangel.Detect.Property17PersistenceTest do
  @moduledoc """
  Property 17 — persistence rule (Requirements 14.2, 14.3, 14.4, 14.5).

  For any sequence of scores (including nil scores) and timestamps, a value
  Anomaly starts exactly at the first Sample where no Anomaly is open and the
  current anomalous run has at least N Samples or spans at least M minutes; its
  start time is the first timestamp of that run; a non-anomalous Sample resets
  the run; nil scores neither extend nor reset it; and no second Anomaly starts
  while one is open.

  Strategy: drive `Autoangel.Detect.Decision.step/4` directly with a config of
  `%{learning: %{duration: 0}, persistence: %{n: N, m: M}}`. With `duration: 0`
  and no seasonal period, `maybe_finish_learning/3` promotes the series to
  `:healthy` on the very first sample (`latest_ts - first_ts == 0 >= 0`), so the
  learning gate never interferes with the persistence logic under test.

  The effective threshold is 75: the cfg carries no `:sensitivity` key and the
  meta has no `:sensitivity_override`, so `effective_threshold/2` computes
  `clamp(100 - (100 - 75) * 1.0, 1.0, 99.9) == 75.0`. Direction defaults to
  `:both` (no `:direction_override`), which makes `direction_ok?` always true,
  so `anomalous?` reduces to `score >= 75`.

  The oracle below is an INDEPENDENT reimplementation of the run model; it does
  not call any Decision internals.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Autoangel.Detect.Decision
  alias Decision.Meta

  @m_minutes 5
  @m_ms @m_minutes * 60_000

  # ---------------------------------------------------------------------------
  # Generators
  # ---------------------------------------------------------------------------

  # One entry in the series. :nil => nil score; :low => finite, below threshold;
  # :high => finite, at/above threshold.
  defp entry_gen do
    one_of([
      constant(:nil_score),
      map(float(min: 0.0, max: 74.0), fn s -> {:low, s} end),
      map(float(min: 75.0, max: 100.0), fn s -> {:high, s} end)
    ])
  end

  # A positive timestamp delta. A spread of small and large gaps so that an
  # M-minute span is sometimes reachable within a short run and sometimes not.
  defp delta_gen do
    one_of([
      integer(1..5_000),
      integer(50_000..400_000)
    ])
  end

  # Attach monotonically increasing timestamps to a list of entries, starting
  # just after `start_ts`.
  defp with_timestamps(entries, start_ts) do
    {acc, _ts} =
      Enum.map_reduce(entries, start_ts, fn {entry, delta}, prev ->
        ts = prev + delta
        {{ts, entry}, ts}
      end)

    acc
  end

  # ---------------------------------------------------------------------------
  # Oracle — independent reference run-model
  # ---------------------------------------------------------------------------

  # Classify a timestamped entry into {:ts, classification} where
  # classification is :high (anomalous), :low (non-anomalous finite), or :nil.
  defp classify({ts, :nil_score}), do: {ts, nil}
  defp classify({ts, {:low, _s}}), do: {ts, :low}
  defp classify({ts, {:high, _s}}), do: {ts, :high}

  # The oracle folds the classified sequence and returns the list of predicted
  # anomaly starts, in order, as `{emit_ts, first_ts}` pairs where `emit_ts` is
  # the timestamp of the sample at which the run reached persistence (the sample
  # that emits the start) and `first_ts` is the start time of the run.
  #
  # State:
  #   open?   - whether a value anomaly is currently open
  #   run     - current anomalous run (nil | %{count, first_ts, last_ts})
  #   calm    - current calm run while open (nil | %{count, first_ts, last_ts})
  defp oracle_starts(classified, n) do
    init = %{open?: false, run: nil, calm: nil, starts: []}

    final =
      Enum.reduce(classified, init, fn {ts, cls}, st ->
        oracle_step(st, ts, cls, n)
      end)

    Enum.reverse(final.starts)
  end

  # nil score: no change to run or calm (and cannot start/resolve).
  defp oracle_step(st, _ts, nil, _n), do: st

  # anomalous and no anomaly open: extend the run, maybe start. When it starts,
  # record the emitting sample ts alongside the run's first_ts.
  defp oracle_step(%{open?: false} = st, ts, :high, n) do
    run = extend(st.run, ts)

    if persists?(run, n) do
      %{st | run: nil, open?: true, calm: nil, starts: [{ts, run.first_ts} | st.starts]}
    else
      %{st | run: run, calm: nil}
    end
  end

  # anomalous while an anomaly is open: run and calm both reset, no new start.
  defp oracle_step(%{open?: true} = st, _ts, :high, _n) do
    %{st | run: nil, calm: nil}
  end

  # non-anomalous finite and no anomaly open: reset the run.
  defp oracle_step(%{open?: false} = st, _ts, :low, _n) do
    %{st | run: nil}
  end

  # non-anomalous finite while an anomaly is open: extend calm, maybe resolve.
  defp oracle_step(%{open?: true} = st, ts, :low, n) do
    calm = extend(st.calm, ts)

    if persists?(calm, n) do
      %{st | open?: false, calm: nil, run: nil}
    else
      %{st | calm: calm, run: nil}
    end
  end

  defp extend(nil, ts), do: %{count: 1, first_ts: ts, last_ts: ts}
  defp extend(run, ts), do: %{run | count: run.count + 1, last_ts: ts}

  defp persists?(run, n), do: run.count >= n or run.last_ts - run.first_ts >= @m_ms

  # ---------------------------------------------------------------------------
  # Driving the real Decision
  # ---------------------------------------------------------------------------

  defp cfg(n), do: %{learning: %{duration: 0}, persistence: %{n: n, m: @m_minutes}}

  # Build a sample result. observed/lower/upper are provided so that direction
  # resolution has values, but with direction :both they are ignored, keeping
  # anomalous? dependent only on score >= threshold.
  defp sample_result(ts, score),
    do: %{ts: ts, recv_ms: ts, score: score, observed: 10.0, lower: 0.0, upper: 1.0}

  defp score_for(:nil_score), do: nil
  defp score_for({:low, s}), do: s
  defp score_for({:high, s}), do: s

  # Run the real detector across the timestamped entries, returning the ordered
  # list of value-anomaly starts actually produced, as `{emit_ts, start_ms}`
  # pairs: `emit_ts` is the timestamp of the sample whose step emitted the
  # `:anomaly_started` event, and `start_ms` is the anomaly's recorded start.
  defp run_decision(timestamped, n) do
    cfg = cfg(n)

    # Priming sample at ts = 0, score 0.0 (non-anomalous). duration: 0 promotes
    # the series to :healthy on this first sample, so the generated sequence
    # (all ts > 0) runs entirely in the healthy regime.
    {meta0, _} = Decision.step(%Meta{}, sample_result(0, 0.0), 0, cfg)
    assert meta0.status == :healthy

    {_meta, starts} =
      Enum.reduce(timestamped, {meta0, []}, fn {ts, entry}, {meta, starts} ->
        {meta, events} = Decision.step(meta, sample_result(ts, score_for(entry)), ts, cfg)

        new_starts =
          for {:anomaly_started, %{kind: :value} = a} <- events, do: {ts, a.start_ms}

        {meta, starts ++ new_starts}
      end)

    starts
  end

  # ---------------------------------------------------------------------------
  # Properties
  # ---------------------------------------------------------------------------

  property "effective threshold is exactly 75 under the test configuration" do
    # Boundary check: a score of 74.0 never starts, a persisted run of 75.0 does.
    check all(n <- integer(1..5)) do
      entries = List.duplicate({74.0}, 10) |> Enum.map(fn {s} -> {:low, s} end)
      ts = with_timestamps(Enum.map(entries, &{&1, 1}), 0)
      assert run_decision(ts, n) == []
    end
  end

  property "value-anomaly starts match the independent oracle" do
    check all(
            n <- integer(1..5),
            entries <- list_of({entry_gen(), delta_gen()}, min_length: 1, max_length: 40),
            max_runs: 400
          ) do
      timestamped = with_timestamps(entries, 0)
      classified = Enum.map(timestamped, &classify/1)

      expected = oracle_starts(classified, n)
      actual = run_decision(timestamped, n)

      assert actual == expected,
             "n=#{n}\nclassified=#{inspect(classified)}\n" <>
               "expected=#{inspect(expected)}\nactual=#{inspect(actual)}"
    end
  end

  property "no second value anomaly starts while one is open (resolution required between starts)" do
    # Two consecutive starts in the actual output imply the detector resolved in
    # between; we assert each start corresponds to a fresh oracle run first_ts
    # and that starts are strictly increasing (a later run cannot reuse an
    # earlier timestamp).
    check all(
            n <- integer(1..5),
            entries <- list_of({entry_gen(), delta_gen()}, min_length: 1, max_length: 40),
            max_runs: 300
          ) do
      timestamped = with_timestamps(entries, 0)
      actual = run_decision(timestamped, n)

      assert actual == Enum.uniq(actual)
      assert actual == Enum.sort(actual)
    end
  end

  property "nil scores neither trigger nor delay a start" do
    # Compare a sequence containing nil scores against the same sequence with the
    # nil scores removed. Because nil neither extends nor resets the run, the two
    # must produce identical start_ms sequences.
    check all(
            n <- integer(1..5),
            entries <- list_of({entry_gen(), delta_gen()}, min_length: 1, max_length: 40),
            max_runs: 300
          ) do
      timestamped = with_timestamps(entries, 0)

      without_nils =
        Enum.reject(timestamped, fn {_ts, entry} -> entry == :nil_score end)

      assert run_decision(timestamped, n) == run_decision(without_nils, n)
    end
  end

  # ---------------------------------------------------------------------------
  # Deterministic boundary examples
  # ---------------------------------------------------------------------------

  # (17.2) N-count boundary. With n=3, three consecutive highs one second apart
  # start an anomaly whose start_ms is the first high's ts; two highs do not.
  # The exactly-N / exactly-75.0 run pins both the `count >= n` boundary (a
  # `count > n` mutation would need a 4th sample) and the `score >= 75` boundary.
  test "n-count boundary: exactly N highs at exactly 75.0 start, N-1 do not (example)" do
    n = 3

    three_highs = [
      {1_000, {:high, 90.0}},
      {2_000, {:high, 90.0}},
      {3_000, {:high, 90.0}}
    ]

    assert run_decision(three_highs, n) == [{3_000, 1_000}]

    two_highs = [
      {1_000, {:high, 90.0}},
      {2_000, {:high, 90.0}}
    ]

    assert run_decision(two_highs, n) == []

    # Exactly N samples at the exact threshold score 75.0 must still start,
    # pinning both the `>= 75` score boundary and the `count >= n` boundary.
    exactly_threshold = [
      {1_000, {:high, 75.0}},
      {2_000, {:high, 75.0}},
      {3_000, {:high, 75.0}}
    ]

    assert run_decision(exactly_threshold, n) == [{3_000, 1_000}]
  end

  # (17.3) M-minute-span boundary. With n=5, two highs can never reach the count
  # threshold, so only the span rule can start an anomaly. Two highs exactly
  # M*60_000 ms apart start one; two highs one millisecond short do not. This
  # pins the `last_ts - first_ts >= M*60000` boundary.
  test "m-minute span boundary: span == M*60000 starts, one ms short does not (example)" do
    n = 5

    at_span = [
      {0, {:high, 90.0}},
      {@m_ms, {:high, 90.0}}
    ]

    assert run_decision(at_span, n) == [{@m_ms, 0}]

    just_short = [
      {0, {:high, 90.0}},
      {@m_ms - 1, {:high, 90.0}}
    ]

    assert run_decision(just_short, n) == []
  end
end

defmodule Autoangel.Detect.Property19MissingDataTest do
  @moduledoc """
  Property 19 (Requirements 11.1-11.7, 15.6).

  "For any Sample arrival times, sweep schedule, source and Scrape_Target
  failing state, and direction setting, a missing-data Anomaly is raised at the
  first sweep after `now - last_recv` exceeds the multiple times the
  Typical_Interval (so within one sweep interval), never for a Series without a
  Typical_Interval, at most once per gap, with start time equal to that sweep
  time and classification \"target down\" exactly when the last source is a
  failing Scrape_Target; the status becomes silent only after learning; and the
  next Sample resolves it at its receive time and sets the status as defined in
  Requirement 11.6."

  `Autoangel.Detect.Decision` is a pure function. A missing-data sweep is driven
  by passing the atom `:missing_sweep` (or a map `%{sweep?: true}`) as the
  `result` argument, with `now_recv` being the sweep time. A scored (or
  unscored) Sample is driven by passing a result map; any Sample resolves an
  open missing anomaly at its `recv_ms`. These properties exercise
  `Decision.step/4` directly with generated integer timestamps.

  The tests carry an INDEPENDENT oracle for the two decidable predicates:

    * `should_raise?/5` — reimplements, from the specification text alone,
      whether a sweep at `now_recv` must raise a missing-data anomaly given the
      meta (last_recv_ms, typical_interval_ms, open_missing, status) and the
      `multiple`.
    * `oracle_classification/3` — reimplements the `:target_down` vs
      `:metric_silent` decision from the last source and the failing-target map.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Autoangel.Detect.Decision
  alias Decision.Meta

  # --- Independent oracle ---------------------------------------------------

  # Reimplements the spec predicate "should this sweep raise a missing-data
  # anomaly?" without consulting the implementation. A sweep raises iff:
  #   * there is a known last receive time, AND
  #   * the series has a Typical_Interval (nil => never), AND
  #   * now_recv - last_recv_ms strictly exceeds multiple * typical_interval, AND
  #   * no missing anomaly is already open (at most once per gap), AND
  #   * the series is not already silenced.
  defp should_raise?(now_recv, last_recv_ms, typical_interval_ms, multiple, meta) do
    is_number(last_recv_ms) and
      is_number(typical_interval_ms) and
      now_recv - last_recv_ms > multiple * typical_interval_ms and
      is_nil(meta.open_missing) and
      meta.status != :silent
  end

  # Reimplements the classification: "target down" exactly when the last source
  # is a Scrape_Target that is currently failing; otherwise the metric simply
  # went silent.
  defp oracle_classification(last_source, failing_targets, target_failing_override) do
    case last_source do
      {:scrape, target} ->
        failing? =
          case target_failing_override do
            nil -> Map.get(failing_targets, target, false)
            value -> value
          end

        if failing?, do: :target_down, else: :metric_silent

      _ ->
        :metric_silent
    end
  end

  # --- Drivers --------------------------------------------------------------

  # A single sweep step: atom `:missing_sweep` with `now_recv` as the sweep time.
  defp sweep(meta, now_recv, cfg), do: Decision.step(meta, :missing_sweep, now_recv, cfg)

  # A single Sample step with a neutral (non-anomalous) score. `recv_ms == ts`.
  defp sample(meta, recv_ms, cfg) do
    result = %{ts: recv_ms, recv_ms: recv_ms, score: 0.0, observed: 10.0, lower: 0.0, upper: 1.0}
    Decision.step(meta, result, recv_ms, cfg)
  end

  # --- Generators -----------------------------------------------------------

  defp target_gen, do: member_of(["target-a", "target-b", "target-c"])

  defp last_source_gen do
    one_of([
      constant(nil),
      bind(target_gen(), fn t -> constant({:scrape, t}) end)
    ])
  end

  # Typical_Interval: a positive integer, or nil (series without one).
  defp typical_interval_gen do
    one_of([constant(nil), integer(1..10_000)])
  end

  defp failing_targets_gen do
    bind(list_of({target_gen(), boolean()}, max_length: 3), fn pairs ->
      constant(Map.new(pairs))
    end)
  end

  # --- Properties -----------------------------------------------------------

  property "a sweep raises iff the oracle predicate holds, with start_ms == sweep time" do
    check all(
            multiple <- float(min: 2.0, max: 5.0),
            typical <- typical_interval_gen(),
            last_recv_ms <- integer(0..10_000),
            # Draw a delta around the threshold so both qualify/non-qualify arise.
            delta <- integer(0..100_000),
            last_source <- last_source_gen(),
            failing_targets <- failing_targets_gen(),
            learning? <- boolean(),
            # (15.6) direction setting: sweeps must ignore it entirely.
            direction <- member_of([nil, :high, :low, :both]),
            max_runs: 400
          ) do
      now_recv = last_recv_ms + delta
      cfg = %{missing_data: %{multiple: multiple}, failing_targets: failing_targets}

      status = if learning?, do: :learning, else: :healthy

      meta = %Meta{
        key: "series",
        status: status,
        first_ts: 0,
        last_recv_ms: last_recv_ms,
        last_source: last_source,
        typical_interval_ms: typical,
        direction_override: direction
      }

      {updated, events} = sweep(meta, now_recv, cfg)

      expected_raise? = should_raise?(now_recv, last_recv_ms, typical, multiple, meta)

      started =
        Enum.find_value(events, fn
          {:anomaly_started, a} -> a
          _ -> nil
        end)

      if expected_raise? do
        # (1) Raises exactly when the oracle says so.
        assert started != nil,
               "oracle expected a raise (now #{now_recv}, last #{last_recv_ms}, " <>
                 "typical #{inspect(typical)}, multiple #{multiple}) but none was emitted"

        assert started.kind == :missing
        # (1 cont.) Start time equals the sweep time.
        assert started.start_ms == now_recv
        assert updated.open_missing != nil

        # (4) Classification matches the oracle.
        expected_class = oracle_classification(last_source, failing_targets, nil)
        assert started.classification == expected_class

        # (6 / 11.7 / 15.6) Status becomes :silent only when NOT learning;
        # while learning a qualifying sweep keeps the status :learning.
        if learning? do
          assert updated.status == :learning,
                 "a qualifying sweep while learning must keep status :learning"

          assert started.learning? == true
        else
          assert updated.status == :silent,
                 "a qualifying sweep while not learning must silence the series"

          assert started.learning? == false
        end
      else
        # No raise: no anomaly_started event, open_missing unchanged.
        assert started == nil,
               "oracle expected NO raise but an anomaly was emitted (now #{now_recv}, " <>
                 "last #{last_recv_ms}, typical #{inspect(typical)}, multiple #{multiple})"

        assert updated.open_missing == meta.open_missing
      end
    end
  end

  property "a series without a Typical_Interval never raises a missing-data anomaly" do
    check all(
            multiple <- float(min: 2.0, max: 5.0),
            last_recv_ms <- integer(0..10_000),
            delta <- integer(0..1_000_000),
            last_source <- last_source_gen(),
            learning? <- boolean(),
            max_runs: 200
          ) do
      now_recv = last_recv_ms + delta
      cfg = %{missing_data: %{multiple: multiple}}
      status = if learning?, do: :learning, else: :healthy

      meta = %Meta{
        key: "series",
        status: status,
        first_ts: 0,
        last_recv_ms: last_recv_ms,
        last_source: last_source,
        typical_interval_ms: nil
      }

      {updated, events} = sweep(meta, now_recv, cfg)

      # (2) No Typical_Interval => threshold is :infinity => never qualifies.
      assert events == []
      assert updated.open_missing == nil
      assert updated.status == status
    end
  end

  property "at most once per gap: repeated sweeps raise once, then nothing until a sample resolves" do
    check all(
            multiple <- float(min: 2.0, max: 5.0),
            typical <- integer(1..1_000),
            last_recv_ms <- integer(0..10_000),
            # Number of sweeps that each exceed the threshold within one gap.
            extra_sweeps <- integer(1..6),
            last_source <- last_source_gen(),
            failing_targets <- failing_targets_gen(),
            learning? <- boolean(),
            max_runs: 300
          ) do
      cfg = %{missing_data: %{multiple: multiple}, failing_targets: failing_targets}
      status = if learning?, do: :learning, else: :healthy
      threshold = multiple * typical

      meta = %Meta{
        key: "series",
        status: status,
        first_ts: 0,
        last_recv_ms: last_recv_ms,
        last_source: last_source,
        typical_interval_ms: typical
      }

      # All sweeps strictly exceed the threshold (same gap, monotonic times).
      base = last_recv_ms + ceil(threshold) + 1

      sweep_times =
        Enum.map(0..extra_sweeps, fn i -> base + i end)

      {meta_after, raise_count} =
        Enum.reduce(sweep_times, {meta, 0}, fn t, {m, count} ->
          {m2, events} = sweep(m, t, cfg)

          raised? =
            Enum.any?(events, fn
              {:anomaly_started, _} -> true
              _ -> false
            end)

          {m2, if(raised?, do: count + 1, else: count)}
        end)

      # (3) Exactly one raise for the whole gap, no matter how many sweeps fire.
      assert raise_count == 1,
             "expected exactly one raise per gap, got #{raise_count} over #{length(sweep_times)} sweeps"

      assert meta_after.open_missing != nil

      # A subsequent Sample resolves the open missing anomaly at its recv_ms.
      resolve_ms = base + extra_sweeps + 10
      {meta_resolved, resolve_events} = sample(meta_after, resolve_ms, cfg)

      resolved =
        Enum.find_value(resolve_events, fn
          {:anomaly_resolved, a} -> a
          _ -> nil
        end)

      assert resolved != nil, "a sample after an open gap must resolve the missing anomaly"
      assert resolved.end_ms == resolve_ms
      assert meta_resolved.open_missing == nil

      # (3 cont.) A fresh sweep AFTER resolution (new gap) can raise again.
      next_sweep_time = resolve_ms + ceil(threshold) + 1
      {_m, next_events} = sweep(meta_resolved, next_sweep_time, cfg)

      raised_again? =
        Enum.any?(next_events, fn
          {:anomaly_started, _} -> true
          _ -> false
        end)

      # It can only re-raise when the series is not silenced. When not learning,
      # the first raise silenced the series, so resolution via a non-anomalous
      # sample restores :healthy and a new gap can raise again.
      assert raised_again?,
             "after resolution a fresh over-threshold sweep must be able to raise again"
    end
  end

  property "classification is :target_down iff the last source is a failing scrape target" do
    check all(
            typical <- integer(1..1_000),
            last_recv_ms <- integer(0..10_000),
            last_source <- last_source_gen(),
            failing_targets <- failing_targets_gen(),
            use_result_override <- boolean(),
            override_value <- boolean(),
            max_runs: 300
          ) do
      multiple = 3.0
      now_recv = last_recv_ms + trunc(multiple * typical) + 1

      base_cfg = %{missing_data: %{multiple: multiple}, failing_targets: failing_targets}

      meta = %Meta{
        key: "series",
        status: :healthy,
        first_ts: 0,
        last_recv_ms: last_recv_ms,
        last_source: last_source,
        typical_interval_ms: typical
      }

      # The sweep "result" may carry :target_failing? to override the cfg map,
      # mirroring the implementation's precedence (result over cfg).
      {result_arg, override} =
        if use_result_override do
          {%{sweep?: true, target_failing?: override_value}, override_value}
        else
          {:missing_sweep, nil}
        end

      {_updated, events} = Decision.step(meta, result_arg, now_recv, base_cfg)

      started =
        Enum.find_value(events, fn
          {:anomaly_started, a} -> a
          _ -> nil
        end)

      assert started != nil, "an over-threshold sweep must raise here"

      expected = oracle_classification(last_source, failing_targets, override)

      # (4) Classification matches the independent oracle under both the cfg
      # map and the per-result override paths.
      assert started.classification == expected,
             "classification mismatch: source #{inspect(last_source)}, " <>
               "failing #{inspect(failing_targets)}, override #{inspect(override)}, " <>
               "got #{started.classification}, expected #{expected}"
    end
  end

  property "the next sample resolves an open missing anomaly at its receive time" do
    check all(
            typical <- integer(1..1_000),
            last_recv_ms <- integer(0..10_000),
            last_source <- last_source_gen(),
            failing_targets <- failing_targets_gen(),
            gap_after_sweep <- integer(1..10_000),
            max_runs: 300
          ) do
      multiple = 3.0
      cfg = %{missing_data: %{multiple: multiple}, failing_targets: failing_targets}
      sweep_time = last_recv_ms + trunc(multiple * typical) + 1

      meta = %Meta{
        key: "series",
        status: :healthy,
        first_ts: 0,
        last_recv_ms: last_recv_ms,
        last_source: last_source,
        typical_interval_ms: typical
      }

      {meta_open, sweep_events} = sweep(meta, sweep_time, cfg)

      assert Enum.any?(sweep_events, fn
               {:anomaly_started, _} -> true
               _ -> false
             end)

      assert meta_open.open_missing != nil
      assert meta_open.status == :silent

      recv_ms = sweep_time + gap_after_sweep
      {meta_resolved, events} = sample(meta_open, recv_ms, cfg)

      resolved =
        Enum.find_value(events, fn
          {:anomaly_resolved, a} -> a
          _ -> nil
        end)

      # (5) The next Sample resolves it at its recv_ms.
      assert resolved != nil, "the next sample must resolve the open missing anomaly"
      assert resolved.end_ms == recv_ms
      assert meta_resolved.open_missing == nil

      # (11.6) After resolution via a non-anomalous sample the series returns to
      # :healthy (it is no longer silent and has no open anomaly).
      assert meta_resolved.status == :healthy
    end
  end

  # Example-based anchor mirroring decision_test.exs idioms and pinning the
  # learning-vs-silent status rule (11.7 / 15.6) at a concrete point.
  test "learning sweep keeps :learning, healthy sweep silences (example)" do
    cfg = %{missing_data: %{multiple: 3.0}, failing_targets: %{"target" => true}}

    learning_meta = %Meta{
      status: :learning,
      first_ts: 0,
      last_recv_ms: 0,
      last_source: {:scrape, "target"},
      typical_interval_ms: 10
    }

    {learning_after, [{:anomaly_started, learning_anomaly} | _]} =
      Decision.step(learning_meta, :missing_sweep, 31, cfg)

    assert learning_anomaly.classification == :target_down
    assert learning_anomaly.start_ms == 31
    assert learning_anomaly.learning? == true
    assert learning_after.status == :learning

    healthy_meta = %{learning_meta | status: :healthy}

    {healthy_after, [{:anomaly_started, healthy_anomaly} | _]} =
      Decision.step(healthy_meta, :missing_sweep, 31, cfg)

    assert healthy_anomaly.classification == :target_down
    assert healthy_anomaly.learning? == false
    assert healthy_after.status == :silent
  end

  # (11.6 status-after-resolution, branch a) When the series was LEARNING when
  # the missing anomaly opened (a qualifying sweep keeps status :learning), a
  # subsequent resolving non-anomalous Sample that does NOT finish learning must
  # leave the status at :learning. This matches decision.ex: after
  # resolve_missing/2 clears open_missing, maybe_finish_learning/3 leaves the
  # still-within-window series :learning, and set_status/1 short-circuits on
  # :learning, so the status stays :learning.
  test "resolution while still learning keeps status :learning (example)" do
    # Long learning window so the resolving sample stays inside it.
    cfg = %{
      missing_data: %{multiple: 3.0},
      learning: %{duration: 86_400},
      persistence: %{n: 3, m: 5},
      failing_targets: %{"target" => true}
    }

    learning_meta = %Meta{
      status: :learning,
      first_ts: 0,
      last_recv_ms: 0,
      last_source: {:scrape, "target"},
      typical_interval_ms: 10
    }

    # A qualifying sweep while learning opens a missing anomaly and keeps
    # status :learning.
    {meta_open, _events} = Decision.step(learning_meta, :missing_sweep, 31, cfg)
    assert meta_open.open_missing != nil
    assert meta_open.status == :learning

    # Resolve with a non-anomalous sample well within the learning window.
    {meta_resolved, events} = sample(meta_open, 100, cfg)

    resolved =
      Enum.find_value(events, fn
        {:anomaly_resolved, a} -> a
        _ -> nil
      end)

    assert resolved != nil
    assert resolved.end_ms == 100
    assert meta_resolved.open_missing == nil

    # Still learning (the sample did not elapse the learning window).
    assert meta_resolved.status == :learning
  end

  # (11.6 status-after-resolution, branch b) When the resolving Sample still has
  # an open VALUE anomaly after resolution, set_status/1 reports :anomalous. We
  # construct a meta with BOTH open_missing and open_value_anomaly set and a
  # persistence config under which a single calm sample does not resolve the
  # value anomaly, so after the missing anomaly resolves the value anomaly
  # remains open and the status is :anomalous.
  test "resolution with an open value anomaly yields :anomalous (example)" do
    # n: 5 so a single calm sample (count 1) cannot resolve the value anomaly,
    # and the two samples are close in time so the M-span rule is not reached.
    cfg = %{
      missing_data: %{multiple: 3.0},
      learning: %{duration: 0},
      persistence: %{n: 5, m: 5},
      failing_targets: %{}
    }

    open_value = %Decision.Anomaly{
      kind: :value,
      classification: nil,
      start_ms: 50,
      score: 90.0
    }

    open_missing = %Decision.Anomaly{
      kind: :missing,
      classification: :metric_silent,
      start_ms: 40
    }

    meta = %Meta{
      status: :anomalous,
      first_ts: 0,
      last_recv_ms: 40,
      last_source: nil,
      typical_interval_ms: 10,
      open_value_anomaly: open_value,
      open_missing: open_missing
    }

    # A non-anomalous sample (score 0.0) resolves the missing anomaly at its
    # recv_ms; the value anomaly's calm run is only count 1 (< n) and spans no
    # M minutes, so it stays open.
    {meta_resolved, events} = sample(meta, 60, cfg)

    resolved =
      Enum.find_value(events, fn
        {:anomaly_resolved, %{kind: :missing} = a} -> a
        _ -> nil
      end)

    assert resolved != nil, "the missing anomaly must resolve"
    assert resolved.end_ms == 60
    assert meta_resolved.open_missing == nil

    # The value anomaly is still open, so set_status/1 reports :anomalous.
    assert meta_resolved.open_value_anomaly != nil
    assert meta_resolved.status == :anomalous
  end

  # (threshold boundary) The missing-data threshold uses a STRICT `>`:
  # `now_recv - last_recv_ms > multiple * typical_interval`. With multiple 3.0
  # and typical 10 the threshold is 30. A sweep where the gap equals 30 must NOT
  # raise, and a sweep where the gap is 31 must raise. Pins the strict `>` so a
  # `>`→`>=` mutation (which would raise at exactly 30) fails.
  test "missing threshold is strict: gap == threshold does not raise, gap+1 does (example)" do
    cfg = %{missing_data: %{multiple: 3.0}, failing_targets: %{}}

    meta = %Meta{
      key: "series",
      status: :healthy,
      first_ts: 0,
      last_recv_ms: 0,
      last_source: nil,
      typical_interval_ms: 10
    }

    # now_recv == 30 => gap 30 == 30, not strictly greater => no raise.
    {meta_at, events_at} = sweep(meta, 30, cfg)
    assert events_at == []
    assert meta_at.open_missing == nil
    assert meta_at.status == :healthy

    # now_recv == 31 => gap 31 > 30 => raise.
    {meta_over, events_over} = sweep(meta, 31, cfg)

    started =
      Enum.find_value(events_over, fn
        {:anomaly_started, a} -> a
        _ -> nil
      end)

    assert started != nil, "a gap one past the threshold must raise"
    assert started.kind == :missing
    assert started.start_ms == 31
    assert meta_over.open_missing != nil
  end
end

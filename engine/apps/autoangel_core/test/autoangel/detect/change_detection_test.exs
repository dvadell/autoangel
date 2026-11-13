defmodule Autoangel.Detect.ChangeDetectionTest do
  @moduledoc """
  Tasks 35.1 to 35.6: level shift (Property 20), drift (Property 21), direction
  awareness (Property 18), run through the real kernel and `Decision`.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Autoangel.Detect.Decision
  alias Autoangel.Detect.Decision.Meta
  alias Autoangel.Kernel.Params
  alias Autoangel.Kernel.Scoring.Elixir, as: Kernel
  alias Autoangel.Kernel.{Sketch, State}
  alias Autoangel.Test.Pipeline

  @step 60_000

  defp series(levels) do
    levels
    |> Enum.with_index()
    |> Enum.map(fn {value, i} -> {i * @step, value} end)
  end

  defp steady(count, level, seed, amplitude \\ 0.5),
    do: Pipeline.noise(seed, count, amplitude) |> Enum.map(&(level + &1))

  # Feature: v1, Property 20: Level shift happens once and then becomes normal
  property "a sustained change is reported once, closes the open value Anomaly, and then scores normal" do
    check all(
            seed <- integer(1..10_000),
            delta <- integer(6..40),
            sign <- member_of([1, -1]),
            max_runs: 25
          ) do
      base = steady(300, 100.0, seed)
      shifted = steady(150, 100.0 + sign * delta, seed + 1)
      shift_ts = 300 * @step
      run = Pipeline.run(series(base ++ shifted))

      shifts = Pipeline.anomalies(run, :anomaly_started, :level_shift)
      assert [shift] = shifts
      assert shift.start_ms >= shift_ts - 25 * @step
      assert shift.start_ms <= shift_ts + 20 * @step

      # Nothing else starts after the level-shift Anomaly.
      shift_index =
        Enum.find_index(run.events, &match?({_, {:anomaly_started, %{kind: :level_shift}}}, &1))

      after_shift = Enum.drop(run.events, shift_index + 1)
      refute Enum.any?(after_shift, &match?({_, {:anomaly_started, _}}, &1))

      # Every Anomaly that started also ended, and none is open at the end.
      started = for {_, {:anomaly_started, a}} <- run.events, do: a.id
      ended = for {_, {:anomaly_resolved, a}} <- run.events, do: a.id
      assert Enum.sort(started) == Enum.sort(ended)
      assert run.meta.open_value_anomaly == nil

      # The new level is normal.
      tail = run.results |> Enum.take(-50) |> Enum.map(& &1.score)
      assert Enum.all?(tail, &(&1 < 75.0))
      assert run.meta.status == :healthy
    end
  end

  test "a value Anomaly that opened before the changepoint closes into the level shift" do
    # A longer minimum lets the value Anomaly (3 Samples) open before the shift is sure.
    run =
      Pipeline.run(series(steady(300, 100.0, 1) ++ steady(100, 130.0, 2)),
        params: %Params{level_shift_min_samples: 30}
      )

    events = for {_, {kind, %{kind: k}}} <- run.events, do: {kind, k}

    value_started = Enum.find_index(events, &(&1 == {:anomaly_started, :value}))
    value_ended = Enum.find_index(events, &(&1 == {:anomaly_resolved, :value}))
    shift_started = Enum.find_index(events, &(&1 == {:anomaly_started, :level_shift}))

    assert value_started < value_ended
    assert value_ended < shift_started
    assert Enum.count(events, &(&1 == {:anomaly_started, :level_shift})) == 1
  end

  test "a short burst of outliers is not a level shift" do
    burst = for i <- 0..3, do: {300 * @step + i * @step, 600.0}
    base = series(steady(300, 100.0, 5))
    tail = for {v, i} <- Enum.with_index(steady(100, 100.0, 6)), do: {(304 + i) * @step, v}
    run = Pipeline.run(base ++ burst ++ tail)

    assert Pipeline.anomalies(run, :anomaly_started, :level_shift) == []
    assert [_value] = Pipeline.anomalies(run, :anomaly_started, :value)
  end

  test "the minimum Samples setting delays the report" do
    levels = steady(300, 100.0, 7) ++ steady(120, 130.0, 8)
    fast = Pipeline.run(series(levels), params: %Params{level_shift_min_samples: 10})
    slow = Pipeline.run(series(levels), params: %Params{level_shift_min_samples: 40})

    [fast_shift] = Pipeline.anomalies(fast, :anomaly_started, :level_shift)
    [slow_shift] = Pipeline.anomalies(slow, :anomaly_started, :level_shift)

    fast_index =
      Enum.find_index(fast.events, &match?({_, {:anomaly_started, %{kind: :level_shift}}}, &1))

    slow_index =
      Enum.find_index(slow.events, &match?({_, {:anomaly_started, %{kind: :level_shift}}}, &1))

    assert elem(Enum.at(slow.events, slow_index), 0) > elem(Enum.at(fast.events, fast_index), 0)
    assert slow_shift.start_ms >= fast_shift.start_ms - 10 * @step
  end

  test "the minimum Samples and long-term window must be positive" do
    assert {:error, :invalid_params} =
             Kernel.score(State.encode(State.new()), [{0, 1.0, :gauge}], %Params{
               level_shift_min_samples: 0
             })

    assert {:error, :invalid_params} =
             Kernel.score(State.encode(State.new()), [{0, 1.0, :gauge}], %Params{longterm_window: 0})
  end

  test "a changed Baseline restarts the long-term reference from the post-change Samples" do
    run = Pipeline.run(series(steady(300, 100.0, 11) ++ steady(60, 150.0, 12)))
    {:ok, state} = State.decode(run.state)
    [%{centroids: centroids}] = state.longterm.blocks
    {median, _mad} = Sketch.median_mad(centroids)
    assert_in_delta median, 150.0, 1.0
  end

  describe "drift" do
    @drift_params %Params{level_shift_min_samples: 60_000, longterm_window: 1024}

    test "a steady series has no drift, and a slow climb does" do
      stable = series(steady(2_000, 100.0, 21))
      stable_run = Pipeline.run(stable, params: @drift_params)

      assert stable_run.results
             |> Enum.map(& &1.drift)
             |> Enum.reject(&is_nil/1)
             |> Enum.all?(&(&1 < 3.0))

      # Long-term reference is not ready before twice the rolling window.
      assert stable_run.results |> Enum.take(300) |> Enum.all?(&is_nil(&1.drift))

      climb = for i <- 0..1_499, do: 100.0 + i * 0.01 + Enum.at(steady(1, 0.0, 22 + i), 0)
      run = Pipeline.run(series(steady(2_000, 100.0, 21) ++ climb), params: @drift_params)
      drifts = run.results |> Enum.drop(2_000) |> Enum.map(& &1.drift) |> Enum.reject(&is_nil/1)
      assert Enum.max(drifts) > 3.0
    end

    # Feature: v1, Property 21: Drift state machine
    property "a drift Anomaly starts above the threshold outside learning, is unique, and ends at or below it" do
      step =
        one_of([
          constant(nil),
          float(min: 0.0, max: 2.9),
          constant(3.0),
          float(min: 3.1, max: 10.0)
        ])

      check all(drifts <- list_of(step, min_length: 1, max_length: 80)) do
        samples =
          for {drift, i} <- Enum.with_index(drifts) do
            %{
              ts: i * 1_000,
              score: 0.0,
              observed: 1.0,
              lower: 0.0,
              upper: 2.0,
              baseline_type: :robust,
              drift: drift,
              kind: :gauge,
              recv_ms: i * 1_000
            }
          end

        cfg = %{learning: %{duration: 5}, persistence: %{n: 3, m: 5}, drift: %{threshold: 3.0}}

        {_meta, started} =
          Enum.reduce(samples, {%Meta{key: {"d", []}}, []}, fn result, {meta, acc} ->
            {meta, events} = Decision.step(meta, result, result.ts, cfg)
            drift_events = for {k, %{kind: :drift}} <- events, do: {k, result.ts}
            {meta, acc ++ drift_events}
          end)

        # Reference: learning lasts until 5 s after the first Sample.
        {model, _} =
          Enum.reduce(Enum.with_index(drifts), {[], false}, fn {d, i}, {acc, open?} ->
            learning? = i * 1_000 - 0 < 5_000

            cond do
              not is_number(d) ->
                {acc, open?}

              d > 3.0 and not open? and not learning? ->
                {acc ++ [{:anomaly_started, i * 1_000}], true}

              d <= 3.0 and open? ->
                {acc ++ [{:anomaly_resolved, i * 1_000}], false}

              true ->
                {acc, open?}
            end
          end)

        assert started == model

        kinds = Enum.map(started, &elem(&1, 0))

        assert kinds ==
                 kinds
                 |> Enum.chunk_every(2)
                 |> Enum.flat_map(&Enum.take([:anomaly_started, :anomaly_resolved], length(&1)))
      end
    end

    test "drift is not reported while learning, and the status shows it when open" do
      cfg = %{learning: %{duration: 100_000}, drift: %{threshold: 3.0}}
      meta = %Meta{key: {"d", []}}

      result = fn ts, drift ->
        %{
          ts: ts,
          score: 0.0,
          observed: 1.0,
          lower: 0.0,
          upper: 2.0,
          baseline_type: :robust,
          drift: drift,
          kind: :gauge,
          recv_ms: ts
        }
      end

      {meta, events} = Decision.step(meta, result.(0, 9.0), 0, cfg)
      assert events == []
      assert meta.open_drift == nil

      cfg = %{cfg | learning: %{duration: 1}}
      {meta, _} = Decision.step(meta, result.(2_000, 9.0), 2_000, cfg)
      assert %{kind: :drift} = meta.open_drift
      assert meta.status == :anomalous
      {meta, events} = Decision.step(meta, result.(3_000, 1.0), 3_000, cfg)
      assert [{:anomaly_resolved, %{kind: :drift, end_ms: 3_000}}, {:status, :healthy}] = events
      assert meta.open_drift == nil
    end
  end

  describe "direction awareness" do
    defp scored(i, side) do
      {observed, score} =
        case side do
          :above -> {10.0, 95.0}
          :below -> {-10.0, 95.0}
          :inside -> {1.0, 10.0}
        end

      %{
        ts: i * 1_000,
        score: score,
        observed: observed,
        lower: 0.0,
        upper: 2.0,
        baseline_type: :robust,
        kind: :gauge,
        recv_ms: i * 1_000
      }
    end

    # Feature: v1, Property 18: Direction filtering
    property "only Samples on an anomalous side extend a run" do
      check all(
              direction <- member_of([:high, :low, :both]),
              sides <- list_of(member_of([:above, :below, :inside]), min_length: 1, max_length: 60)
            ) do
        cfg = %{learning: %{duration: 0}, persistence: %{n: 3, m: 5}}
        meta = %Meta{key: {"d", []}, direction_override: direction}

        {_meta, started} =
          sides
          |> Enum.with_index()
          |> Enum.reduce({meta, 0}, fn {side, i}, {meta, count} ->
            {meta, events} = Decision.step(meta, scored(i, side), i * 1_000, cfg)
            {meta, count + Enum.count(events, &match?({:anomaly_started, _}, &1))}
          end)

        assert started == model_starts(direction, sides)
      end
    end

    # Reference: a run of 3 anomalous Samples opens an Anomaly; 3 calm Samples close it.
    defp model_starts(direction, sides) do
      {_run, _calm, _open, starts} =
        Enum.reduce(sides, {0, 0, false, 0}, &model_step(&1, &2, direction))

      starts
    end

    defp model_step(side, state, direction) do
      if side_anomalous?(side, direction), do: model_anomalous(state), else: model_calm(state)
    end

    defp model_anomalous({_run, _calm, true, starts}), do: {0, 0, true, starts}

    defp model_anomalous({run, _calm, false, starts}) when run + 1 >= 3,
      do: {0, 0, true, starts + 1}

    defp model_anomalous({run, _calm, false, starts}), do: {run + 1, 0, false, starts}

    defp model_calm({_run, calm, true, starts}) when calm + 1 >= 3, do: {0, 0, false, starts}
    defp model_calm({_run, calm, true, starts}), do: {0, calm + 1, true, starts}
    defp model_calm({_run, _calm, false, starts}), do: {0, 0, false, starts}

    defp side_anomalous?(:above, direction), do: direction in [:high, :both]
    defp side_anomalous?(:below, direction), do: direction in [:low, :both]
    defp side_anomalous?(:inside, _direction), do: false

    test "a Series bound to a floor of zero is inferred :high; others stay :both" do
      cfg = %{learning: %{duration: 0}}
      floor_values = for i <- 1..40, do: if(rem(i, 3) == 0, do: 2.0, else: 0.0)
      other_values = for i <- 1..40, do: 50.0 + rem(i, 5)
      negative_values = for i <- 1..40, do: if(rem(i, 3) == 0, do: -1.0, else: 0.0)

      infer = fn values ->
        values
        |> Enum.with_index()
        |> Enum.reduce(%Meta{key: {"d", []}}, fn {v, i}, meta ->
          result = Map.put(scored(i, :inside), :observed, v)
          {meta, _} = Decision.step(meta, result, i * 1_000, cfg)
          meta
        end)
      end

      assert infer.(floor_values).direction == :high
      assert infer.(other_values).direction == :both
      assert infer.(negative_values).direction == :both
      # Not enough Samples yet: nothing inferred, and both sides count.
      assert infer.(Enum.take(floor_values, 10)).direction == nil
    end

    test "an inferred :high direction ignores dips; an override wins over the inference" do
      cfg = %{learning: %{duration: 0}, persistence: %{n: 3, m: 5}}
      floor = %Meta{key: {"d", []}, direction: :high}

      {_, events} =
        Enum.reduce(0..4, {floor, []}, fn i, {meta, acc} ->
          {meta, ev} = Decision.step(meta, scored(i, :below), i * 1_000, cfg)
          {meta, acc ++ ev}
        end)

      refute Enum.any?(events, &match?({:anomaly_started, _}, &1))

      overridden = %Meta{key: {"d", []}, direction: :high, direction_override: :low}

      {_, events} =
        Enum.reduce(0..4, {overridden, []}, fn i, {meta, acc} ->
          {meta, ev} = Decision.step(meta, scored(i, :below), i * 1_000, cfg)
          {meta, acc ++ ev}
        end)

      assert Enum.any?(events, &match?({:anomaly_started, _}, &1))
    end

    test "missing-data Anomalies are never filtered by direction" do
      cfg = %{learning: %{duration: 0}, missing_data: %{multiple: 3.0}}
      meta = %Meta{key: {"d", []}, direction_override: :high}

      {meta, _} =
        Decision.step(meta, Map.put(scored(0, :inside), :typical_interval_ms, 1_000), 0, cfg)

      {_meta, events} = Decision.step(meta, :missing_sweep, 60_000, cfg)
      assert Enum.any?(events, &match?({:anomaly_started, %{kind: :missing}}, &1))
    end
  end
end

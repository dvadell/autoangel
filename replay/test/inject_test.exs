defmodule AutoangelReplay.InjectTest do
  @moduledoc "Task 24.5: each injector's Sample changes and label (Requirements 77.2, 77.3)."
  use ExUnit.Case, async: true

  alias AutoangelReplay.{Inject, Series}

  @n 200
  @step 60_000

  defp series do
    Series.new("m", %{"host" => "a"}, for(i <- 0..(@n - 1), do: {i * @step, 10.0 + rem(i, 5)}))
  end

  # Injects `type` into the slot covering Samples 100..160 with a fixed rng.
  defp injected(type) do
    s = series()
    rng = :rand.seed_s(:exsss, 42)
    {:ok, out, label, _rng} = Inject.inject(s, type, rng, {100, 160})
    {s, out, label}
  end

  defp window(%Series{samples: samples}, label),
    do: Enum.filter(samples, fn {t, _} -> t >= label.start_ms and t <= label.end_ms end)

  defp outside(%Series{samples: samples}, label),
    do: Enum.reject(samples, fn {t, _} -> t >= label.start_ms and t <= label.end_ms end)

  test "labels name the Series, the type and the injected interval inside the slot" do
    for type <- Inject.types() do
      {s, _out, label} = injected(type)
      assert label.series_key == Series.key(s)
      assert label.type == type
      assert label.start_ms >= 100 * @step and label.end_ms < 160 * @step
      assert label.start_ms <= label.end_ms
    end
  end

  test "Samples outside the label interval are unchanged" do
    for type <- Inject.types() do
      {s, out, label} = injected(type)
      assert outside(out, label) == outside(s, label), "#{type}"
    end
  end

  test "spike raises and drop lowers every Sample in the interval by the same amount" do
    for {type, sign} <- [spike: 1, drop: -1] do
      {s, out, label} = injected(type)
      deltas = Enum.zip_with(window(out, label), window(s, label), fn {_, a}, {_, b} -> a - b end)
      assert length(deltas) in 3..5
      [d | _] = deltas
      assert sign * d > 4 * Inject.scale(s.samples) - 1.0e-9
      assert Enum.all?(deltas, &(abs(&1 - d) < 1.0e-9))
    end
  end

  test "a level shift moves the whole sustained interval by one constant amount" do
    {s, out, label} = injected(:level_shift)
    deltas = Enum.zip_with(window(out, label), window(s, label), fn {_, a}, {_, b} -> a - b end)
    assert length(deltas) >= 4
    assert Enum.all?(deltas, &(abs(&1 - hd(deltas)) < 1.0e-9))
    assert hd(deltas) > 0
  end

  test "a silence removes every Sample in the interval and nothing else" do
    {s, out, label} = injected(:silence)
    assert window(out, label) == []
    assert length(out.samples) == length(s.samples) - length(window(s, label))
    assert length(window(s, label)) >= 4
  end

  test "a drift ramps up steadily across the interval" do
    {s, out, label} = injected(:drift)
    deltas = Enum.zip_with(window(out, label), window(s, label), fn {_, a}, {_, b} -> a - b end)
    assert length(deltas) >= 4
    assert deltas == Enum.sort(deltas)
    steps = deltas |> Enum.chunk_every(2, 1, :discard) |> Enum.map(fn [a, b] -> b - a end)
    assert Enum.all?(steps, &(abs(&1 - hd(steps)) < 1.0e-9))
    # It really rises, starting at delta/width: first = last / count.
    assert hd(steps) > 1.0e-6
    assert_in_delta hd(deltas), List.last(deltas) / length(deltas), 1.0e-9
  end

  test "non-finite values are left untouched" do
    s =
      Series.new(
        "m",
        %{},
        for(i <- 0..(@n - 1), do: {i * @step, if(rem(i, 2) == 0, do: :nan, else: 1.0)})
      )

    {:ok, out, label, _} = Inject.inject(s, :level_shift, :rand.seed_s(:exsss, 1), {100, 160})
    assert Enum.any?(window(s, label), &(elem(&1, 1) == :nan))

    for {{t, v}, {t, o}} <- Enum.zip(window(s, label), window(out, label)),
        v == :nan,
        do: assert(o == :nan)
  end

  test "a slot too small for the type is skipped" do
    assert {:skip, _} = Inject.inject(series(), :level_shift, :rand.seed_s(:exsss, 1), {100, 104})
    assert {:skip, _} = Inject.inject(series(), :spike, :rand.seed_s(:exsss, 1), {100, 104})
    assert {:ok, _, _, _} = Inject.inject(series(), :spike, :rand.seed_s(:exsss, 1), {100, 106})
    short = Series.new("s", %{}, for(i <- 0..5, do: {i, 1.0}))
    assert {:skip, _} = Inject.inject(short, :spike, :rand.seed_s(:exsss, 1), {0, 5})
  end

  test "run/3 injects every type, deterministically per seed and differently across seeds" do
    assert {:ok, {_, a}} = Inject.run([series()], 1)
    assert {:ok, {_, ^a}} = Inject.run([series()], 1)
    assert {:ok, {_, b}} = Inject.run([series()], 2)
    refute a == b
    assert Enum.sort(Enum.map(a, & &1.type)) == Enum.sort(Inject.types())
  end

  test "injections in one Series never overlap and start after its first 40%" do
    assert {:ok, {_, labels}} = Inject.run([series()], 7, per_type: 3)
    assert Enum.all?(labels, &(&1.start_ms >= div(@n * 4, 10) * @step))
    sorted = Enum.sort_by(labels, & &1.start_ms)
    for [x, y] <- Enum.chunk_every(sorted, 2, 1, :discard), do: assert(x.end_ms < y.start_ms)
  end

  test "the scale is the robust spread with fallbacks" do
    assert Inject.scale([]) == 1.0
    assert Inject.scale([{1, 5.0}, {2, 5.0}]) == 0.5
    assert Inject.scale([{1, 0.0}, {2, 0.0}]) == 1.0

    assert_in_delta Inject.scale([{1, 1.0}, {2, 2.0}, {3, 3.0}, {4, 4.0}, {5, 100.0}]),
                    1.4826,
                    1.0e-9
  end
end

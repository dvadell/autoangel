defmodule Autoangel.SensitivityPropertyTest do
  @moduledoc """
  Property 22: sensitivity is monotone and follows precedence
  (Req 30.2, 31.1, 31.2, 31.3, 31.4).
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Autoangel.Detect.Decision
  alias Autoangel.{Feedback, SeriesRegistry, StateStore}

  @key {"latency", [{"host", "h1"}]}

  # One scored Sample against a healthy Series with persistence 1, so the
  # Sample alerts exactly when it is anomalous.
  defp anomalous?(score, threshold, global, override) do
    meta = %Decision.Meta{key: @key, status: :healthy, sensitivity_override: override}
    cfg = %{score_threshold: threshold, persistence: %{n: 1, m: 5}}
    cfg = if global, do: Map.put(cfg, :sensitivity, global), else: cfg
    sample = %{score: score, ts: 1, observed: 1.0, lower: 0.0, upper: 0.5}
    {_meta, events} = Decision.step(meta, sample, 1, cfg)
    Enum.any?(events, &match?({:anomaly_started, _}, &1))
  end

  # Independent reference: clamp(100 - (100 - T) * s, 1, 99.9).
  defp reference(score, threshold, s), do: score >= min(max(100 - (100 - threshold) * s, 1.0), 99.9)

  defp sensitivity, do: float(min: 0.05, max: 3.0)

  property "a Sample anomalous under s1 is anomalous under any s2 > s1" do
    check all(
            score <- float(min: 0.0, max: 100.0),
            threshold <- integer(1..99),
            s1 <- sensitivity(),
            delta <- float(min: 0.001, max: 2.0),
            via_override <- boolean(),
            max_runs: 400
          ) do
      s2 = s1 + delta
      {g1, o1, g2, o2} = if via_override, do: {nil, s1, nil, s2}, else: {s1, nil, s2, nil}

      if anomalous?(score, threshold, g1, o1), do: assert(anomalous?(score, threshold, g2, o2))
    end
  end

  property "effective sensitivity: per-Series override, else global, else default" do
    check all(
            score <- float(min: 0.0, max: 100.0),
            threshold <- integer(1..99),
            global <- one_of([constant(nil), sensitivity()]),
            override <- one_of([constant(nil), sensitivity()]),
            max_runs: 400
          ) do
      effective = override || global || 1.0

      assert anomalous?(score, threshold, global, override) ==
               reference(score, threshold, effective)
    end
  end

  property "'this was normal' never increases effective sensitivity and is idempotent" do
    check all(
            base <- sensitivity(),
            current <- one_of([constant(nil), sensitivity()]),
            max_runs: 150
          ) do
      dir = Path.join(System.tmp_dir!(), "aa-p22-#{System.unique_integer([:positive])}")
      on_exit(fn -> File.rm_rf(dir) end)
      {:ok, store} = StateStore.start_link(name: nil, path: dir)
      id = SeriesRegistry.series_id(@key)
      if current, do: :ok = StateStore.put_sensitivity(store, id, current)

      {:ok, fb} =
        Feedback.start_link(
          name: nil,
          state_store: store,
          base_sensitivity: base,
          lookup_series: fn _ -> {:ok, %{id: id, key: @key, worker: 0, kind: :gauge}} end,
          apply_sensitivity: fn _, _ -> :ok end
        )

      before = current || base
      target = %{series: %{name: "latency", labels: %{"host" => "h1"}}}
      {:ok, _} = Feedback.apply(fb, "this was normal", target)
      after_once = Map.fetch!(Feedback.overrides(fb), id)
      {:ok, _} = Feedback.apply(fb, "this was normal", target)

      assert after_once <= before
      assert after_once >= min(before, 0.25)
      assert Map.fetch!(Feedback.overrides(fb), id) == after_once

      GenServer.stop(fb)
      GenServer.stop(store)
      File.rm_rf!(dir)
    end
  end
end

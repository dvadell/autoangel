defmodule Autoangel.Detect.DecisionTest do
  use ExUnit.Case, async: true

  alias Autoangel.Detect.Decision
  alias Decision.Meta

  @cfg %{learning: %{duration: 1}, persistence: %{n: 3, m: 5}, missing_data: %{multiple: 3.0}}

  defp result(ts, score, extra \\ %{}),
    do:
      Map.merge(%{ts: ts, recv_ms: ts, score: score, observed: 10.0, lower: 0.0, upper: 1.0}, extra)

  test "starts an anomaly at the first persisted sample and resolves hysteretically" do
    {meta, []} = Decision.step(%Meta{}, result(0, 0.0), 0, @cfg)
    {meta, []} = Decision.step(meta, result(1, 90.0), 1, %{persistence: %{n: 3, m: 5}})
    {meta, []} = Decision.step(meta, result(2, 90.0), 2, %{persistence: %{n: 3, m: 5}})

    {meta, [{:anomaly_started, anomaly} | _]} =
      Decision.step(meta, result(3, 90.0), 3, %{persistence: %{n: 3, m: 5}})

    assert anomaly.start_ms == 1

    {meta, []} = Decision.step(meta, result(4, 0.0), 4, %{persistence: %{n: 3, m: 5}})
    {meta, []} = Decision.step(meta, result(5, 0.0), 5, %{persistence: %{n: 3, m: 5}})

    {_meta, [{:anomaly_resolved, resolved} | _]} =
      Decision.step(meta, result(6, 0.0), 6, %{persistence: %{n: 3, m: 5}})

    assert resolved.end_ms == 4
  end

  test "nil scores do not alter persistence" do
    {meta, []} = Decision.step(%Meta{}, result(0, 0.0), 0, @cfg)
    {meta, []} = Decision.step(meta, result(1, 90.0), 1, @cfg)
    {meta, []} = Decision.step(meta, result(2, nil), 2, @cfg)
    {meta, []} = Decision.step(meta, result(3, 90.0), 3, @cfg)
    {_meta, [{:anomaly_started, _} | _]} = Decision.step(meta, result(4, 90.0), 4, @cfg)
  end

  test "atom missing sweeps classify scrape failures without crashing" do
    meta = %Meta{last_recv_ms: 0, last_source: {:scrape, "target"}, typical_interval_ms: 10}

    {updated, [{:anomaly_started, anomaly} | _]} =
      Decision.step(meta, :missing_sweep, 31, %{failing_targets: %{"target" => true}})

    assert anomaly.classification == :target_down
    assert updated.status == :learning
  end

  test "learning ends and marks anomalies raised during learning" do
    {meta, events} = Decision.step(%Meta{}, result(0, 0.0), 0, @cfg)
    assert events == []

    {meta, _} =
      Decision.step(meta, result(1_000, 90.0), 1_000, %{
        learning: %{duration: 1},
        persistence: %{n: 1, m: 5}
      })

    assert meta.status == :anomalous
  end
end

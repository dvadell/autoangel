defmodule Autoangel.MetricsExtraTest do
  @moduledoc "The histogram maximum, bucket and observation readers used by the scale benchmark."
  use ExUnit.Case, async: false

  alias Autoangel.Metrics

  test "the maximum, the buckets and the count follow the observations" do
    before = Metrics.observations(:nif_call_duration_seconds)
    Metrics.observe(:nif_call_duration_seconds, 0.0004)
    Metrics.observe(:nif_call_duration_seconds, 0.003)
    Metrics.observe(:nif_call_duration_seconds, 0.0002)

    assert Metrics.observations(:nif_call_duration_seconds) == before + 3
    assert Metrics.max_observed(:nif_call_duration_seconds) >= 0.003
    buckets = Metrics.buckets(:nif_call_duration_seconds)
    assert Enum.map(buckets, &elem(&1, 0)) == [0.0001, 0.001, 0.005, 0.01, 0.1, 1.0]
    counts = Enum.map(buckets, &elem(&1, 1))
    assert counts == Enum.sort(counts)
  end

  test "an unknown histogram reads as empty" do
    assert Metrics.max_observed(:never_observed) == 0.0
    assert Metrics.buckets(:never_observed) == []
    assert Metrics.observations(:never_observed) == 0
  end
end

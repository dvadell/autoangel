defmodule Autoangel.SeriesKindTest do
  use ExUnit.Case, async: true

  alias Autoangel.SeriesKind

  test "explicit counter and gauge TYPE hints take precedence" do
    assert SeriesKind.infer("requests_total", :gauge) == :gauge
    assert SeriesKind.infer("temperature", :counter) == :counter
  end

  test "histogram children are counters and quantile summary children are gauges" do
    assert SeriesKind.infer("latency_bucket", :histogram) == :counter
    assert SeriesKind.infer("latency_count", :histogram) == :counter
    assert SeriesKind.infer("latency_sum", :histogram) == :counter
    assert SeriesKind.infer("latency", :summary, %{"quantile" => "0.99"}) == :gauge
    assert SeriesKind.infer("latency_count", :summary) == :counter
  end

  test "suffix inference is case-sensitive and applies without TYPE metadata" do
    assert SeriesKind.infer("requests_total") == :counter
    assert SeriesKind.infer("requests_count", :untyped) == :counter
    assert SeriesKind.infer("REQUESTS_TOTAL") == :gauge
    assert SeriesKind.infer("requestsTotal") == :gauge
  end

  test "parser preserves TYPE metadata and infers child kinds" do
    body =
      "# TYPE latency histogram\n" <>
        "latency_bucket{le=\"1\"} 2\n" <>
        "latency_sum 4\n" <>
        "latency_count 2\n"

    assert {:ok, [family]} = Autoangel.Exposition.parse(body)
    assert family.type == :histogram
    assert Enum.all?(family.samples, &(&1.type_hint == :histogram))
    assert Enum.all?(family.samples, &(&1.series_kind == :counter))
  end

  test "explicit gauge TYPE beats a counter suffix and untyped uses the suffix" do
    assert {:ok, [gauge]} =
             Autoangel.Exposition.parse("# TYPE requests_total gauge\nrequests_total 1\n")

    assert [%{type_hint: :gauge, series_kind: :gauge}] = gauge.samples

    assert {:ok, [counter]} =
             Autoangel.Exposition.parse("# TYPE requests_total untyped\nrequests_total 1\n")

    assert [%{type_hint: :untyped, series_kind: :counter}] = counter.samples
  end

  test "summary quantiles remain gauges while sum and count are counters" do
    body =
      "# TYPE latency summary\n" <>
        "latency{quantile=\"0.99\"} 2\n" <>
        "latency_sum 4\n" <>
        "latency_count 2\n"

    assert {:ok, [family]} = Autoangel.Exposition.parse(body)
    assert Enum.find(family.samples, &Map.has_key?(&1.labels, "quantile")).series_kind == :gauge

    assert Enum.all?(
             Enum.filter(family.samples, &(not Map.has_key?(&1.labels, "quantile"))),
             &(&1.series_kind == :counter)
           )
  end
end

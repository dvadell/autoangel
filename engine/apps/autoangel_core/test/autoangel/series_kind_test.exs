defmodule Autoangel.SeriesKindTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

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

  # Independent oracle: re-derives the expected Series_Kind directly from the rule
  # text in Property 7 without ever calling Autoangel.SeriesKind. This re-implements
  # the rule from scratch so the property is a genuine cross-check of infer/3.
  defp reference_kind(name, type_hint, labels) do
    case type_hint do
      :counter -> :counter
      :gauge -> :gauge
      :histogram -> reference_histogram_kind(name)
      :summary -> reference_summary_kind(name, labels)
      _ -> reference_suffix_kind(name)
    end
  end

  # Histogram children: _bucket/_count/_sum end-suffix -> counter, else gauge.
  defp reference_histogram_kind(name) do
    if reference_ends_with_any?(name, ["_bucket", "_count", "_sum"]),
      do: :counter,
      else: :gauge
  end

  # Summary children: a "quantile" label -> gauge; _count/_sum -> counter; anything
  # else -> gauge. The quantile label wins over any suffix.
  defp reference_summary_kind(name, labels) do
    cond do
      Map.has_key?(labels, "quantile") -> :gauge
      reference_ends_with_any?(name, ["_count", "_sum"]) -> :counter
      true -> :gauge
    end
  end

  # :untyped or nil: case-sensitive _total/_count/_sum/_bucket end-suffix -> counter,
  # anything else -> gauge.
  defp reference_suffix_kind(name) do
    if reference_ends_with_any?(name, ["_total", "_count", "_sum", "_bucket"]),
      do: :counter,
      else: :gauge
  end

  # Case-sensitive end-suffix test, implemented independently of String.ends_with?
  # intent by comparing the literal trailing bytes.
  defp reference_ends_with_any?(name, suffixes) do
    Enum.any?(suffixes, fn suffix ->
      suffix_size = byte_size(suffix)

      byte_size(name) >= suffix_size and
        binary_part(name, byte_size(name) - suffix_size, suffix_size) == suffix
    end)
  end

  # A random segment that will not itself end in a significant suffix, used to join
  # significant suffixes into the middle vs the end of a generated name.
  defp segment_generator do
    string([?a..?z, ?A..?Z, ?0..?9], min_length: 1, max_length: 8)
  end

  # Builds metric names that mix random segments with the significant suffixes and
  # their uppercase/camelCase variants, so the case-sensitivity rule (5.3) and the
  # "suffix in the middle must not count" rule are both exercised.
  defp name_generator do
    significant =
      member_of([
        "_total",
        "_count",
        "_sum",
        "_bucket",
        # Case variants that must NOT trigger the counter suffix rule.
        "_TOTAL",
        "_Count",
        "Total",
        "Sum",
        "Bucket",
        "_Total"
      ])

    gen all(
          base <- segment_generator(),
          parts <- list_of(one_of([segment_generator(), significant]), max_length: 4),
          # Decide whether a trailing significant suffix lands at the very end.
          trailing <- one_of([constant(""), significant])
        ) do
      ([base] ++ parts)
      |> Enum.join("_")
      |> Kernel.<>(trailing)
    end
  end

  defp type_hint_generator do
    member_of([:counter, :gauge, :histogram, :summary, :untyped, nil])
  end

  # Labels that may or may not carry a "quantile" key, since the quantile label drives
  # the summary-child branch (requirement 5.1/5.2).
  defp labels_generator do
    gen all(
          base <-
            map_of(
              string([?a..?z, ?A..?Z], min_length: 1, max_length: 6),
              string(:ascii, min_length: 0, max_length: 6),
              max_length: 4
            ),
          include_quantile <- boolean(),
          quantile_value <- member_of(["0.5", "0.9", "0.99", "+Inf"])
        ) do
      if include_quantile, do: Map.put(base, "quantile", quantile_value), else: base
    end
  end

  property "Feature: v1, Property 7: Series_Kind inference" do
    # **Validates: Requirements 1.9, 5.1, 5.2, 5.3**
    #
    # This pure property covers the inference rule itself: infer/3 as a function of a
    # metric name, a single TYPE hint, and labels. The override behaviour of a *later*
    # TYPE line against an already-existing Series (5.1/5.7) is a stateful registry
    # concern and is validated by the registry-backed override property in
    # Autoangel.SeriesRegistryTest, not here.
    check all(
            name <- name_generator(),
            type_hint <- type_hint_generator(),
            labels <- labels_generator(),
            max_runs: 200
          ) do
      # infer/3 agrees with the independent oracle for every generated case.
      assert SeriesKind.infer(name, type_hint, labels) ==
               reference_kind(name, type_hint, labels)

      # A significant suffix in the MIDDLE of a name must not trigger the counter rule,
      # while the same suffix at the END must. Both assertions depend on the generated
      # name, so they exercise the suffix rule across the whole generated space.
      assert SeriesKind.infer(name <> "_total_middle") == :gauge
      assert SeriesKind.infer(name <> "_total") == :counter
    end
  end
end

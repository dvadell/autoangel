defmodule Console.ViewsPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Console.{AnomalyList, Banner, Chart, SeriesSearch}

  @now ~U[2026-06-01 12:00:00Z]

  defp time_around(span),
    do: map(integer(-span..span), &DateTime.add(@now, &1, :second))

  defp anomaly do
    gen all(
          id <- string(:alphanumeric, length: 6),
          start <- time_around(5 * 86_400),
          open? <- boolean(),
          len <- integer(0..(3 * 86_400))
        ) do
      ended = if open?, do: nil, else: DateTime.add(start, len, :second)
      %{id: id, start_time: start, end_time: ended}
    end
  end

  # Feature: v1, Property 53: Anomaly list selection and order
  property "the list holds exactly the open and recently ended Anomalies, open first, newest first" do
    check all(list <- list_of(anomaly(), max_length: 40), recent <- integer(1..(2 * 86_400))) do
      shown = AnomalyList.select(list, @now, recent)
      cutoff = DateTime.add(@now, -recent, :second)

      expected =
        Enum.filter(list, fn a ->
          is_nil(a.end_time) or
            (DateTime.compare(a.end_time, cutoff) != :lt and
               DateTime.compare(a.end_time, @now) != :gt)
        end)

      assert Enum.sort(shown) == Enum.sort(expected)

      {open, ended} = Enum.split_while(shown, &is_nil(&1.end_time))
      assert Enum.all?(ended, & &1.end_time)

      for group <- [open, ended], [a, b] <- Enum.chunk_every(group, 2, 1, :discard) do
        assert DateTime.compare(a.start_time, b.start_time) != :lt
      end
    end
  end

  # Feature: v1, Property 54: Series search is case-sensitive substring matching
  property "a Series matches exactly when the term is inside its name, a label name or value" do
    alphabet = Enum.map(~c"abAB_1", &<<&1>>)
    word = map(list_of(member_of(alphabet), max_length: 6), &Enum.join/1)

    check all(
            name <- word,
            labels <- map_of(word, word, max_length: 3),
            term <- word
          ) do
      series = %{name: name, labels: labels}
      fields = [name | Enum.flat_map(labels, fn {k, v} -> [k, v] end)]
      assert SeriesSearch.matches?(series, term) == Enum.any?(fields, &String.contains?(&1, term))

      if term != "" and term != String.upcase(term) and String.upcase(term) not in fields and
           not Enum.any?(fields, &String.contains?(&1, String.upcase(term))) do
        refute SeriesSearch.matches?(series, String.upcase(term))
      end
    end
  end

  # Feature: v1, Property 55: Chart tier selection
  property "the chart uses the finest tier that holds the range and fits the point limit" do
    check all(
            range <- integer(60..(400 * 86_400)),
            age_extra <- integer(0..86_400),
            raw_count <- integer(0..20_000),
            limit <- integer(100..5_000),
            raw_retention <- integer(3_600..(30 * 86_400)),
            agg_extra <- integer(0..(400 * 86_400))
          ) do
      to = DateTime.add(@now, -age_extra, :second)
      from = DateTime.add(to, -range, :second)
      age = DateTime.diff(@now, from, :second)
      agg = raw_retention + agg_extra

      tier =
        Chart.tier(%{
          from: from,
          to: to,
          now: @now,
          raw_count: raw_count,
          raw_retention: raw_retention,
          aggregate_retention: agg,
          point_limit: limit
        })

      raw_ok = age <= raw_retention and raw_count <= limit
      minute_ok = age <= agg and div(range, 60) <= limit

      expected =
        cond do
          raw_ok -> :raw
          minute_ok -> :minute
          true -> :hour
        end

      assert tier == expected
    end
  end

  property "downsampling never exceeds the point limit" do
    check all(n <- integer(0..3_000), limit <- integer(1..500)) do
      points = for i <- 1..n//1, do: %{ts: DateTime.add(@now, i, :second), value: i * 1.0}
      assert length(Chart.downsample(points, limit)) <= max(limit, 0)
    end
  end

  property "counter rates are never negative, even across resets" do
    check all(values <- list_of(integer(0..1_000), max_length: 30)) do
      points =
        values
        |> Enum.with_index()
        |> Enum.map(fn {v, i} -> %{ts: DateTime.add(@now, i * 10, :second), value: v} end)

      assert Enum.all?(Chart.counter_rates(points), &(&1.value >= 0))
    end
  end

  # Feature: v1, Property 56: Engine-not-reporting banner predicate
  property "the banner shows iff no Heartbeat exists or it is older than the threshold" do
    check all(age <- integer(-10..1_000), threshold <- integer(1..600), none? <- boolean()) do
      latest = if none?, do: nil, else: DateTime.add(@now, -age, :second)
      expected = none? or age > threshold
      assert Banner.not_reporting?(latest, @now, threshold) == expected
    end
  end

  test "markers are chosen by interval overlap, open Anomalies end now" do
    from = DateTime.add(@now, -3_600, :second)
    a = %{start_time: DateTime.add(from, -100, :second), end_time: DateTime.add(from, 1, :second)}
    b = %{start_time: DateTime.add(from, -100, :second), end_time: DateTime.add(from, -1, :second)}
    c = %{start_time: DateTime.add(@now, -10, :second), end_time: nil}
    assert Chart.markers([a, b, c], from, @now, @now) == [a, c]
  end
end

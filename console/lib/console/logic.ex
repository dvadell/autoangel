defmodule Console.AnomalyList do
  @moduledoc """
  Which Anomalies the list shows, and in what order (Property 53).

  Shown: every open Anomaly, and every Anomaly whose end lies within the recent
  period (default 24 h) before `now`. Order: open first, then ended; inside each
  group the newest start first, ties broken by id. The database query
  (`AutoangelSchema.Query.anomalies/1`) applies the same rule; this function is
  the reference the property test checks it against.
  """

  @spec select([map()], DateTime.t(), pos_integer()) :: [map()]
  def select(anomalies, now, recent_seconds) do
    cutoff = DateTime.add(now, -recent_seconds, :second)

    anomalies
    |> Enum.filter(&shown?(&1, cutoff, now))
    |> Enum.sort_by(&sort_key/1, &key_order/2)
  end

  defp shown?(%{end_time: nil}, _cutoff, _now), do: true

  defp shown?(%{end_time: ended}, cutoff, now),
    do: DateTime.compare(ended, cutoff) != :lt and DateTime.compare(ended, now) != :gt

  defp sort_key(a),
    do: {if(is_nil(a.end_time), do: 0, else: 1), DateTime.to_unix(a.start_time, :microsecond), a.id}

  # Group ascending, start descending, id ascending.
  defp key_order({g1, s1, i1}, {g2, s2, i2}) do
    cond do
      g1 != g2 -> g1 < g2
      s1 != s2 -> s1 > s2
      true -> i1 <= i2
    end
  end
end

defmodule Console.SeriesSearch do
  @moduledoc """
  Series search (Property 54): a case-sensitive substring match over the name,
  every label name and every label value. An empty term matches everything.
  The SQL form (`strpos`) lives in `AutoangelSchema.Query.series/2`.
  """

  @spec matches?(%{name: String.t(), labels: map()}, String.t()) :: boolean()
  def matches?(_series, ""), do: true

  def matches?(%{name: name, labels: labels}, term) do
    String.contains?(name, term) or
      Enum.any?(labels || %{}, fn {k, v} ->
        String.contains?(to_string(k), term) or String.contains?(to_string(v), term)
      end)
  end
end

defmodule Console.Chart do
  @moduledoc """
  Chart data selection.

  `tier/2` (Property 55) picks the finest storage tier that both still holds the
  start of the range and fits the chart point limit:

    * `:raw` when the range start is inside raw retention and the raw sample
      count is at most the point limit;
    * `:minute` when the start is inside aggregate retention and the range has
      at most `point_limit` minutes;
    * `:hour` otherwise (downsampled to the point limit by `downsample/2`).
  """

  @type tier :: :raw | :minute | :hour

  @ranges [
    {"1h", 3_600},
    {"6h", 6 * 3_600},
    {"24h", 86_400},
    {"7d", 7 * 86_400},
    {"30d", 30 * 86_400},
    {"90d", 90 * 86_400},
    {"13mo", 395 * 86_400}
  ]

  @doc "Range choices for the selector, from 1 hour to 13 months, as `{label, seconds}`."
  @spec ranges() :: [{String.t(), pos_integer()}]
  def ranges, do: @ranges

  @doc "Seconds for a range label; unknown labels fall back to 24 h."
  @spec range_seconds(String.t() | nil) :: pos_integer()
  def range_seconds(label) do
    case List.keyfind(@ranges, label, 0) do
      {_, seconds} -> seconds
      nil -> 86_400
    end
  end

  @type tier_input :: %{
          from: DateTime.t(),
          to: DateTime.t(),
          now: DateTime.t(),
          raw_count: non_neg_integer(),
          raw_retention: pos_integer(),
          aggregate_retention: pos_integer(),
          point_limit: pos_integer()
        }

  @spec tier(tier_input()) :: tier()
  def tier(%{} = i) do
    age = DateTime.diff(i.now, i.from, :second)
    minutes = div(DateTime.diff(i.to, i.from, :second), 60)

    cond do
      age <= i.raw_retention and i.raw_count <= i.point_limit -> :raw
      age <= i.aggregate_retention and minutes <= i.point_limit -> :minute
      true -> :hour
    end
  end

  @doc """
  Per-second rate of a counter. A drop between two points is a reset: the rate
  over that step is the new value divided by the step, never negative.
  """
  @spec counter_rates([%{ts: DateTime.t(), value: number()}]) :: [
          %{ts: DateTime.t(), value: float()}
        ]
  def counter_rates(points) do
    points
    |> Enum.chunk_every(2, 1, :discard)
    |> Enum.flat_map(fn [a, b] ->
      dt = DateTime.diff(b.ts, a.ts, :microsecond) / 1_000_000

      cond do
        dt <= 0 -> []
        b.value >= a.value -> [%{ts: b.ts, value: (b.value - a.value) / dt}]
        true -> [%{ts: b.ts, value: b.value / dt}]
      end
    end)
  end

  @doc "Averages consecutive points so at most `limit` remain."
  @spec downsample([%{ts: DateTime.t(), value: number()}], pos_integer()) :: [map()]
  def downsample(points, limit) when length(points) <= limit, do: points

  def downsample(points, limit) do
    size = ceil(length(points) / limit)

    points
    |> Enum.chunk_every(size)
    |> Enum.map(fn chunk ->
      values = for %{value: v} <- chunk, is_number(v), do: v
      value = if values == [], do: nil, else: Enum.sum(values) / length(values)
      %{ts: hd(chunk).ts, value: value}
    end)
  end

  @doc "Anomalies whose interval (open ones end at `now`) overlaps `[from, to]`."
  @spec markers([map()], DateTime.t(), DateTime.t(), DateTime.t()) :: [map()]
  def markers(anomalies, from, to, now) do
    Enum.filter(anomalies, fn a ->
      ended = a.end_time || now
      DateTime.compare(a.start_time, to) != :gt and DateTime.compare(ended, from) != :lt
    end)
  end
end

defmodule Console.Banner do
  @moduledoc """
  The Engine-not-reporting predicate (Property 56): the banner shows when no
  Heartbeat is recorded, or the latest one is older than the threshold.
  """

  @spec not_reporting?(DateTime.t() | nil, DateTime.t(), pos_integer()) :: boolean()
  def not_reporting?(nil, _now, _threshold), do: true
  def not_reporting?(latest, now, threshold), do: DateTime.diff(now, latest, :second) > threshold
end

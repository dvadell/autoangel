defmodule AutoangelReplay.Report do
  @moduledoc """
  Matching rule and report (Requirement 77.4, design "Replay_Harness").

  An alert matches a label when the alert's Series equals the label's Series and
  the alert's Anomaly start lies in `[label.start, label.end + tolerance]`, with

      tolerance = max(M, N × typical_interval) + (2 × sweep_interval for silence)

  where `M` is the persistence duration, `N` the persistence count and
  `typical_interval` the median spacing of the label's Series.

  * recall    = labels matched by at least one alert / labels
  * precision = alerts matching at least one label / alerts
  * alerts per simulated day = alerts / (span of replayed timestamps in days)

  Per type, recall uses that type's labels; precision and alerts per day count
  the alerts whose matched label has that type (an alert matching labels of two
  types counts for both), over all alerts and over the span respectively. An
  undefined ratio (no labels, no alerts or zero span) is `nil`.
  """

  @day_ms 86_400_000

  @type alert :: %{series_key: term(), start_ms: integer()}
  @type label :: %{series_key: term(), type: atom(), start_ms: integer(), end_ms: integer()}
  @type metrics :: %{
          recall: float() | nil,
          precision: float() | nil,
          alerts_per_day: float() | nil
        }
  @type params :: %{
          persistence_ms: non_neg_integer(),
          persistence_n: non_neg_integer(),
          sweep_ms: non_neg_integer(),
          typical_interval_ms: %{optional(term()) => non_neg_integer()},
          span_ms: non_neg_integer()
        }

  @spec tolerance(label(), params()) :: non_neg_integer()
  def tolerance(label, params) do
    interval = Map.get(params.typical_interval_ms, label.series_key, 0)
    base = max(params.persistence_ms, params.persistence_n * interval)
    if label.type == :silence, do: base + 2 * params.sweep_ms, else: base
  end

  @spec matches?(alert(), label(), params()) :: boolean()
  def matches?(alert, label, params) do
    alert.series_key == label.series_key and alert.start_ms >= label.start_ms and
      alert.start_ms <= label.end_ms + tolerance(label, params)
  end

  @spec compute([label()], [alert()], params()) :: %{overall: metrics(), per_type: map()}
  def compute(labels, alerts, params) do
    matched_types =
      Enum.map(alerts, fn a ->
        labels |> Enum.filter(&matches?(a, &1, params)) |> Enum.map(& &1.type) |> Enum.uniq()
      end)

    recalled = Enum.filter(labels, fn l -> Enum.any?(alerts, &matches?(&1, l, params)) end)
    days = if params.span_ms > 0, do: params.span_ms / @day_ms

    overall = %{
      recall: ratio(length(recalled), length(labels)),
      precision: ratio(Enum.count(matched_types, &(&1 != [])), length(alerts)),
      alerts_per_day: if(days, do: length(alerts) / days)
    }

    per_type =
      labels
      |> Enum.map(& &1.type)
      |> Enum.uniq()
      |> Map.new(fn type ->
        of_type = Enum.count(matched_types, &(type in &1))

        {type,
         %{
           recall:
             ratio(Enum.count(recalled, &(&1.type == type)), Enum.count(labels, &(&1.type == type))),
           precision: ratio(of_type, length(alerts)),
           alerts_per_day: if(days, do: of_type / days)
         }}
      end)

    %{overall: overall, per_type: per_type}
  end

  defp ratio(_num, 0), do: nil
  defp ratio(num, den), do: num / den

  @doc "Median spacing of a Series' timestamps (0 when fewer than two Samples)."
  @spec typical_interval([integer()]) :: non_neg_integer()
  def typical_interval(timestamps) do
    gaps =
      timestamps
      |> Enum.sort()
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.map(fn [a, b] -> b - a end)
      |> Enum.sort()

    case gaps do
      [] -> 0
      _ -> Enum.at(gaps, div(length(gaps), 2))
    end
  end
end

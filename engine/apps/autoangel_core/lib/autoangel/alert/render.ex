defmodule Autoangel.Alert.Render do
  @moduledoc """
  Builds and formats alert content (Requirements 17.4, 17.5, 17.7, 19.4, 19.5,
  19.7, 26.1-26.5).

  `build/3` produces the full `%Autoangel.Alert{}` once; Plugins format that same
  struct through `text/1`. Wording is neutral: correlated Series are labelled
  "candidates", weak ones carry "(weak evidence)", the score is labelled
  "anomaly score" and printed without a percent sign, and nothing is described as
  a cause.
  """

  alias Autoangel.Alert

  @default_max_candidates 5

  @doc """
  Builds the alert for an Anomaly start.

  Options: `:console_base_url` (link is added only when set) and
  `:max_candidates` (default #{@default_max_candidates}). `candidates` is the
  Correlator's list, ordered or not; the lowest ranks are kept in ascending order.
  """
  @spec build(map(), [map()], keyword()) :: Alert.t()
  def build(anomaly, candidates, opts \\ []) when is_map(anomaly) and is_list(candidates) do
    max = positive(Keyword.get(opts, :max_candidates), @default_max_candidates)
    series_key = Map.get(anomaly, :series_key)
    start_ms = Map.get(anomaly, :start_ms)
    id = Map.get(anomaly, :id)

    %Alert{
      name: Map.get(anomaly, :name) || metric_name(series_key),
      labels: labels(anomaly, series_key),
      observed: Map.get(anomaly, :observed),
      range: %{lower: Map.get(anomaly, :lower), upper: Map.get(anomaly, :upper)},
      baseline_type: Map.get(anomaly, :baseline_type),
      score: Map.get(anomaly, :score),
      start_time: start_ms,
      start_time_iso: iso(start_ms),
      anomaly_id: id,
      anomaly: anomaly,
      candidates: candidates |> Enum.map(&candidate/1) |> top(max),
      candidate_count: length(candidates),
      console_url: console_url(Keyword.get(opts, :console_base_url), id)
    }
  end

  @doc "Formats an alert (or a self-monitoring alert) as neutral plain text."
  @spec text(map()) :: String.t()
  def text(%{message: message}) when is_binary(message), do: message

  def text(alert) when is_map(alert) do
    lines =
      alert
      |> header_fields()
      |> Enum.reject(fn {_name, value} -> is_nil(value) end)
      |> Enum.map(fn {name, value} -> "#{name}: #{value}" end)

    Enum.join(lines ++ candidate_lines(field(alert, :candidates) || []), "\n")
  end

  defp header_fields(alert) do
    range = field(alert, :range) || %{}

    [
      {"metric", field(alert, :name) || field(alert, :metric_name)},
      {"labels", format_labels(field(alert, :labels) || %{})},
      {"observed value", present(number(field(alert, :observed)))},
      {"expected range lower", present(number(field(range, :lower)))},
      {"expected range upper", present(number(field(range, :upper)))},
      {"baseline type", present(number(field(alert, :baseline_type)))},
      {"anomaly score", present(score(field(alert, :score)))},
      {"start", field(alert, :start_time_iso) || iso(field(alert, :start_time))},
      {"console", field(alert, :console_url)}
    ]
  end

  defp present(nil), do: "none"
  defp present(value), do: value

  @doc "Formats an Anomaly_Score as an integer or one decimal, without a percent sign."
  @spec score(term()) :: String.t() | nil
  def score(nil), do: nil

  def score(value) when is_number(value) do
    clamped = value |> max(0) |> min(100)
    rounded = Float.round(clamped * 1.0, 1)

    if rounded == Float.round(rounded, 0),
      do: Integer.to_string(trunc(rounded)),
      else: Float.to_string(rounded)
  end

  def score(_), do: nil

  defp candidate_lines([]), do: ["no candidates found inside the Correlation_Window"]

  defp candidate_lines(candidates) do
    ["candidates:" | Enum.map(candidates, &candidate_line/1)]
  end

  defp candidate_line(candidate) do
    c = candidate(candidate)
    labels = if c.labels == %{}, do: "", else: " [#{format_labels(c.labels)}]"
    weak = if c.weak?, do: " (weak evidence)", else: ""
    "#{c.rank || "?"}. #{c.name}#{labels}#{weak}"
  end

  defp candidate(c) when is_map(c) do
    key = field(c, :series_key)

    %{
      rank: field(c, :rank),
      name: field(c, :name) || field(c, :metric_name) || metric_name(key) || "?",
      labels: field(c, :labels) || labels(%{}, key),
      weak?: field(c, :weak?) == true or field(c, :weak_evidence) == true
    }
  end

  defp candidate(_), do: %{rank: nil, name: "?", labels: %{}, weak?: false}

  defp top(candidates, max) do
    candidates
    |> Enum.sort_by(&(&1.rank || :infinity))
    |> Enum.take(max)
  end

  defp console_url(base, id) when is_binary(base) and is_binary(id) and base != "",
    do: String.trim(base) |> String.trim_trailing("/") |> Kernel.<>("/anomalies/" <> id)

  defp console_url(_base, _id), do: nil

  defp iso(ms) when is_integer(ms) do
    case DateTime.from_unix(ms, :millisecond) do
      {:ok, time} -> time |> DateTime.truncate(:second) |> DateTime.to_iso8601()
      {:error, _} -> nil
    end
  end

  defp iso(_), do: nil

  defp metric_name({name, _labels}) when is_binary(name), do: name
  defp metric_name(name) when is_binary(name), do: name
  defp metric_name(_), do: nil

  defp labels(anomaly, key) do
    case Map.get(anomaly, :labels) do
      labels when is_map(labels) -> labels
      _ -> labels_from_key(key)
    end
  end

  defp labels_from_key({_name, labels}) when is_list(labels), do: Map.new(labels)
  defp labels_from_key({_name, labels}) when is_map(labels), do: labels
  defp labels_from_key(_), do: %{}

  defp format_labels(labels) when is_map(labels) do
    labels |> Enum.sort() |> Enum.map_join(", ", fn {k, v} -> "#{k}=#{v}" end)
  end

  defp format_labels(_), do: ""

  defp number(nil), do: nil
  defp number(value), do: to_string(value)

  defp field(map, key) when is_map(map), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))
  defp field(_map, _key), do: nil

  defp positive(value, _default) when is_integer(value) and value > 0, do: value
  defp positive(_value, default), do: default
end

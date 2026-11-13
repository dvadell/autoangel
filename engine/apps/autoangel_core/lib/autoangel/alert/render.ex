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

  @doc """
  Builds the resolution notification of a closed Incident (Requirements 29.1 to 29.4):
  a resolved indication, every member Series, the start and end times, and the
  Console link when a base URL is configured.
  """
  @spec resolution(map(), keyword()) :: Alert.t()
  def resolution(incident, opts \\ []) when is_map(incident) do
    anomalies =
      incident
      |> Map.get(:anomalies, %{})
      |> Map.values()
      |> Enum.sort_by(&Map.get(&1, :start_ms, 0))

    first = List.first(anomalies) || %{}
    ended = incident |> Map.get(:closed_at) |> end_time(anomalies)

    %Alert{
      name: metric_name(incident.series_key),
      labels: labels_from_key(incident.series_key),
      resolved?: true,
      incident_id: incident_number(Map.get(incident, :id)),
      members: Enum.map(Map.get(incident, :series_keys, [incident.series_key]), &member/1),
      start_time: Map.get(incident, :opened_at),
      start_time_iso: iso(Map.get(incident, :opened_at)),
      end_time: ended,
      end_time_iso: iso(ended),
      anomaly_id: Map.get(first, :id),
      console_url: console_url(Keyword.get(opts, :console_base_url), Map.get(first, :id))
    }
  end

  defp incident_number({:incident, n}), do: n
  defp incident_number(other), do: other

  defp member(key), do: %{name: metric_name(key), labels: labels_from_key(key)}

  defp end_time(closed_at, _anomalies) when is_integer(closed_at), do: closed_at

  defp end_time(_closed_at, anomalies),
    do:
      anomalies
      |> Enum.map(&Map.get(&1, :end_ms))
      |> Enum.filter(&is_integer/1)
      |> Enum.max(fn -> nil end)

  @doc "Formats an alert (or a self-monitoring alert) as neutral plain text."
  @spec text(map()) :: String.t()
  def text(%{message: message}) when is_binary(message), do: message

  def text(%{resolved?: true} = alert) do
    members = field(alert, :members) || []

    lines =
      [
        {"status", "resolved"},
        {"metric", field(alert, :name)},
        {"labels", format_labels(field(alert, :labels) || %{})},
        {"start", field(alert, :start_time_iso)},
        {"end", field(alert, :end_time_iso)},
        {"console", field(alert, :console_url)}
      ]
      |> Enum.reject(fn {_name, value} -> is_nil(value) or value == "" end)
      |> Enum.map(fn {name, value} -> "#{name}: #{value}" end)

    Enum.join(lines ++ member_lines(members), "\n")
  end

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

  defp member_lines(members) do
    ["members:" | Enum.map(members, fn m -> "- #{m.name}#{member_labels(m.labels)}" end)]
  end

  defp member_labels(labels) when labels == %{}, do: ""
  defp member_labels(labels), do: " [#{format_labels(labels)}]"

  defp candidate_lines([]), do: ["no candidates found inside the Correlation_Window"]

  defp candidate_lines(candidates) do
    ["candidates:" | Enum.map(candidates, &candidate_line/1)]
  end

  defp candidate_line(candidate) do
    c = candidate(candidate)
    labels = if c.labels == %{}, do: "", else: " [#{format_labels(c.labels)}]"
    weak = if c.weak?, do: " (weak evidence)", else: ""
    window = if c.event_like?, do: " (co-occurrence inside the window)", else: ""
    "#{c.rank || "?"}. #{c.name}#{labels}#{weak}#{window}"
  end

  defp candidate(c) when is_map(c) do
    key = field(c, :series_key)

    %{
      rank: field(c, :rank),
      name: field(c, :name) || field(c, :metric_name) || metric_name(key) || "?",
      labels: field(c, :labels) || labels(%{}, key),
      weak?: field(c, :weak?) == true or field(c, :weak_evidence) == true,
      event_like?: field(c, :event_like?) == true or event_like?(field(c, :evidence))
    }
  end

  defp candidate(_), do: %{rank: nil, name: "?", labels: %{}, weak?: false, event_like?: false}

  # Event-like Series are not described by lag or order, only by co-occurrence.
  defp event_like?(evidence) when is_map(evidence), do: field(evidence, :event_like?) == true
  defp event_like?(_evidence), do: false

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

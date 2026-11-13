defmodule Autoangel.HTTP.Listings do
  @moduledoc """
  The JSON shapes of `GET /api/v1/incidents` and `GET /api/v1/series`
  (Requirements 46.1 to 46.5). Both are read-only views of what the Engine holds
  in memory, sorted so the answer is the same for the same state; an empty state
  gives an empty list.
  """

  @doc "Incidents sorted by number: `id`, `open`, `muted`, `opened_at`, `closed_at` and the member Series."
  @spec incidents(map()) :: [map()]
  def incidents(incidents) when is_map(incidents) do
    incidents
    |> Enum.sort_by(fn {id, _incident} -> id_number(id) end)
    |> Enum.map(fn {id, incident} ->
      %{
        "id" => id_number(id),
        "open" => Map.get(incident, :open?, false) == true,
        "muted" => Map.get(incident, :muted?, false) == true,
        "opened_at" => Map.get(incident, :opened_at),
        "closed_at" => Map.get(incident, :closed_at),
        "series" => incident |> series_keys() |> Enum.map(&series_ref/1)
      }
    end)
  end

  def incidents(_other), do: []

  @doc "Series sorted by name and labels: `name`, `labels`, `kind` and `status`."
  @spec series([map()]) :: [map()]
  def series(entries) when is_list(entries) do
    entries
    |> Enum.map(fn entry ->
      %{
        "name" => series_name(entry.key),
        "labels" => series_labels(entry.key),
        "kind" => to_string(entry.kind),
        "status" => to_string(entry.status)
      }
    end)
    |> Enum.sort_by(&{&1["name"], Enum.sort(&1["labels"])})
  end

  def series(_other), do: []

  defp id_number({:incident, n}) when is_integer(n), do: n
  defp id_number(n) when is_integer(n), do: n
  defp id_number(_id), do: 0

  defp series_keys(incident),
    do: Map.get(incident, :series_keys) || List.wrap(Map.get(incident, :series_key))

  defp series_ref(key), do: %{"name" => series_name(key), "labels" => series_labels(key)}

  defp series_name({name, _labels}), do: to_string(name)
  defp series_name(name), do: to_string(name)

  defp series_labels({_name, labels}) when is_list(labels) or is_map(labels),
    do: Map.new(labels, fn {k, v} -> {to_string(k), to_string(v)} end)

  defp series_labels(_key), do: %{}
end

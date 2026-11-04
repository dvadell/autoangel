defmodule Autoangel.SeriesKind do
  @moduledoc "Infers the counter or gauge kind of an exposition series."

  @type kind :: :counter | :gauge
  @type type_hint :: :counter | :gauge | :histogram | :summary | :untyped | nil

  @counter_suffixes ["_total", "_count", "_sum", "_bucket"]

  @doc "Infers a Series_Kind from a metric name, TYPE hint, and labels."
  @spec infer(String.t(), type_hint(), map()) :: kind()
  def infer(name, type_hint \\ nil, labels \\ %{}) when is_binary(name) and is_map(labels) do
    case type_hint do
      :counter -> :counter
      :gauge -> :gauge
      :histogram -> child_kind(name, labels, :histogram)
      :summary -> child_kind(name, labels, :summary)
      _ -> suffix_kind(name)
    end
  end

  @doc "Infers the kind for an exposition sample struct or map."
  @spec infer_sample(%{name: String.t(), type_hint: type_hint(), labels: map()}) :: kind()
  def infer_sample(%{name: name, type_hint: type_hint, labels: labels}) do
    infer(name, type_hint, labels)
  end

  defp child_kind(name, labels, type) do
    cond do
      type == :summary and Map.has_key?(labels, "quantile") -> :gauge
      type == :histogram and String.ends_with?(name, "_bucket") -> :counter
      String.ends_with?(name, "_count") -> :counter
      String.ends_with?(name, "_sum") -> :counter
      true -> :gauge
    end
  end

  defp suffix_kind(name) do
    if Enum.any?(@counter_suffixes, &String.ends_with?(name, &1)), do: :counter, else: :gauge
  end
end

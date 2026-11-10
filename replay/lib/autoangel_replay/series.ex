defmodule AutoangelReplay.Series do
  @moduledoc """
  A recorded Series: metric name, labels and Samples `{timestamp_ms, value}` in
  ascending timestamp order. `key/1` gives the canonical identity used in
  ground-truth labels: `{name, sorted [{label, value}]}`.
  """

  @enforce_keys [:name, :labels, :samples]
  defstruct [:name, :labels, :samples]

  @type sample :: {integer(), float() | :nan | :inf | :neg_inf}
  @type t :: %__MODULE__{name: String.t(), labels: %{String.t() => String.t()}, samples: [sample()]}
  @type key :: {String.t(), [{String.t(), String.t()}]}

  @spec new(String.t(), map(), [sample()]) :: t()
  def new(name, labels, samples),
    do: %__MODULE__{name: name, labels: labels, samples: Enum.sort_by(samples, &elem(&1, 0))}

  @spec key(t()) :: key()
  def key(%__MODULE__{name: name, labels: labels}), do: {name, Enum.sort(labels)}

  @doc "A Prometheus-style display name, e.g. `cpu{host=\"a\"}`."
  @spec display(t()) :: String.t()
  def display(%__MODULE__{name: name, labels: labels}) when map_size(labels) == 0, do: name

  def display(%__MODULE__{name: name, labels: labels}) do
    inner = labels |> Enum.sort() |> Enum.map_join(",", fn {k, v} -> ~s(#{k}="#{escape(v)}") end)
    "#{name}{#{inner}}"
  end

  # Label values are escaped as in the Prometheus text exposition format.
  defp escape(value) do
    value
    |> String.replace("\\", "\\\\")
    |> String.replace("\"", "\\\"")
    |> String.replace("\n", "\\n")
  end
end

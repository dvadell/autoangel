defmodule Console.MCP.Untrusted do
  @moduledoc """
  Marks values that came from monitored systems (Series names, label names and
  values, candidate evidence) so an AI client can tell data from instructions
  (Requirements 63.4 to 63.6).

  A marked value is `<untrusted>` + the value + `</untrusted>`. The value is cut
  to 256 characters first, then `&`, `<` and `>` are escaped, so the text between
  the tags can never contain a tag of its own and cannot close the marking early.
  """

  @max_chars 256
  @open "<untrusted>"
  @close "</untrusted>"

  @doc "The longest value kept, in characters."
  @spec max_chars() :: 256
  def max_chars, do: @max_chars

  @doc "Marks one string (other terms are converted with `to_string/1`)."
  @spec mark(term()) :: String.t()
  def mark(value) do
    @open <> (value |> to_string() |> String.slice(0, @max_chars) |> escape()) <> @close
  end

  @doc "Series labels as a list of marked `name`/`value` pairs, sorted by name."
  @spec labels(map() | nil) :: [%{String.t() => String.t()}]
  def labels(nil), do: []

  def labels(labels) do
    labels
    |> Enum.sort_by(fn {k, _v} -> to_string(k) end)
    |> Enum.map(fn {k, v} -> %{"name" => mark(k), "value" => mark(v)} end)
  end

  @doc "A Series as `%{name: marked, labels: marked pairs}`."
  @spec series(%{name: term(), labels: map() | nil} | nil) :: map() | nil
  def series(nil), do: nil
  def series(s), do: %{"name" => mark(s.name), "labels" => labels(s.labels)}

  @doc "Marks every string inside a nested term (map keys and values, lists)."
  @spec deep(term()) :: term()
  def deep(value) when is_binary(value), do: mark(value)
  def deep(value) when is_list(value), do: Enum.map(value, &deep/1)

  def deep(value) when is_map(value) and not is_struct(value),
    do: Map.new(value, fn {k, v} -> {mark(k), deep(v)} end)

  def deep(value) when is_atom(value) and not is_boolean(value) and not is_nil(value),
    do: mark(value)

  def deep(value), do: value

  defp escape(text) do
    text
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
  end
end

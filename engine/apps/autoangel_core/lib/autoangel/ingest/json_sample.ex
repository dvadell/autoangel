defmodule Autoangel.Ingest.JsonSample do
  @moduledoc """
  Validates the body of `POST /api/v1/push/json` (Requirements 3.1 to 3.5): one
  JSON object with

    * `name` (required): a metric name, `[a-zA-Z_:][a-zA-Z0-9_:]*`
    * `value` (required): a JSON number
    * `timestamp` (optional): an integer, Unix time in milliseconds
    * `labels` (optional): an object whose names are label names and whose values are strings

  Errors are `:not_a_json_object` (the body is not one JSON object), 
  `{:missing_field, field}` (`name` or `value` is absent or null) and
  `{:invalid_field, field}` (the field is present but wrong). `name` and `value`
  are checked before the optional fields; the first problem found is reported.
  """

  alias Autoangel.Exposition.Sample

  @metric_name ~r/\A[a-zA-Z_:][a-zA-Z0-9_:]*\z/
  @label_name ~r/\A[a-zA-Z_][a-zA-Z0-9_]*\z/
  @max_int 9_223_372_036_854_775_807
  @min_int -9_223_372_036_854_775_808
  @max_float 1.797_693_134_862_315_7e308

  @type error :: :not_a_json_object | {:missing_field, String.t()} | {:invalid_field, String.t()}

  @spec parse(binary()) :: {:ok, Sample.t()} | {:error, error()}
  def parse(body) when is_binary(body) do
    case JSON.decode(body) do
      {:ok, %{} = object} -> from_object(object)
      _not_an_object -> {:error, :not_a_json_object}
    end
  end

  @doc "Validates an already decoded JSON value."
  @spec from_object(term()) :: {:ok, Sample.t()} | {:error, error()}
  def from_object(%{} = object) when not is_struct(object) do
    with {:ok, name} <- fetch(object, "name", &metric_name/1),
         {:ok, value} <- fetch(object, "value", &number/1),
         {:ok, timestamp} <- optional(object, "timestamp", &timestamp/1),
         {:ok, labels} <- optional(object, "labels", &labels/1) do
      {:ok, %Sample{name: name, labels: labels || %{}, value: value, timestamp: timestamp}}
    end
  end

  def from_object(_other), do: {:error, :not_a_json_object}

  defp fetch(object, field, check) do
    case Map.get(object, field) do
      nil -> {:error, {:missing_field, field}}
      value -> checked(field, check.(value))
    end
  end

  # An absent or null optional field is simply not given.
  defp optional(object, field, check) do
    case Map.get(object, field) do
      nil -> {:ok, nil}
      value -> checked(field, check.(value))
    end
  end

  defp checked(_field, {:ok, value}), do: {:ok, value}
  defp checked(field, :error), do: {:error, {:invalid_field, field}}

  defp metric_name(value) when is_binary(value) do
    if Regex.match?(@metric_name, value), do: {:ok, value}, else: :error
  end

  defp metric_name(_value), do: :error

  defp number(value) when is_float(value), do: {:ok, value}

  defp number(value) when is_integer(value) do
    if abs(value) <= @max_float, do: {:ok, value * 1.0}, else: :error
  rescue
    ArithmeticError -> :error
  end

  defp number(_value), do: :error

  defp timestamp(value) when is_integer(value) and value >= @min_int and value <= @max_int,
    do: {:ok, value}

  defp timestamp(_value), do: :error

  defp labels(%{} = labels) when not is_struct(labels) do
    if Enum.all?(labels, fn {name, value} ->
         is_binary(name) and Regex.match?(@label_name, name) and is_binary(value)
       end),
       do: {:ok, labels},
       else: :error
  end

  defp labels(_other), do: :error
end

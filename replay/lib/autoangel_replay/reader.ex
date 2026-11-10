defmodule AutoangelReplay.Reader do
  @moduledoc """
  Reads recorded Series for a replay (Requirements 77.1, 77.7).

  Two formats, chosen by file extension:

    * `.csv` with the header `series,timestamp_ms,value`, where `series` is a
      Prometheus selector such as `cpu{host="a"}` (quoted when it contains a
      comma), `timestamp_ms` an integer and `value` a float, `NaN`, `+Inf` or
      `-Inf`;
    * `.json`, a Prometheus range-query export
      (`{"data": {"resultType": "matrix", "result": [{"metric": {...},
      "values": [[seconds, "value"], ...]}]}}`).

  An unreadable or malformed file, a file with no Series, or any Series with no
  Samples is reported as an error naming the file or the Series, and no Series
  are returned, so no metrics are computed from partial input.
  """

  alias AutoangelReplay.Series

  @type error ::
          {:unreadable, Path.t(), term()}
          | {:empty_series, String.t()}
          | {:no_series, Path.t()}

  @spec read(Path.t()) :: {:ok, [Series.t()]} | {:error, error()}
  def read(path) do
    with {:ok, contents} <- read_file(path),
         {:ok, series} <- parse(Path.extname(path), contents, path) do
      validate(series, path)
    end
  end

  @doc "Reads several files; the first error stops the read."
  @spec read_all([Path.t()]) :: {:ok, [Series.t()]} | {:error, error()}
  def read_all(paths) do
    paths
    |> Enum.reduce_while({:ok, []}, fn path, {:ok, acc} ->
      case read(path) do
        {:ok, series} -> {:cont, {:ok, acc ++ series}}
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, series} -> validate(series, Enum.join(paths, ","))
      error -> error
    end
  end

  defp read_file(path) do
    case File.read(path) do
      {:ok, contents} -> {:ok, contents}
      {:error, reason} -> {:error, {:unreadable, path, reason}}
    end
  end

  defp parse(".csv", contents, path), do: parse_csv(contents, path)
  defp parse(".json", contents, path), do: parse_json(contents, path)
  defp parse(ext, _contents, path), do: {:error, {:unreadable, path, {:unsupported_format, ext}}}

  defp validate([], path), do: {:error, {:no_series, path}}

  defp validate(series, path) do
    cond do
      empty = Enum.find(series, &(&1.samples == [])) ->
        {:error, {:empty_series, Series.display(empty)}}

      duplicate = duplicate(series) ->
        {:error, {:unreadable, path, {:duplicate_series, Series.display(duplicate)}}}

      true ->
        {:ok, series}
    end
  end

  # The same Series twice would make ground-truth labels ambiguous.
  defp duplicate(series) do
    series
    |> Enum.group_by(&Series.key/1)
    |> Enum.find_value(fn {_key, group} -> if length(group) > 1, do: hd(group) end)
  end

  # --- CSV --------------------------------------------------------------------

  defp parse_csv(contents, path) do
    case contents |> String.split(~r/\r?\n/, trim: true) do
      [header | rows] ->
        if header |> String.trim() |> String.replace(" ", "") == "series,timestamp_ms,value",
          do: csv_rows(rows, path),
          else: {:error, {:unreadable, path, :bad_header}}

      [] ->
        {:error, {:no_series, path}}
    end
  end

  defp csv_rows(rows, path) do
    rows
    |> Enum.with_index(2)
    |> Enum.reduce_while({:ok, %{}}, fn {row, line}, {:ok, acc} ->
      with {:ok, selector, ts, value} <- split_row(row),
           {:ok, name, labels} <- parse_selector(selector),
           {:ok, ts} <- parse_int(ts),
           {:ok, value} <- parse_value(value) do
        {:cont, {:ok, Map.update(acc, {name, labels}, [{ts, value}], &[{ts, value} | &1])}}
      else
        _ -> {:halt, {:error, {:unreadable, path, {:bad_row, line}}}}
      end
    end)
    |> case do
      {:ok, groups} -> {:ok, for({{n, l}, s} <- Enum.sort(groups), do: Series.new(n, l, s))}
      error -> error
    end
  end

  # The selector may be quoted (it contains commas); timestamp and value never are.
  defp split_row(row) do
    case Regex.run(~r/^\s*(?:"((?:[^"]|"")*)"|([^,]*))\s*,\s*([^,]+?)\s*,\s*([^,]+?)\s*$/, row) do
      [_, quoted, "", ts, value] -> {:ok, String.replace(quoted, ~s(""), ~s(")), ts, value}
      [_, "", plain, ts, value] -> {:ok, plain, ts, value}
      _ -> :error
    end
  end

  @doc false
  def parse_selector(selector) do
    case Regex.run(~r/^\s*([a-zA-Z_:][a-zA-Z0-9_:]*)\s*(?:\{(.*)\})?\s*$/, selector) do
      [_, name] -> {:ok, name, %{}}
      [_, name, ""] -> {:ok, name, %{}}
      [_, name, inner] -> parse_labels(name, inner)
      _ -> :error
    end
  end

  @label_pair ~r/\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"((?:[^"\\]|\\.)*)"\s*,?/

  defp parse_labels(name, inner) do
    pairs = Regex.scan(@label_pair, inner)
    consumed = pairs |> Enum.map_join(&hd/1) |> String.replace(" ", "")
    labels = Map.new(pairs, fn [_, k, v] -> {k, unescape(v)} end)
    if consumed == String.replace(inner, " ", ""), do: {:ok, name, labels}, else: :error
  end

  defp unescape(v), do: v |> String.replace(~s(\\"), ~s(")) |> String.replace("\\\\", "\\")

  defp parse_int(s) do
    case Integer.parse(String.trim(s)) do
      {n, ""} -> {:ok, n}
      _ -> :error
    end
  end

  @doc false
  def parse_value(s) do
    case String.trim(s) do
      "NaN" -> {:ok, :nan}
      "+Inf" -> {:ok, :inf}
      "Inf" -> {:ok, :inf}
      "-Inf" -> {:ok, :neg_inf}
      other -> float(other)
    end
  end

  defp float(s) do
    case Float.parse(s) do
      {f, ""} -> {:ok, f}
      _ -> :error
    end
  end

  # --- JSON range export --------------------------------------------------------

  defp parse_json(contents, path) do
    with {:ok, doc} <- decode(contents),
         {:ok, result} <- matrix(doc) do
      json_series(result, path)
    else
      _ -> {:error, {:unreadable, path, :not_a_range_export}}
    end
  end

  defp decode(contents) do
    case JSON.decode(contents) do
      {:ok, doc} -> {:ok, doc}
      {:error, _} -> :error
    end
  end

  defp matrix(%{"data" => %{"resultType" => "matrix", "result" => result}}) when is_list(result),
    do: {:ok, result}

  defp matrix(_), do: :error

  defp json_series(result, path) do
    Enum.reduce_while(result, {:ok, []}, fn entry, {:ok, acc} ->
      case json_entry(entry) do
        {:ok, series} -> {:cont, {:ok, [series | acc]}}
        :error -> {:halt, {:error, {:unreadable, path, :bad_series}}}
      end
    end)
    |> case do
      {:ok, list} -> {:ok, Enum.reverse(list)}
      error -> error
    end
  end

  defp json_entry(%{"metric" => metric, "values" => values})
       when is_map(metric) and is_list(values) do
    {name, labels} = Map.pop(metric, "__name__", "unnamed")

    with true <- Enum.all?(labels, fn {k, v} -> is_binary(k) and is_binary(v) end),
         {:ok, samples} <- json_samples(values) do
      {:ok, Series.new(name, labels, samples)}
    else
      _ -> :error
    end
  end

  defp json_entry(_), do: :error

  defp json_samples(values) do
    Enum.reduce_while(values, {:ok, []}, fn
      [ts, value], {:ok, acc} when is_number(ts) and is_binary(value) ->
        case parse_value(value) do
          {:ok, v} -> {:cont, {:ok, [{round(ts * 1000), v} | acc]}}
          :error -> {:halt, :error}
        end

      _, _ ->
        {:halt, :error}
    end)
  end
end

defmodule Autoangel.Exposition do
  @moduledoc "Parser and printer for Prometheus text exposition format 0.0.4."

  defmodule Sample do
    @moduledoc "A single Prometheus exposition sample."
    @type t :: %__MODULE__{}
    defstruct name: nil,
              labels: %{},
              value: nil,
              timestamp: nil,
              type_hint: nil,
              series_kind: nil,
              source: nil,
              received_at: nil,
              batch_uuid: nil
  end

  defmodule Family do
    @moduledoc "A metric family and its exposition samples."
    @type t :: %__MODULE__{}
    defstruct name: nil, help: nil, type: nil, samples: []
  end

  @name ~r/^[a-zA-Z_:][a-zA-Z0-9_:]*$/
  @label_name ~r/^[a-zA-Z_][a-zA-Z0-9_]*$/
  @types ~w(counter gauge histogram summary untyped)a

  @spec parse(binary()) ::
          {:ok, [Family.t()]} | {:error, %{line: pos_integer(), reason: String.t()}}
  def parse(body) when is_binary(body) do
    body
    |> String.split("\n", trim: false)
    |> Enum.with_index(1)
    |> parse_lines(%{}, %{}, %{}, [])
  end

  def parse(_body), do: {:error, %{line: 1, reason: "body must be a binary"}}

  @spec print([Family.t()]) :: list()
  def print(families) when is_list(families), do: Enum.map(families, &print_family/1)

  defp print_family(family) do
    type_line =
      if family.type, do: ["# TYPE ", family.name, " ", Atom.to_string(family.type), "\n"], else: []

    help_line =
      if family.help != nil,
        do: ["# HELP ", family.name, " ", escape_help(family.help), "\n"],
        else: []

    [help_line, type_line, Enum.map(family.samples, &print_sample/1)]
  end

  defp print_sample(sample) do
    timestamp =
      if is_integer(sample.timestamp), do: [" ", Integer.to_string(sample.timestamp)], else: []

    [sample.name, print_labels(sample.labels), " ", print_value(sample.value), timestamp, "\n"]
  end

  def print(_families), do: []

  defp parse_lines([], families, metadata, samples, _seen) do
    family_map =
      Enum.reduce(metadata, %{}, fn {family, %{type: type}}, acc ->
        acc
        |> Map.put(family, family)
        |> add_child_families(family, type)
      end)

    grouped =
      Enum.reduce(samples, %{}, fn {sample_name, values}, acc ->
        family = Map.get(family_map, sample_name, sample_name)
        Map.update(acc, family, Enum.reverse(values), &(Enum.reverse(values) ++ &1))
      end)

    names =
      (Map.keys(families) ++ Map.keys(metadata) ++ Map.keys(grouped))
      |> Enum.uniq()
      |> Enum.reject(&(Map.get(family_map, &1, &1) != &1 and not Map.has_key?(metadata, &1)))

    result =
      Enum.map(names, fn name ->
        meta = Map.get(metadata, name, %{})
        hint = Map.get(meta, :type)
        parsed_samples = Map.get(grouped, name, [])

        %Family{
          name: name,
          help: Map.get(meta, :help),
          type: hint,
          samples:
            Enum.map(parsed_samples, fn sample ->
              %{
                sample
                | type_hint: hint_for_sample(hint, sample),
                  series_kind: kind_for_sample(sample, hint)
              }
            end)
        }
      end)

    {:ok, result}
  end

  defp parse_lines([{line, number} | rest], families, metadata, samples, seen) do
    case String.trim(line) do
      "" ->
        parse_lines(rest, families, metadata, samples, seen)

      <<"#", comment::binary>> ->
        parse_comment(String.trim_leading(comment), number, rest, families, metadata, samples, seen)

      content ->
        parse_sample(content, number, rest, families, metadata, samples, seen)
    end
  end

  defp parse_comment("HELP " <> content, number, rest, families, metadata, samples, seen) do
    case String.split(content, " ", parts: 2) do
      [name, help] -> parse_help(name, help, number, rest, families, metadata, samples, seen)
      [name] -> parse_help(name, "", number, rest, families, metadata, samples, seen)
      _ -> error(number, "invalid HELP line")
    end
  end

  defp parse_help(name, help, number, rest, families, metadata, samples, seen) do
    cond do
      not valid_name?(name) ->
        error(number, "invalid HELP line")

      Map.has_key?(metadata, name) and Map.has_key?(Map.get(metadata, name), :help) ->
        error(number, "duplicate HELP line")

      Map.has_key?(samples, name) ->
        error(number, "HELP line must precede samples")

      not String.valid?(help) ->
        error(number, "invalid HELP text")

      true ->
        value = unescape_help(help)

        parse_lines(
          rest,
          Map.put_new(families, name, true),
          Map.update(metadata, name, %{help: value}, &Map.put(&1, :help, value)),
          samples,
          seen
        )
    end
  end

  defp parse_comment("TYPE " <> content, number, rest, families, metadata, samples, seen) do
    case String.split(content, " ", parts: 2) do
      [name, type] ->
        cond do
          not valid_name?(name) or type not in Enum.map(@types, &Atom.to_string/1) ->
            error(number, "invalid TYPE line")

          Map.has_key?(metadata, name) and Map.has_key?(Map.get(metadata, name), :type) ->
            error(number, "duplicate TYPE line")

          Map.has_key?(samples, name) ->
            error(number, "TYPE line must precede samples")

          true ->
            atom_type = String.to_atom(type)

            parse_lines(
              rest,
              Map.put_new(families, name, true),
              Map.update(metadata, name, %{type: atom_type}, &Map.put(&1, :type, atom_type)),
              samples,
              seen
            )
        end

      _ ->
        error(number, "invalid TYPE line")
    end
  end

  defp parse_comment(_comment, _number, rest, families, metadata, samples, seen),
    do: parse_lines(rest, families, metadata, samples, seen)

  defp parse_sample(content, number, rest, families, metadata, samples, seen) do
    with {:ok, name, labels, value_text, timestamp_text} <- split_sample(content),
         :ok <- validate_name(name),
         {:ok, value} <- parse_value(value_text),
         {:ok, timestamp} <- parse_timestamp(timestamp_text),
         {:ok, labels} <- parse_labels(labels) do
      sample = %Sample{name: name, labels: labels, value: value, timestamp: timestamp}
      family_name = family_name(name)
      updated = Map.update(samples, family_name, [sample], &[sample | &1])

      parse_lines(rest, Map.put_new(families, family_name, true), metadata, updated, [
        family_name | seen
      ])
    else
      {:error, reason} -> error(number, reason)
    end
  end

  defp split_sample(content) do
    case Regex.run(~r/^([^\s{]+)(?:\{(.*)\})?[ \t]+([^\s]+)(?:[ \t]+([^\s]+))?[ \t]*$/, content,
           capture: :all_but_first
         ) do
      [name, labels, value, timestamp] -> {:ok, name, labels, value, timestamp}
      [name, labels, value] -> {:ok, name, labels, value, nil}
      _ -> {:error, "invalid sample syntax"}
    end
  end

  defp parse_labels(""), do: {:ok, %{}}

  defp parse_labels(text) do
    with {:ok, pairs} <- label_pairs(text, [], false),
         :ok <- unique_labels(pairs) do
      {:ok, Map.new(pairs)}
    end
  end

  defp label_pairs(<<>>, acc, true), do: {:ok, Enum.reverse(acc)}
  defp label_pairs(<<>>, _acc, false), do: {:error, "unterminated label set"}

  defp label_pairs(text, acc, _started) do
    case Regex.run(~r/^([a-zA-Z_][a-zA-Z0-9_]*)[ \t]*=[ \t]*"/, text) do
      [full, name] ->
        prefix = binary_part(text, byte_size(full), byte_size(text) - byte_size(full))

        case take_quoted(prefix, <<>>) do
          {:ok, value, remainder} -> handle_label(name, value, remainder, acc)
          :error -> {:error, "unterminated label value"}
        end

      _ ->
        {:error, "invalid label set"}
    end
  end

  defp take_quoted(<<>>, _acc), do: :error

  defp take_quoted(<<?\\, next, rest::binary>>, acc),
    do: take_quoted(rest, <<acc::binary, ?\\, next>>)

  defp take_quoted(<<?\", rest::binary>>, acc), do: {:ok, acc, rest}
  defp take_quoted(<<char, rest::binary>>, acc), do: take_quoted(rest, <<acc::binary, char>>)

  defp handle_label(name, value, rest, acc) do
    with true <- valid_label_name?(name),
         {:ok, decoded} <- unescape(value),
         true <- String.valid?(decoded) do
      cond do
        rest == "" ->
          {:ok, [{name, decoded} | acc] |> Enum.reverse()}

        String.starts_with?(rest, ",") ->
          label_pairs(
            binary_part(rest, 1, byte_size(rest) - 1) |> String.trim_leading(),
            [{name, decoded} | acc],
            true
          )

        true ->
          {:error, "missing label separator"}
      end
    else
      false -> {:error, "invalid label name"}
      _ -> {:error, "invalid label value"}
    end
  end

  defp unique_labels(pairs) do
    if length(pairs) == map_size(Map.new(pairs)), do: :ok, else: {:error, "duplicate label name"}
  end

  defp parse_value("NaN"), do: {:ok, :nan}
  defp parse_value("+Inf"), do: {:ok, :inf}
  defp parse_value("-Inf"), do: {:ok, :neg_inf}

  defp parse_value(text) do
    case Float.parse(text) do
      {value, ""} when is_float(value) -> {:ok, value}
      _ -> {:error, "invalid sample value"}
    end
  end

  defp parse_timestamp(nil), do: {:ok, nil}

  defp parse_timestamp(text) do
    with true <- Regex.match?(~r/^-?\d+$/, text),
         {timestamp, ""} <- Integer.parse(text),
         true <- timestamp >= -9_223_372_036_854_775_808 and timestamp <= 9_223_372_036_854_775_807 do
      {:ok, timestamp}
    else
      _ -> {:error, "invalid timestamp"}
    end
  end

  defp validate_name(name) do
    if valid_name?(name), do: :ok, else: {:error, "invalid metric name"}
  end

  defp valid_name?(name), do: Regex.match?(@name, name)
  defp valid_label_name?(name), do: Regex.match?(@label_name, name)

  defp family_name(name), do: name

  defp add_child_families(acc, family, :histogram) do
    acc
    |> Map.put(family <> "_bucket", family)
    |> Map.put(family <> "_sum", family)
    |> Map.put(family <> "_count", family)
  end

  defp add_child_families(acc, family, :summary) do
    acc
    |> Map.put(family <> "_sum", family)
    |> Map.put(family <> "_count", family)
  end

  defp add_child_families(acc, _family, _type), do: acc

  defp hint_for_sample(nil, _sample), do: nil

  defp hint_for_sample(type, _sample), do: type

  defp kind_for_sample(%Sample{} = sample, type) do
    Autoangel.SeriesKind.infer(sample.name, type, sample.labels)
  end

  defp unescape_help(text) do
    case unescape(text) do
      {:ok, value} -> value
      {:error, _} -> text
    end
  end

  defp unescape(text), do: unescape(text, <<>>)
  defp unescape(<<>>, acc), do: {:ok, acc}
  defp unescape(<<?\\, ?n, rest::binary>>, acc), do: unescape(rest, <<acc::binary, ?\n>>)

  defp unescape(<<?\\, char, rest::binary>>, acc) when char in [?\\, ?"],
    do: unescape(rest, <<acc::binary, char>>)

  defp unescape(<<?\\, _char, _rest::binary>>, _acc), do: {:error, "invalid escape"}
  defp unescape(<<char, rest::binary>>, acc), do: unescape(rest, <<acc::binary, char>>)

  defp escape_help(text), do: text |> String.replace("\\", "\\\\") |> String.replace("\n", "\\n")

  defp print_labels(labels) when map_size(labels) == 0, do: []

  defp print_labels(labels) do
    rendered =
      labels
      |> Enum.sort_by(fn {name, _} -> name end)
      |> Enum.map_join(",", fn {name, value} -> name <> "=\"" <> escape_label(value) <> "\"" end)

    ["{", rendered, "}"]
  end

  defp escape_label(value),
    do:
      value
      |> String.replace("\\", "\\\\")
      |> String.replace("\n", "\\n")
      |> String.replace("\"", "\\\"")

  defp print_value(:nan), do: "NaN"
  defp print_value(:inf), do: "+Inf"
  defp print_value(:neg_inf), do: "-Inf"
  defp print_value(value) when is_float(value), do: :erlang.float_to_binary(value, [:short])
  defp print_value(value) when is_integer(value), do: :erlang.float_to_binary(value / 1, [:short])

  defp error(line, reason), do: {:error, %{line: line, reason: reason}}
end

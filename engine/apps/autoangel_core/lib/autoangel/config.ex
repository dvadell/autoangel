defmodule Autoangel.Config do
  # credo:disable-for-this-file Credo.Check.Refactor.CyclomaticComplexity
  # credo:disable-for-this-file Credo.Check.Refactor.Nesting
  @moduledoc "Loads, validates and redacts Engine configuration before supervision starts."

  require Logger

  alias Autoangel.Secret

  @defaults %{
    cardinality: %{per_metric: 2_000, total: 50_000},
    learning: %{duration: 86_400, cycles: 2},
    missing_data: %{multiple: 3.0, sweep_interval: 15},
    persistence: %{n: 3, m: 5},
    correlation: %{candidate_pool_limit: 20, support_threshold: 2, window: 600},
    retention: %{raw: 14 * 86_400, aggregate: 13 * 30 * 86_400, anomaly: 90 * 86_400},
    heartbeat_interval: 15,
    ingest_rate_limit: %{rate: 50, burst: 200},
    storage: %{backend: :none, password: nil},
    scrape_targets: [],
    ingest: %{token: nil, username: nil, password: nil},
    control_api: %{token: nil, username: nil, password: nil},
    http: %{ingest_port: 9_400, control_port: 9_401},
    tls: %{cert_file: nil, key_file: nil},
    auth_exemptions: [],
    plugins: %{}
  }

  @secret_paths [
    [:ingest, :token],
    [:ingest, :password],
    [:control_api, :token],
    [:control_api, :password],
    [:storage, :password]
  ]

  @doc "Loads a TOML file or TOML string and exits with an error tuple when invalid."
  def load(source, env \\ System.get_env()) do
    with {:ok, raw} <- read_source(source),
         {:ok, parsed} <- safe_parse(raw),
         config <- merge(@defaults, atomize_keys(parsed)),
         config <- normalize_config(config),
         config <- safe_apply_secret_env(config, env),
         plugin_result <- Autoangel.Plugin.Registry.validate(config),
         errors <- validate(config, plugin_result) do
      if errors == [] do
        {:ok, plugins} = plugin_result
        config = config |> Map.put(:enabled_plugins, plugins) |> redact_secrets()
        install_filter(config)
        {:ok, config}
      else
        Enum.each(errors, &Logger.error("invalid configuration setting: #{&1}"))
        exit({:shutdown, {:invalid_configuration, errors}})
      end
    else
      {:error, _reason} ->
        Logger.error("invalid configuration setting: configuration")
        exit({:shutdown, {:invalid_configuration, ["configuration"]}})
    end
  end

  defp read_source(source) when is_binary(source) do
    if String.contains?(source, "\n") or String.contains?(source, "=") or
         String.starts_with?(String.trim_leading(source), "[") do
      {:ok, source}
    else
      File.read(source)
    end
  end

  defp safe_parse(raw) do
    parse(raw)
  rescue
    _ -> {:error, :invalid_toml}
  end

  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity, Credo.Check.Refactor.Nesting
  defp parse(text) do
    text
    |> String.split("\n")
    |> Enum.reduce_while({:ok, {%{}, []}}, fn line, {:ok, {config, path}} ->
      line = line |> String.trim() |> strip_comment()

      cond do
        line == "" ->
          {:cont, {:ok, {config, path}}}

        String.starts_with?(line, "[[") and String.ends_with?(line, "]]") ->
          name = line |> String.trim_leading("[[") |> String.trim_trailing("]]") |> split_path()

          current = get_in(config, name) || []

          {:cont, {:ok, {put_nested(config, name, current ++ [%{}]), name ++ [length(current)]}}}

        String.starts_with?(line, "[") and String.ends_with?(line, "]") ->
          {:cont,
           {:ok,
            {config, line |> String.trim_leading("[") |> String.trim_trailing("]") |> split_path()}}}

        true ->
          case String.split(line, "=", parts: 2) do
            [name, value] ->
              case parse_value(String.trim(value)) do
                {:ok, parsed} ->
                  {:cont,
                   {:ok, {put_path(config, path ++ split_path(String.trim(name)), parsed), path}}}

                _ ->
                  {:halt, {:error, {:invalid_toml, line}}}
              end

            _ ->
              {:halt, {:error, {:invalid_toml, line}}}
          end
      end
    end)
    |> case do
      {:ok, {config, _}} -> {:ok, config}
      error -> error
    end
  end

  defp parse_value(<<34, rest::binary>>) do
    case parse_quoted(rest, []) do
      {:ok, value, ""} ->
        {:ok, value}

      {:ok, value, trailing} ->
        if String.trim(trailing) == "", do: {:ok, value}, else: {:error, :invalid_value}

      :error ->
        {:error, :invalid_value}
    end
  end

  defp parse_quoted(<<>>, _acc), do: :error

  defp parse_quoted(<<34, rest::binary>>, acc),
    do: {:ok, acc |> Enum.reverse() |> IO.iodata_to_binary(), rest}

  defp parse_quoted(<<?\\, code, rest::binary>>, acc) do
    escaped =
      case code do
        ?" -> "\""
        ?\\ -> "\\"
        ?n -> "\n"
        ?r -> "\r"
        ?t -> "\t"
        _ -> :invalid
      end

    if escaped == :invalid, do: :error, else: parse_quoted(rest, [escaped | acc])
  end

  defp parse_quoted(<<code, rest::binary>>, acc), do: parse_quoted(rest, [<<code>> | acc])

  defp parse_value("true"), do: {:ok, true}
  defp parse_value("false"), do: {:ok, false}

  defp parse_value(value) do
    cond do
      String.starts_with?(value, "[") and String.ends_with?(value, "]") ->
        inner = value |> String.trim_leading("[") |> String.trim_trailing("]") |> String.trim()
        {:ok, if(inner == "", do: [], else: split_commas(inner) |> Enum.map(&parse_value!/1))}

      String.starts_with?(value, "{") and String.ends_with?(value, "}") ->
        value
        |> String.trim_leading("{")
        |> String.trim_trailing("}")
        |> split_commas()
        |> Enum.reduce(%{}, fn pair, acc ->
          [k, v] = String.split(pair, "=", parts: 2)
          Map.put(acc, key(k), parse_value!(String.trim(v)))
        end)
        |> then(&{:ok, &1})

      Regex.match?(~r/^[-+]?\d+$/, value) ->
        {:ok, String.to_integer(value)}

      Regex.match?(~r/^[-+]?(\d+\.\d*|\d*\.\d+)([eE][-+]?\d+)?$/, value) ->
        {:ok, String.to_float(value)}

      true ->
        {:error, :invalid_value}
    end
  end

  defp parse_value!(value) do
    case parse_value(value) do
      {:ok, parsed} -> parsed
      {:error, _} -> raise ArgumentError, "invalid TOML value"
    end
  end

  defp split_commas(value),
    do:
      Regex.scan(~r/(?:[^,{}\"]|\"[^\"]*\"|\{[^}]*\})+/, value)
      |> Enum.map(&(hd(&1) |> String.trim()))

  defp strip_comment(line),
    do: line |> String.to_charlist() |> strip_comment([], false) |> Enum.reverse() |> to_string()

  defp strip_comment([], acc, _quoted), do: acc
  defp strip_comment([?# | _rest], acc, false), do: acc

  defp strip_comment([?\\, next | rest], acc, quoted),
    do: strip_comment(rest, [next, ?\\ | acc], quoted)

  defp strip_comment([?" | rest], acc, quoted), do: strip_comment(rest, [?" | acc], not quoted)
  defp strip_comment([char | rest], acc, quoted), do: strip_comment(rest, [char | acc], quoted)
  defp split_path(name), do: name |> String.split(".") |> Enum.map(&key/1)
  defp key(name), do: name |> String.trim() |> String.trim("\"") |> String.to_atom()

  defp put_path(_map, [], value), do: value

  defp put_path(list, [index | rest], value) when is_list(list) and is_integer(index) do
    List.update_at(list, index, &put_path(&1, rest, value))
  end

  defp put_path(map, [key], value), do: Map.put(map, key, value)

  defp put_path(map, [key | rest], value),
    do: Map.put(map, key, put_path(Map.get(map, key, %{}), rest, value))

  defp put_nested(map, [key], value), do: Map.put(map, key, value)

  defp put_nested(map, [key | rest], value),
    do: Map.put(map, key, put_nested(Map.get(map, key, %{}), rest, value))

  defp merge(defaults, overrides) when is_map(defaults) and is_map(overrides) do
    Map.merge(defaults, overrides, fn _key, left, right -> merge(left, right) end)
  end

  defp merge(_left, right), do: right

  defp atomize_keys(map) when is_map(map),
    do: Map.new(map, fn {k, v} -> {if(is_binary(k), do: key(k), else: k), atomize_keys(v)} end)

  defp atomize_keys(list) when is_list(list), do: Enum.map(list, &atomize_keys/1)
  defp atomize_keys(value), do: value

  defp safe_apply_secret_env(config, env) do
    apply_secret_env(config, env)
  rescue
    _ -> Map.put(config, :configuration_error, true)
  end

  defp apply_secret_env(config, env) do
    Enum.reduce(@secret_paths, config, fn path, acc ->
      env_key = "AUTOANGEL_" <> (path |> Enum.map_join("_", &Atom.to_string/1) |> String.upcase())

      case Map.get(env, env_key) do
        nil -> acc
        value -> put_path(acc, path, value)
      end
    end)
  end

  defp normalize_config(config) do
    config
    |> normalize_backend()
    |> normalize_auth_exemptions()
  end

  defp normalize_backend(%{storage: storage} = config) when is_map(storage) do
    backend = storage[:backend]
    put_in(config, [:storage, :backend], normalize_backend_value(backend))
  end

  defp normalize_backend(config), do: config
  defp normalize_backend_value("none"), do: :none
  defp normalize_backend_value("timescaledb"), do: :timescaledb
  defp normalize_backend_value(value), do: value

  defp normalize_auth_exemptions(%{auth: auth} = config) when is_map(auth) do
    case Map.fetch(auth, :exemptions) do
      {:ok, exemptions} -> Map.put(config, :auth_exemptions, exemptions)
      :error -> config
    end
  end

  defp normalize_auth_exemptions(config), do: config

  defp validate(config, plugin_result) when is_map(config) do
    cardinality = section(config, :cardinality)
    learning = section(config, :learning)
    missing = section(config, :missing_data)
    persistence = section(config, :persistence)
    correlation = section(config, :correlation)
    retention = section(config, :retention)
    rate = section(config, :ingest_rate_limit)
    storage = section(config, :storage)
    tls = section(config, :tls)
    http = section(config, :http)

    checks = [
      {:configuration, config[:configuration_error] != true, & &1},
      {:cardinality_per_metric, cardinality[:per_metric], &positive_integer?/1},
      {:cardinality_total, cardinality[:total], &positive_integer?/1},
      {:learning_duration, learning[:duration], &positive_number?/1},
      {:learning_cycles, learning[:cycles], &(is_integer(&1) and &1 >= 2)},
      {:missing_data_multiple, missing[:multiple], &(is_number(&1) and &1 > 1)},
      {:sweep_interval, missing[:sweep_interval], &positive_number?/1},
      {:persistence_n, persistence[:n], &positive_integer?/1},
      {:persistence_m, persistence[:m], &positive_number?/1},
      {:candidate_pool_limit, correlation[:candidate_pool_limit], &positive_integer?/1},
      {:support_threshold, correlation[:support_threshold], &(is_integer(&1) and &1 >= 0)},
      {:retention_raw, retention[:raw], &positive_integer?/1},
      {:retention_aggregate, retention[:aggregate], &positive_integer?/1},
      {:retention_anomaly, retention[:anomaly], &positive_integer?/1},
      {:heartbeat_interval, config[:heartbeat_interval], &positive_number?/1},
      {:ingest_rate, rate[:rate], &positive_number?/1},
      {:ingest_burst, rate[:burst], &positive_integer?/1},
      {:storage_backend, storage[:backend], &valid_backend?/1},
      {:storage_backend_selection, config, &storage_selection_valid?/1},
      {:storage_password, storage[:password], &valid_secret?/1},
      {:http_ingest_port, http[:ingest_port], &valid_port?/1},
      {:http_control_port, http[:control_port], &valid_port?/1},
      {:tls, tls, &valid_tls?/1},
      {:auth_exemptions, config[:auth_exemptions], &valid_exemptions?/1},
      {:distinct_credentials, distinct_credentials?(config), & &1},
      {:scrape_targets, config[:scrape_targets], &valid_targets?/1},
      {:ingest_credentials, config[:ingest], &valid_credentials?/1},
      {:control_api_credentials, config[:control_api], &valid_credentials?/1}
    ]

    Enum.flat_map(checks, fn {name, value, predicate} ->
      if predicate.(value), do: [], else: [name]
    end) ++ plugin_validation_errors(plugin_result)
  end

  defp plugin_validation_errors({:ok, _enabled}), do: []
  defp plugin_validation_errors({:error, errors}) when is_list(errors), do: errors

  defp section(config, key), do: if(is_map(config[key]), do: config[key], else: %{})

  defp valid_backend?(value), do: value in [:none, :timescaledb, "none", "timescaledb"]

  defp storage_selection_valid?(config) do
    match?({:ok, _backend}, Autoangel.Storage.select_backend(config))
  end

  defp distinct_credentials?(config) do
    ingest = section(config, :ingest)
    control = section(config, :control_api)
    same_token = ingest[:token] not in [nil, ""] and ingest[:token] == control[:token]

    same_basic =
      ingest[:username] not in [nil, ""] and ingest[:username] == control[:username] and
        ingest[:password] == control[:password]

    not same_token and not same_basic
  end

  defp valid_exemptions?(value),
    do:
      is_list(value) and Enum.uniq(value) == value and
        Enum.all?(value, &(&1 in ["/healthz", "/readyz"]))

  defp valid_credentials?(nil), do: true

  defp valid_credentials?(credentials) when is_map(credentials) do
    valid_secret?(credentials[:token]) and
      valid_basic?(credentials[:username], credentials[:password])
  end

  defp valid_credentials?(_), do: false
  defp valid_basic?(nil, nil), do: true

  defp valid_basic?(username, password),
    do: is_binary(username) and username != "" and is_binary(password) and password != ""

  defp valid_secret?(nil), do: true
  defp valid_secret?(value), do: is_binary(value) and value != ""

  defp valid_targets?(targets) when is_list(targets), do: Enum.all?(targets, &valid_target?/1)
  defp valid_targets?(_), do: false

  defp valid_target?(target) when is_map(target),
    do:
      is_binary(target[:url]) and target[:url] != "" and positive_number?(target[:interval]) and
        positive_number?(target[:timeout]) and target[:timeout] <= target[:interval]

  defp valid_target?(_), do: false
  defp positive_integer?(value), do: is_integer(value) and value > 0
  defp positive_number?(value), do: (is_integer(value) or is_float(value)) and value > 0
  defp valid_port?(value), do: is_integer(value) and value in 1..65_535

  defp valid_tls?(tls) when is_map(tls) do
    cert_file = tls[:cert_file]
    key_file = tls[:key_file]

    (is_nil(cert_file) and is_nil(key_file)) or
      (is_binary(cert_file) and cert_file != "" and is_binary(key_file) and key_file != "")
  end

  defp valid_tls?(_), do: false

  defp redact_secrets(config),
    do:
      Enum.reduce(@secret_paths, config, fn path, acc ->
        if value = get_in(acc, path), do: put_path(acc, path, Secret.new(value)), else: acc
      end)

  defp install_filter(config) do
    secrets =
      @secret_paths
      |> Enum.map(&get_in(config, &1))
      |> Enum.filter(&match?(%Secret{}, &1))
      |> Enum.sort_by(&byte_size(Secret.unwrap(&1)), :desc)

    :logger.remove_primary_filter(:autoangel_secrets)

    if secrets != [] do
      case :logger.add_primary_filter(
             :autoangel_secrets,
             {&Autoangel.LoggerSecretFilter.filter/2, secrets}
           ) do
        :ok -> :ok
        {:error, reason} -> exit({:shutdown, {:logger_filter_install_failed, reason}})
      end
    end
  end
end

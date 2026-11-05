defmodule Autoangel.Plugin.Email do
  @moduledoc "SMTP alert plugin."
  @behaviour Autoangel.Plugin

  @default_port 587
  @default_timeout 10_000

  @impl true
  def validate_config(config) when is_map(config) do
    missing =
      [
        {:server, value(config, :server)},
        {:sender, value(config, :sender)},
        {:recipients, value(config, :recipients)}
      ]
      |> Enum.flat_map(fn
        {:recipients, recipients} when is_list(recipients) and recipients != [] -> []
        {_field, value} when is_binary(value) and byte_size(value) > 0 -> []
        {field, _value} -> [field]
      end)

    invalid =
      case value(config, :recipients) do
        recipients when is_list(recipients) ->
          if Enum.all?(recipients, &(is_binary(&1) and &1 != "" and safe_header?(&1))),
            do: [],
            else: [:recipients]

        _ ->
          []
      end

    auth_fields =
      cond do
        is_binary(value(config, :username)) and not is_binary(value(config, :password)) ->
          [:password]

        is_binary(value(config, :password)) and not is_binary(value(config, :username)) ->
          [:username]

        true ->
          []
      end

    invalid_fields =
      Enum.flat_map([:server, :sender, :subject], fn field ->
        case value(config, field) do
          value when is_binary(value) -> if safe_header?(value), do: [], else: [field]
          _ -> []
        end
      end)

    type_fields =
      if(
        is_nil(value(config, :port)) or
          (is_integer(value(config, :port)) and value(config, :port) in 1..65_535),
        do: [],
        else: [:port]
      ) ++
        if(
          is_nil(value(config, :timeout)) or
            (is_integer(value(config, :timeout)) and value(config, :timeout) > 0),
          do: [],
          else: [:timeout]
        ) ++
        if value(config, :tls) in [nil, false, true, :ssl], do: [], else: [:tls]

    fields = Enum.uniq(missing ++ invalid ++ auth_fields ++ invalid_fields ++ type_fields)
    if fields == [], do: :ok, else: {:error, fields}
  end

  def validate_config(_), do: {:error, [:configuration]}

  @impl true
  def deliver(alert, config) when is_map(config) do
    with :ok <- validate_config_result(config),
         {:ok, socket} <- connect(config),
         {:ok, socket} <- expect(socket, 220),
         {:ok, socket} <- command(socket, "EHLO autoangel", :success),
         {:ok, socket} <- authenticate(socket, config),
         {:ok, socket} <- command(socket, "MAIL FROM:<#{value(config, :sender)}>", :success),
         {:ok, socket} <- recipients(socket, value(config, :recipients)),
         {:ok, socket} <- command(socket, "DATA", 354),
         {:ok, socket} <- send_data(socket, message(alert, config)) do
      _ = command(socket, "QUIT", 221)
      :ok
    else
      {:error, outcome} -> outcome
    end
  end

  @doc "Maps an SMTP response code to the plugin delivery outcome."
  @spec classify_response(non_neg_integer()) :: :ok | :retryable | :permanent
  def classify_response(code) when code in 200..399, do: :ok
  def classify_response(code) when code in 400..499, do: :retryable
  def classify_response(535), do: :permanent
  def classify_response(code) when code in 500..599, do: :permanent
  def classify_response(_code), do: :retryable

  defp validate_config_result(config) do
    case validate_config(config) do
      :ok -> :ok
      {:error, fields} -> {:error, {:permanent, {:invalid_configuration, fields}}}
    end
  end

  defp connect(config) do
    host = String.to_charlist(value(config, :server))
    port = value(config, :port) || @default_port
    timeout = value(config, :timeout) || @default_timeout
    options = [:binary, packet: :line, active: false]

    result =
      if value(config, :tls) in [true, :ssl] do
        :ssl.connect(
          host,
          port,
          options ++
            [verify: :verify_peer, cacerts: :public_key.cacerts_get(), server_name_indication: host],
          timeout
        )
      else
        :gen_tcp.connect(host, port, options, timeout)
      end

    case result do
      {:ok, socket} ->
        {:ok, if(value(config, :tls) in [true, :ssl], do: {:ssl, socket}, else: {:tcp, socket})}

      {:error, reason} ->
        {:error, {:retryable, {:connection, reason}}}
    end
  end

  defp authenticate(socket, config) do
    username = value(config, :username)
    password = value(config, :password)

    if is_binary(username) and is_binary(password) do
      case socket do
        {:tcp, _socket} ->
          {:error, {:permanent, :credentials_require_tls}}

        _ ->
          with {:ok, socket} <- command(socket, "AUTH LOGIN", 334),
               {:ok, socket} <- command(socket, Base.encode64(username), 334) do
            command(socket, Base.encode64(password), 235)
          end
      end
    else
      {:ok, socket}
    end
  end

  defp recipients(socket, recipients) do
    results = Enum.map(recipients, &command(socket, "RCPT TO:<#{&1}>", :success))

    cond do
      Enum.any?(results, &match?({:ok, _}, &1)) ->
        {:ok, socket}

      Enum.any?(results, &match?({:error, {:retryable, _}}, &1)) ->
        {:error, {:retryable, :recipients_unavailable}}

      true ->
        {:error, {:permanent, :recipients_rejected}}
    end
  end

  defp send_data(socket, body) do
    case send_packet(socket, dot_stuff(body) <> ".\r\n") do
      :ok -> expect(socket, 250)
      {:error, reason} -> {:error, {:retryable, {:send, reason}}}
    end
  end

  defp dot_stuff(body) do
    body
    |> String.replace("\r\n", "\n")
    |> String.replace("\r", "\n")
    |> String.split("\n", trim: false)
    |> Enum.map_join("\r\n", fn
      <<".", _rest::binary>> = line -> "." <> line
      line -> line
    end)
  end

  defp command(socket, text, expected) do
    case send_packet(socket, text <> "\r\n") do
      :ok -> expect(socket, expected)
      {:error, reason} -> {:error, {:retryable, {:send, reason}}}
    end
  end

  defp send_packet(socket, data) do
    case transport_send(socket, data) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp expect(socket, expected) do
    case response(socket) do
      {:ok, code} when expected == :success and code in 200..299 -> {:ok, socket}
      {:ok, code} when code == expected -> {:ok, socket}
      {:ok, code} when code in 200..399 -> {:error, {:permanent, {:unexpected_reply, code}}}
      {:ok, code} -> {:error, {classify_response(code), {:smtp, code}}}
      {:error, reason} -> {:error, {:retryable, {:receive, reason}}}
    end
  end

  defp response(socket), do: response(socket, nil)

  defp response(socket, code) do
    case transport_recv(socket) do
      {:ok, line} ->
        parsed = parse_code(line)

        if String.length(line) >= 4 and binary_part(line, 3, 1) == "-",
          do: response(socket, parsed),
          else: {:ok, parsed || code}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp parse_code(<<a, b, c, _rest::binary>>) when a in ?0..?9 and b in ?0..?9 and c in ?0..?9,
    do: String.to_integer(<<a, b, c>>)

  defp parse_code(_), do: nil

  defp transport_send({:ssl, socket}, data), do: :ssl.send(socket, data)
  defp transport_send({:tcp, socket}, data), do: :gen_tcp.send(socket, data)
  defp transport_recv({:ssl, socket}), do: :ssl.recv(socket, 0, @default_timeout)
  defp transport_recv({:tcp, socket}), do: :gen_tcp.recv(socket, 0, @default_timeout)

  defp message(alert, config) do
    sender = value(config, :sender)
    recipients = value(config, :recipients) |> Enum.join(", ")
    subject = value(config, :subject) || "Autoangel anomaly alert"

    [
      "From: ",
      sender,
      "\r\n",
      "To: ",
      recipients,
      "\r\n",
      "Subject: ",
      subject,
      "\r\n",
      "Content-Type: text/plain; charset=utf-8\r\n\r\n",
      format_alert(alert)
    ]
    |> IO.iodata_to_binary()
  end

  defp format_alert(alert) do
    range = get(alert, :range, %{})
    lower = get(range, :lower, get(alert, :lower, "unknown"))
    upper = get(range, :upper, get(alert, :upper, "unknown"))

    start_time =
      format_time(
        get(alert, :start_ms, get(alert, :start_time, get(alert, :started_at, "unknown")))
      )

    candidates = format_candidates(get(alert, :candidates, []))

    [
      {"metric", get(alert, :metric_name, get(alert, :name, "unknown"))},
      {"labels", format_labels(get(alert, :labels, %{}))},
      {"observed", get(alert, :observed, get(alert, :observed_value, "unknown"))},
      {"range_lower", lower},
      {"range_upper", upper},
      {"score", get(alert, :score, "unknown")},
      {"start_time", start_time},
      {"candidates", candidates}
    ]
    |> Enum.map_join("\r\n", fn {name, value} -> "#{name}: #{value}" end)
    |> then(&(&1 <> "\r\n"))
  end

  defp format_labels(labels) when is_map(labels),
    do: Enum.map_join(labels, ", ", fn {key, value} -> "#{key}=#{value}" end)

  defp format_labels(labels), do: to_string(labels)

  defp format_candidates([]), do: "none found inside the Correlation_Window"

  defp format_candidates(candidates) when is_list(candidates) do
    Enum.map_join(candidates, "; ", fn candidate ->
      name = get(candidate, :metric_name, get(candidate, :name, "unknown"))
      labels = format_labels(get(candidate, :labels, %{}))
      rank = get(candidate, :rank, "unknown")

      evidence =
        if get(candidate, :weak_evidence?, get(candidate, :weak_evidence, false)),
          do: " (weak evidence)",
          else: ""

      "rank #{rank}: #{name}{#{labels}}#{evidence}"
    end)
  end

  defp format_candidates(candidates), do: to_string(candidates)

  defp format_time(value) when is_integer(value) do
    case DateTime.from_unix(value, :millisecond) do
      {:ok, datetime} -> DateTime.to_iso8601(datetime)
      _ -> to_string(value)
    end
  end

  defp format_time(value), do: to_string(value)

  defp get(nil, _key, default), do: default

  defp get(map, key, default) when is_map(map) do
    case Map.get(map, key) do
      nil -> Map.get(map, Atom.to_string(key), default)
      value -> value
    end
  end

  defp get(_value, _key, default), do: default

  defp safe_header?(value), do: not String.contains?(value, ["\r", "\n", "<", ">"])

  defp value(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
end

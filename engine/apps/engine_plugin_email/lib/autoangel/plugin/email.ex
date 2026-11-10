defmodule Autoangel.Plugin.Email do
  @moduledoc "SMTP alert plugin."
  @behaviour Autoangel.Plugin

  alias Autoangel.Alert.Render

  @default_port 587
  @default_timeout 10_000

  @impl true
  def validate_config(config) when is_map(config) do
    fields =
      Enum.uniq(
        missing_fields(config) ++
          invalid_recipient_fields(config) ++
          auth_pair_fields(config) ++
          header_safe_fields(config) ++
          type_fields(config)
      )

    if fields == [], do: :ok, else: {:error, fields}
  end

  def validate_config(_), do: {:error, [:configuration]}

  defp missing_fields(config) do
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
  end

  defp invalid_recipient_fields(config) do
    case value(config, :recipients) do
      recipients when is_list(recipients) ->
        if Enum.all?(recipients, &(is_binary(&1) and &1 != "" and safe_header?(&1))),
          do: [],
          else: [:recipients]

      _ ->
        []
    end
  end

  defp auth_pair_fields(config) do
    username = value(config, :username)
    password = value(config, :password)

    cond do
      is_binary(username) and not is_binary(password) -> [:password]
      is_binary(password) and not is_binary(username) -> [:username]
      true -> []
    end
  end

  defp header_safe_fields(config) do
    Enum.flat_map([:server, :sender, :subject], &header_field_errors(&1, value(config, &1)))
  end

  defp header_field_errors(field, value) when is_binary(value) do
    if safe_header?(value), do: [], else: [field]
  end

  defp header_field_errors(_field, _value), do: []

  defp type_fields(config) do
    port_field(value(config, :port)) ++
      timeout_field(value(config, :timeout)) ++
      tls_field(value(config, :tls))
  end

  defp port_field(nil), do: []
  defp port_field(port) when is_integer(port) and port in 1..65_535, do: []
  defp port_field(_port), do: [:port]

  defp timeout_field(nil), do: []
  defp timeout_field(timeout) when is_integer(timeout) and timeout > 0, do: []
  defp timeout_field(_timeout), do: [:timeout]

  defp tls_field(tls) when tls in [nil, false, true, :ssl], do: []
  defp tls_field(_tls), do: [:tls]

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

    if is_binary(username) and is_binary(password),
      do: authenticate_login(socket, username, password),
      else: {:ok, socket}
  end

  defp authenticate_login({:tcp, _socket}, _username, _password),
    do: {:error, {:permanent, :credentials_require_tls}}

  defp authenticate_login(socket, username, password) do
    with {:ok, socket} <- command(socket, "AUTH LOGIN", 334),
         {:ok, socket} <- command(socket, Base.encode64(username), 334) do
      command(socket, Base.encode64(password), 235)
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
    alert
    |> Render.text()
    |> String.replace("\n", "\r\n")
    |> Kernel.<>("\r\n")
  end

  defp safe_header?(value), do: not String.contains?(value, ["\r", "\n", "<", ">"])

  defp value(map, key), do: Map.get(map, key, Map.get(map, Atom.to_string(key)))
end

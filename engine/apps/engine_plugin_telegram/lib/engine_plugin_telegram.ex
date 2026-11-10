defmodule EnginePluginTelegram do
  @moduledoc """
  Telegram Bot API plugin for Autoangel alerts.

  The plugin sends one `sendMessage` request per delivery attempt. Network
  failures, timeouts, rate limits and server errors are retryable; invalid bot
  credentials and invalid chats are permanent.
  """

  @behaviour Autoangel.Plugin

  alias Autoangel.Alert.Render

  @default_api_base "https://api.telegram.org"
  @default_timeout 10_000

  @impl true
  @doc "Validates the bot token and destination chat id without exposing secrets."
  def validate_config(config) when is_map(config) do
    missing =
      []
      |> missing_field(config, :bot_token)
      |> missing_field(config, :chat_id)

    if missing == [], do: :ok, else: {:error, missing}
  end

  def validate_config(_), do: {:error, [:bot_token, :chat_id]}

  @impl true
  @doc "Sends an alert through Telegram's Bot API and returns the Plugin outcome."
  def deliver(alert, config) when is_map(config) do
    case validate_config(config) do
      :ok ->
        config
        |> request(alert)
        |> classify_response()

      {:error, fields} ->
        {:permanent, {:invalid_config, fields}}
    end
  end

  def deliver(_alert, _config), do: {:permanent, {:invalid_config, [:bot_token, :chat_id]}}

  @doc "Classifies a Telegram HTTP status according to the delivery contract."
  @spec classify_status(non_neg_integer()) :: :ok | :retryable | :permanent
  def classify_status(status) when status in 200..299, do: :ok
  def classify_status(408), do: :retryable
  def classify_status(429), do: :retryable
  def classify_status(status) when status >= 500, do: :retryable
  def classify_status(status) when status in [400, 401, 403], do: :permanent
  def classify_status(status) when status in 300..499, do: :permanent
  def classify_status(_status), do: :retryable

  defp request(config, alert) do
    url =
      config
      |> Map.get(:api_base_url, @default_api_base)
      |> String.trim_trailing("/")
      |> Kernel.<>("/bot" <> value(config[:bot_token]) <> "/sendMessage")

    body =
      %{chat_id: value(config[:chat_id]), text: render_alert(alert)}
      |> maybe_add_buttons(alert)
      |> encode_object()

    headers = [{~c"content-type", ~c"application/json"}]
    timeout = Map.get(config, :timeout, @default_timeout)

    case Map.get(config, :http_client) do
      client when is_function(client, 2) ->
        client.(url, body)

      _ ->
        :inets.start()

        :httpc.request(
          :post,
          {String.to_charlist(url), headers, ~c"application/json", body},
          [timeout: timeout],
          []
        )
    end
  end

  defp classify_response({status, _headers, _body}) when is_integer(status) do
    case classify_status(status) do
      :ok -> :ok
      :retryable -> {:retryable, {:http_status, status}}
      :permanent -> {:permanent, {:http_status, status}}
    end
  end

  defp classify_response({:ok, {{_version, status, _reason}, headers, body}}),
    do: classify_response({status, headers, body})

  defp classify_response({:error, reason}), do: {:retryable, {:http_error, reason}}
  defp classify_response(_), do: {:retryable, :invalid_http_response}

  defp render_alert(alert) when is_map(alert), do: truncate(Render.text(alert))

  defp truncate(text), do: String.slice(text, 0, 4096)

  defp maybe_add_buttons(body, alert) do
    incident_id =
      Map.get(alert, :incident_id) || Map.get(alert, "incident_id") ||
        Map.get(alert, :anomaly_id) || Map.get(alert, "anomaly_id")

    if is_nil(incident_id) do
      body
    else
      Map.put(body, :reply_markup, %{
        inline_keyboard: [
          [
            %{text: "mute", callback_data: "m:#{incident_id}"},
            %{text: "this was normal", callback_data: "n:#{incident_id}"}
          ]
        ]
      })
    end
  end

  defp missing_field(fields, config, :bot_token) do
    token = Map.get(config, :bot_token)
    if valid_token?(token), do: fields, else: fields ++ [:bot_token]
  end

  defp missing_field(fields, config, :chat_id) do
    chat_id = Map.get(config, :chat_id)
    if valid_chat_id?(chat_id), do: fields, else: fields ++ [:chat_id]
  end

  defp valid_token?(%Autoangel.Secret{value: token}), do: valid_token?(token)
  defp valid_token?(token) when is_binary(token), do: String.trim(token) != ""
  defp valid_token?(_), do: false

  defp valid_chat_id?(chat_id) when is_integer(chat_id), do: true
  defp valid_chat_id?(chat_id) when is_binary(chat_id), do: String.trim(chat_id) != ""
  defp valid_chat_id?(_), do: false

  defp value(%Autoangel.Secret{} = secret), do: Autoangel.Secret.unwrap(secret)
  defp value(value), do: to_string(value)

  defp encode_object(map) do
    entries =
      Enum.map_join(map, ",", fn {key, value} ->
        encode_string(to_string(key)) <> ":" <> encode_value(value)
      end)

    "{" <> entries <> "}"
  end

  defp encode_value(nil), do: "null"
  defp encode_value(value) when is_binary(value), do: encode_string(value)
  defp encode_value(value) when is_atom(value), do: encode_string(to_string(value))
  defp encode_value(value) when is_number(value), do: to_string(value)
  defp encode_value(value) when is_map(value), do: encode_object(value)

  defp encode_value(value) when is_list(value),
    do: "[" <> Enum.map_join(value, ",", &encode_value/1) <> "]"

  defp encode_string(value) do
    escaped =
      value
      |> :unicode.characters_to_list()
      |> Enum.map_join(fn
        ?" -> ~s(\\\")
        ?\\ -> ~s(\\\\)
        ?\n -> ~s(\\n)
        ?\r -> ~s(\\r)
        ?\t -> ~s(\\t)
        char when char < 32 -> "\\u" <> String.pad_leading(Integer.to_string(char, 16), 4, "0")
        char -> <<char::utf8>>
      end)

    "\"" <> escaped <> "\""
  end
end

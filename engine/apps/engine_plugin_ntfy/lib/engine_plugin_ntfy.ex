defmodule EnginePluginNtfy do
  @moduledoc """
  [ntfy](https://ntfy.sh) plugin: one `POST` of the alert text to `server/topic`.

  Settings: `topic` (required; letters, digits, `_` and `-`, up to 64), `server`
  (default `https://ntfy.sh`), `token` (optional access token, sent as a bearer
  token and kept secret), `priority` (optional, 1 to 5) and `timeout` in
  milliseconds. The title is the metric name, or "resolved: <name>" for a
  resolution.

  A 2xx reply is delivered; 408, 425, 429, 5xx and network errors are retryable;
  any other reply (a wrong topic or token) is permanent.
  """

  @behaviour Autoangel.Plugin

  alias Autoangel.Alert.Render
  alias Autoangel.Plugin.Http

  @default_server "https://ntfy.sh"

  @impl true
  def validate_config(config) when is_map(config) do
    Http.failed(
      topic: valid_topic?(Map.get(config, :topic)),
      server: Http.valid_url?(Map.get(config, :server, @default_server)),
      token: valid_optional_token?(Map.get(config, :token)),
      priority: Map.get(config, :priority, 3) in 1..5
    )
  end

  def validate_config(_config), do: {:error, [:topic]}

  @impl true
  def deliver(alert, config) when is_map(config) do
    case validate_config(config) do
      :ok ->
        Http.post(
          url(config),
          headers(alert, config),
          Http.truncate(Render.text(alert), 4096),
          config
        )

      {:error, fields} ->
        {:permanent, {:invalid_config, fields}}
    end
  end

  def deliver(_alert, _config), do: {:permanent, {:invalid_config, [:topic]}}

  defp url(config) do
    server = config |> Map.get(:server, @default_server) |> String.trim_trailing("/")
    server <> "/" <> config.topic
  end

  defp headers(alert, config) do
    title = Map.get(alert, :name) || "alert"
    resolved? = Map.get(alert, :resolved?) == true

    [
      {"content-type", "text/plain; charset=utf-8"},
      {"title", header_text(if(resolved?, do: "resolved: #{title}", else: to_string(title)))},
      {"priority", Integer.to_string(Map.get(config, :priority, 3))},
      {"tags", if(resolved?, do: "white_check_mark", else: "warning")}
    ] ++ auth(config)
  end

  defp auth(config) do
    case Http.unwrap(Map.get(config, :token)) do
      token when is_binary(token) and token != "" -> [{"authorization", "Bearer " <> token}]
      _none -> []
    end
  end

  # Header values cannot hold line breaks or non-ASCII text.
  defp header_text(text),
    do: text |> String.replace(~r/[^\x20-\x7E]/, "?") |> Http.truncate(200)

  defp valid_topic?(topic),
    do: is_binary(topic) and Regex.match?(~r/\A[A-Za-z0-9_-]{1,64}\z/, topic)

  defp valid_optional_token?(nil), do: true
  defp valid_optional_token?(token), do: is_binary(Http.unwrap(token)) and Http.unwrap(token) != ""
end

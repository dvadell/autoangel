defmodule EnginePluginDiscord do
  @moduledoc """
  Discord plugin: one `POST` to a channel webhook URL.

  Settings: `webhook_url` (required, https; the URL is the credential, so it is
  never logged) and `timeout` in milliseconds. The message is the alert text in a
  code block with mentions switched off (`allowed_mentions` is empty).

  A 2xx reply (Discord answers 204) is delivered; 408, 425, 429, 5xx and network
  errors are retryable; any other reply is permanent.
  """

  @behaviour Autoangel.Plugin

  alias Autoangel.Alert.Render
  alias Autoangel.Plugin.Http

  # Discord refuses a message over 2,000 characters.
  @max_content 2_000
  @fence_overhead 8

  @impl true
  def validate_config(config) when is_map(config) do
    Http.failed(webhook_url: Http.valid_url?(Map.get(config, :webhook_url), schemes: ["https"]))
  end

  def validate_config(_config), do: {:error, [:webhook_url]}

  @impl true
  def deliver(alert, config) when is_map(config) do
    case validate_config(config) do
      :ok ->
        Http.post(
          Http.unwrap(config.webhook_url),
          [{"content-type", "application/json"}],
          body(alert),
          config
        )

      {:error, fields} ->
        {:permanent, {:invalid_config, fields}}
    end
  end

  def deliver(_alert, _config), do: {:permanent, {:invalid_config, [:webhook_url]}}

  @doc "The JSON body sent for an alert; the content is never over 2,000 characters."
  @spec body(map()) :: binary()
  def body(alert) do
    text =
      alert
      |> Render.text()
      |> String.replace("```", "'''")
      |> Http.truncate(@max_content - @fence_overhead)

    JSON.encode!(%{
      "content" => "```\n" <> text <> "\n```",
      "allowed_mentions" => %{"parse" => []}
    })
  end
end

defmodule EnginePluginSlack do
  @moduledoc """
  Slack plugin: one `POST` to an incoming-webhook URL.

  Settings: `webhook_url` (required, https; the URL is the credential, so it is
  never logged) and `timeout` in milliseconds. The message is the alert text in a
  code block, so label values cannot turn into mentions or links.

  A 2xx reply is delivered; 408, 425, 429, 5xx and network errors are retryable;
  any other reply (a revoked or wrong webhook) is permanent.
  """

  @behaviour Autoangel.Plugin

  alias Autoangel.Alert.Render
  alias Autoangel.Plugin.Http

  # Slack cuts messages near 40,000 characters.
  @max_text 3_000

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

  @doc "The JSON body sent for an alert."
  @spec body(map()) :: binary()
  def body(alert) do
    text = alert |> Render.text() |> String.replace("```", "'''") |> Http.truncate(@max_text)
    JSON.encode!(%{"text" => "```\n" <> text <> "\n```"})
  end
end

defmodule EnginePluginWebhook do
  @moduledoc """
  Generic webhook plugin: one JSON `POST` per alert to a configured URL.

  Settings: `url` (required, http or https; it may hold a secret, so it is never
  logged), `headers` (optional map of extra request headers, for a token) and
  `timeout` in milliseconds. The body is a JSON object with the alert text and its
  main fields (`text`, `name`, `labels`, `score`, `start_time`, `anomaly_id`,
  `incident_id`, `console_url`, `resolved`).

  A 2xx reply is delivered; 408, 425, 429, 5xx and network errors are retryable;
  any other reply is permanent.
  """

  @behaviour Autoangel.Plugin

  alias Autoangel.Alert.Render
  alias Autoangel.Plugin.Http

  @impl true
  def validate_config(config) when is_map(config) do
    Http.failed(
      url: Http.valid_url?(Map.get(config, :url)),
      headers: Map.get(config, :headers, %{}) |> Http.valid_headers?()
    )
  end

  def validate_config(_config), do: {:error, [:url]}

  @impl true
  def deliver(alert, config) when is_map(config) do
    case validate_config(config) do
      :ok ->
        headers = [{"content-type", "application/json"} | extra_headers(config)]
        Http.post(Http.unwrap(config.url), headers, body(alert), config)

      {:error, fields} ->
        {:permanent, {:invalid_config, fields}}
    end
  end

  def deliver(_alert, _config), do: {:permanent, {:invalid_config, [:url]}}

  @doc "The JSON body sent for an alert."
  @spec body(map()) :: binary()
  def body(alert) when is_map(alert) do
    %{
      "text" => Render.text(alert),
      "name" => field(alert, :name),
      "labels" => field(alert, :labels) || %{},
      "score" => field(alert, :score),
      "start_time" => field(alert, :start_time_iso),
      "anomaly_id" => field(alert, :anomaly_id),
      "incident_id" => field(alert, :incident_id),
      "console_url" => field(alert, :console_url),
      "resolved" => field(alert, :resolved?) == true
    }
    |> JSON.encode!()
  end

  defp extra_headers(config) do
    config
    |> Map.get(:headers, %{})
    |> Enum.map(fn {name, value} -> {name, Http.unwrap(value)} end)
  end

  defp field(alert, key), do: Map.get(alert, key)
end

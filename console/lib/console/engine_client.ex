defmodule Console.EngineClient do
  @moduledoc """
  The Console's only path to the Engine: its Control_API over HTTP. The Console
  never writes to the database; feedback goes through `POST /api/v1/feedback`.

  Settings (from `Console.Config`): `:engine_url` (no URL means feedback is off)
  and `:engine_token` (sent as a bearer token). Tests swap the module with
  `Application.put_env(:autoangel_console, :engine_client, Fake)`.
  """

  @callback ready?() :: boolean()
  @callback feedback(action :: String.t(), target :: map()) :: :ok | {:error, String.t()}

  @ui_actions %{"mute" => "mute", "this is normal" => "this was normal"}

  @doc "The configured implementation."
  @spec impl() :: module()
  def impl, do: Application.get_env(:autoangel_console, :engine_client, Console.EngineClient.HTTP)

  @doc "Maps a button label to the Control_API action, or `:error`."
  @spec api_action(String.t()) :: {:ok, String.t()} | :error
  def api_action(label), do: Map.fetch(@ui_actions, label)

  @doc "The Control_API target for a stored Series."
  @spec series_target(%{name: String.t(), labels: map()}) :: map()
  def series_target(series),
    do: %{"series" => %{"name" => series.name, "labels" => series.labels || %{}}}
end

defmodule Console.EngineClient.HTTP do
  @moduledoc "`Console.EngineClient` over `:httpc`, with short timeouts."
  @behaviour Console.EngineClient

  @timeout 3_000

  @impl true
  def ready? do
    case request(:get, "/readyz", nil) do
      {:ok, 200, _body} -> true
      _ -> false
    end
  end

  @impl true
  def feedback(action, target) do
    body = Jason.encode!(%{"action" => action, "target" => target})

    case request(:post, "/api/v1/feedback", body) do
      {:ok, 200, _body} -> :ok
      {:ok, status, body} -> {:error, reason(status, body)}
      {:error, :not_configured} -> {:error, "the Engine address is not configured"}
      {:error, _reason} -> {:error, "the Engine could not be reached"}
    end
  end

  defp reason(status, body) do
    case Jason.decode(body) do
      {:ok, %{"error" => "invalid_field", "field" => field}} -> "the Engine refused the #{field}"
      {:ok, %{"error" => error}} -> "the Engine answered #{status}: #{error}"
      _ -> "the Engine answered #{status}"
    end
  end

  defp request(method, path, body) do
    case Application.get_env(:autoangel_console, :engine_url) do
      nil -> {:error, :not_configured}
      base -> do_request(method, String.trim_trailing(base, "/") <> path, body)
    end
  end

  defp do_request(method, url, body) do
    headers =
      case Application.get_env(:autoangel_console, :engine_token) do
        nil -> []
        token -> [{~c"authorization", String.to_charlist("Bearer " <> token)}]
      end

    req =
      if method == :post,
        do: {String.to_charlist(url), headers, ~c"application/json", body},
        else: {String.to_charlist(url), headers}

    case :httpc.request(method, req, [timeout: @timeout, connect_timeout: @timeout],
           body_format: :binary
         ) do
      {:ok, {{_, status, _}, _headers, resp}} -> {:ok, status, resp}
      {:error, reason} -> {:error, reason}
    end
  end
end

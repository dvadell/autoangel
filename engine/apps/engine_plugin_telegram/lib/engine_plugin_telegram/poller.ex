defmodule EnginePluginTelegram.Poller do
  @moduledoc """
  Long-polls Telegram `getUpdates` for the "mute" and "this was normal" button
  presses and passes each one to `Autoangel.Feedback.apply/3`
  (Requirements 30.5 and 30.9).

  A button's callback data is `m:<incident>` or `n:<incident>`. Only presses from
  the configured chat count; anything else is ignored. Each press is answered with
  `answerCallbackQuery` so the button stops spinning. A failed poll is retried
  after a pause, and the bot token never appears in a log line.

  Options: `:config` (the plugin settings), `:feedback` (a function taking the
  action and the target, default `Autoangel.Feedback`), `:client` (a function
  taking the API method and a map and returning `{:ok, decoded}` or
  `{:error, reason}`; default is HTTPS to the Bot API), `:poll_timeout` in
  seconds (default 30), `:retry_ms` after a failure (default 5000) and `:name`.
  """

  use GenServer
  require Logger

  @default_api_base "https://api.telegram.org"
  @default_poll_timeout 30
  @default_retry_ms 5_000

  @actions %{"m" => "mute", "n" => "this was normal"}

  @spec child_spec(keyword() | map()) :: Supervisor.child_spec()
  def child_spec(%{} = config), do: child_spec(config: config)

  def child_spec(opts),
    do: %{id: __MODULE__, start: {__MODULE__, :start_link, [opts]}, restart: :permanent}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))

  @doc "Parses callback data: `{:ok, action, incident_id}` or `:error`."
  @spec parse_callback(term()) :: {:ok, String.t(), pos_integer()} | :error
  def parse_callback(data) when is_binary(data) do
    with [kind, id] <- String.split(data, ":", parts: 2),
         {:ok, action} <- Map.fetch(@actions, kind),
         {number, ""} when number > 0 <- Integer.parse(id) do
      {:ok, action, number}
    else
      _ -> :error
    end
  end

  def parse_callback(_data), do: :error

  @impl true
  def init(opts) do
    config = Keyword.fetch!(opts, :config)

    state = %{
      chat_id: chat_id(config),
      feedback: Keyword.get(opts, :feedback, &default_feedback/2),
      client: Keyword.get(opts, :client) || default_client(config),
      poll_timeout: Keyword.get(opts, :poll_timeout, @default_poll_timeout),
      retry_ms: Keyword.get(opts, :retry_ms, @default_retry_ms),
      offset: nil
    }

    send(self(), :poll)
    {:ok, state}
  end

  @impl true
  def handle_info(:poll, state) do
    case fetch(state) do
      {:ok, updates} ->
        state = Enum.reduce(updates, state, &handle_update/2)
        send(self(), :poll)
        {:noreply, state}

      {:error, reason} ->
        Logger.warning("telegram poll failed: #{inspect(sanitize(reason))}")
        Process.send_after(self(), :poll, state.retry_ms)
        {:noreply, state}
    end
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp fetch(state) do
    params = %{
      "timeout" => state.poll_timeout,
      "allowed_updates" => ["callback_query"]
    }

    params = if state.offset, do: Map.put(params, "offset", state.offset), else: params

    case state.client.("getUpdates", params) do
      {:ok, %{"ok" => true, "result" => updates}} when is_list(updates) -> {:ok, updates}
      {:ok, other} -> {:error, {:unexpected_response, Map.get(other, "error_code")}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp handle_update(%{"update_id" => id} = update, state) when is_integer(id) do
    state = %{state | offset: max(state.offset || 0, id + 1)}

    case update do
      %{"callback_query" => query} -> handle_callback(query, state)
      _other -> :ok
    end

    state
  end

  defp handle_update(_update, state), do: state

  defp handle_callback(%{"id" => query_id, "data" => data} = query, state) do
    with true <- from_configured_chat?(query, state.chat_id),
         {:ok, action, incident} <- parse_callback(data) do
      result = apply_feedback(state.feedback, action, %{"incident_id" => incident})
      answer(state, query_id, answer_text(result))
    else
      _ignored -> :ok
    end
  end

  defp handle_callback(_query, _state), do: :ok

  defp from_configured_chat?(query, chat_id) do
    case get_in(query, ["message", "chat", "id"]) do
      nil -> false
      id -> to_string(id) == chat_id
    end
  end

  defp apply_feedback(feedback, action, target) do
    feedback.(action, target)
  catch
    :exit, _reason -> {:error, :unavailable}
  end

  defp answer_text({:ok, _change}), do: "done"
  defp answer_text(:ok), do: "done"
  defp answer_text({:error, :invalid, _field}), do: "this alert is no longer open"
  defp answer_text(_other), do: "could not apply that, try again"

  defp answer(state, query_id, text) do
    _ = state.client.("answerCallbackQuery", %{"callback_query_id" => query_id, "text" => text})
    :ok
  end

  defp default_feedback(action, target),
    do: Autoangel.Feedback.apply(Autoangel.Feedback, action, target)

  defp chat_id(config) do
    config |> Map.get(:chat_id) |> to_string()
  end

  defp default_client(config) do
    base = config |> Map.get(:api_base_url, @default_api_base) |> String.trim_trailing("/")
    token = config |> Map.get(:bot_token) |> unwrap()

    fn method, params ->
      url = String.to_charlist("#{base}/bot#{token}/#{method}")
      body = JSON.encode!(params)
      timeout = (Map.get(params, "timeout", 0) + 10) * 1_000

      case :httpc.request(
             :post,
             {url, [], ~c"application/json", body},
             [timeout: timeout],
             body_format: :binary
           ) do
        {:ok, {{_, 200, _}, _headers, response}} -> JSON.decode(response) |> decoded()
        {:ok, {{_, status, _}, _headers, _response}} -> {:error, {:http_status, status}}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp decoded({:ok, map}), do: {:ok, map}
  defp decoded({:error, _}), do: {:error, :invalid_json}

  defp unwrap(%Autoangel.Secret{} = secret), do: Autoangel.Secret.unwrap(secret)
  defp unwrap(value), do: to_string(value)

  # Reasons from the HTTP client can carry the request URL, which holds the token.
  defp sanitize({:http_status, status}), do: {:http_status, status}
  defp sanitize(reason) when is_atom(reason), do: reason
  defp sanitize(_reason), do: :request_failed
end

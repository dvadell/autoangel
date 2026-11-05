defmodule Autoangel.Ingest.RateLimit do
  # credo:disable-for-this-file Credo.Check.Refactor.Nesting
  @moduledoc "Per-credential token-bucket rate limiting for the ingest endpoint."

  import Plug.Conn

  @default_rate 50
  @default_burst 200
  @default_table :autoangel_ingest_rate_limit
  @microtokens 1_000_000

  @doc "Initializes the ETS table used by the limiter."
  @spec init(keyword()) :: keyword()
  def init(opts) when is_list(opts) do
    ensure_table(Keyword.get(opts, :table, @default_table))
    opts
  end

  @doc "Consumes one token or halts the request with HTTP 429."
  @spec call(Plug.Conn.t(), keyword()) :: Plug.Conn.t()
  def call(conn, opts) when is_list(opts) do
    config = Keyword.get(opts, :config, Application.get_env(:autoangel_core, :config, %{}))
    {rate, burst} = limits(opts, config)
    table = Keyword.get(opts, :table, @default_table)
    ensure_table(table)

    case consume(table, credential_id(conn), now(opts), rate, burst) do
      :ok ->
        conn

      {:limited, retry_after} ->
        conn
        |> put_resp_header("retry-after", Integer.to_string(retry_after))
        |> send_resp(429, "rate limit exceeded")
        |> halt()
    end
  end

  defp limits(opts, config) do
    configured = Map.get(config, :ingest_rate_limit, %{})
    rate = Keyword.get(opts, :rate, Map.get(configured, :rate, @default_rate))
    burst = Keyword.get(opts, :burst, Map.get(configured, :burst, @default_burst))

    if is_number(rate) and rate > 0 and is_integer(burst) and burst > 0,
      do: {rate, burst},
      else: {@default_rate, @default_burst}
  end

  defp consume(table, credential, now, rate, burst) do
    rate_micro = max(1, trunc(rate * @microtokens))
    capacity = burst * @microtokens
    key = credential

    :global.trans({__MODULE__, {table, key}}, fn ->
      case :ets.lookup(table, key) do
        [] ->
          :ets.insert(table, {key, capacity - @microtokens, now})
          :ok

        [{^key, tokens, last}] ->
          available = min(capacity, tokens + div(max(0, now - last) * rate_micro, @microtokens))

          if available >= @microtokens do
            :ets.insert(table, {key, available - @microtokens, now})
            :ok
          else
            deficit = @microtokens - available
            {:limited, max(1, div(deficit + rate_micro - 1, rate_micro))}
          end
      end
    end)
  end

  defp credential_id(conn) do
    case conn.assigns[:credential_id] do
      nil ->
        authorization = get_req_header(conn, "authorization") |> List.first("anonymous")
        {:credential, :crypto.hash(:sha256, authorization)}

      id ->
        {:credential, id}
    end
  end

  defp now(opts) do
    case Keyword.get(opts, :clock) do
      clock when is_function(clock, 0) -> clock.()
      _ -> System.monotonic_time(:microsecond)
    end
  end

  defp ensure_table(table) do
    case :ets.whereis(table) do
      :undefined ->
        :global.trans({__MODULE__, table}, fn ->
          case :ets.whereis(table) do
            :undefined ->
              parent = self()

              spawn(fn ->
                :ets.new(table, [:named_table, :set, :public, read_concurrency: true])
                send(parent, {:rate_limit_table_ready, table})
                Process.sleep(:infinity)
              end)

              receive do
                {:rate_limit_table_ready, ^table} -> table
              after
                1_000 -> raise "unable to create rate-limit table"
              end

            _tid ->
              table
          end
        end)

      _tid ->
        table
    end
  end
end

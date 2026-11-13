defmodule AutoangelFaults.Toxiproxy do
  @moduledoc """
  Minimal Toxiproxy HTTP client for the `timescaledb` proxy that
  `scripts/fault_env.sh` creates. Connection details come from
  `AUTOANGEL_FAULT_PROXY_API` and `AUTOANGEL_FAULT_PROXY_PORT`.
  """

  @proxy "timescaledb"

  @spec available?() :: boolean()
  def available? do
    with api when is_binary(api) <- System.get_env("AUTOANGEL_FAULT_PROXY_API"),
         {:ok, {{_, 200, _}, _, _}} <- request(:get, "#{api}/version", nil) do
      true
    else
      _ -> false
    end
  end

  @doc "Port on 127.0.0.1 that forwards to TimescaleDB through the proxy."
  @spec port() :: pos_integer()
  def port, do: String.to_integer(System.fetch_env!("AUTOANGEL_FAULT_PROXY_PORT"))

  @doc "Cuts every connection and refuses new ones, like a stopped database."
  @spec down() :: :ok
  def down, do: set_enabled(false)

  @doc "Accepts connections again."
  @spec up() :: :ok
  def up, do: set_enabled(true)

  @doc "Adds `latency_ms` of delay to every reply from the database."
  @spec slow(pos_integer()) :: :ok
  def slow(latency_ms) do
    body = %{
      name: "slow",
      type: "latency",
      stream: "downstream",
      attributes: %{latency: latency_ms}
    }

    ok!(request(:post, "#{api()}/proxies/#{@proxy}/toxics", body))
  end

  @doc "Removes all added toxics and re-enables the proxy."
  @spec reset() :: :ok
  def reset, do: ok!(request(:post, "#{api()}/reset", %{}))

  defp set_enabled(enabled?),
    do: ok!(request(:post, "#{api()}/proxies/#{@proxy}", %{enabled: enabled?}))

  defp api, do: System.fetch_env!("AUTOANGEL_FAULT_PROXY_API")

  defp ok!({:ok, {{_, status, _}, _, _}}) when status in 200..299, do: :ok
  defp ok!(other), do: raise("Toxiproxy request failed: #{inspect(other)}")

  defp request(:get, url, _body),
    do: :httpc.request(:get, {String.to_charlist(url), []}, [timeout: 5_000], [])

  defp request(:post, url, body) do
    request = {String.to_charlist(url), [], ~c"application/json", Jason.encode!(body)}
    :httpc.request(:post, request, [timeout: 5_000], [])
  end
end

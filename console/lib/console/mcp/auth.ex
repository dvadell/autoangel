defmodule Console.MCP.Auth do
  @moduledoc """
  Authentication for `/mcp` (Requirements 64.1 to 64.3, 74.1, 74.2, 74.7).

  Every request needs `Authorization: Bearer <token>` with the MCP credential
  (`AUTOANGEL_CONSOLE_MCP_TOKEN`). That setting is separate from the web
  credentials: a web basic login or web token never opens `/mcp`, and the MCP
  token is never read by `Console.Auth`, so it does not open a web route.
  With no MCP token configured, every request is refused.

  A refused request gets a bare 401 with no body, so nothing about the data
  leaks, and it never reaches `Console.MCP.Tools`.
  """
  @behaviour Plug

  import Plug.Conn

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(conn, _opts) do
    if authorized?(conn), do: conn, else: deny(conn)
  end

  @doc "True when the request carries the configured MCP credential."
  @spec authorized?(Plug.Conn.t()) :: boolean()
  def authorized?(conn) do
    case Application.get_env(:autoangel_console, :mcp_token) do
      token when is_binary(token) and token != "" ->
        # Every header is compared, so timing does not reveal which one matched.
        conn
        |> get_req_header("authorization")
        |> Enum.reduce(false, fn header, found -> matches?(header, token) or found end)

      _none ->
        false
    end
  end

  defp matches?("Bearer " <> presented, token), do: Plug.Crypto.secure_compare(presented, token)
  defp matches?(_header, _token), do: false

  defp deny(conn) do
    conn
    |> put_resp_header("www-authenticate", ~s(Bearer realm="Autoangel Console MCP"))
    |> send_resp(401, "")
    |> halt()
  end
end

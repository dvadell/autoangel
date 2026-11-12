defmodule Console.Auth do
  @moduledoc """
  Static-credential authentication for pages, LiveView sockets and `/healthz`.

  Accepts either an `Authorization: Basic ...` header or an
  `Authorization: Bearer ...` header that matches the configured credentials
  (`Console.Config`). A request that fails gets a bare 401: no body, no metric
  data, and the request never reaches a controller or the Control_API.

  A success stores a flag in the signed session cookie; `on_mount/4` checks that
  flag so a LiveView socket cannot be opened without an authenticated page load.
  """
  @behaviour Plug

  import Plug.Conn

  @session_key "console_authenticated"
  @realm ~s(Basic realm="Autoangel Console", charset="UTF-8")

  @impl Plug
  def init(opts), do: opts

  @impl Plug
  def call(conn, _opts) do
    if authorized?(conn) do
      conn
      |> fetch_session()
      |> put_session(@session_key, true)
    else
      deny(conn)
    end
  end

  @doc "True when the request carries credentials that match the configuration."
  @spec authorized?(Plug.Conn.t()) :: boolean()
  def authorized?(conn) do
    credentials = Application.get_env(:autoangel_console, :auth, [])

    conn
    |> get_req_header("authorization")
    |> Enum.any?(&header_matches?(&1, credentials))
  end

  @doc "LiveView hook: only sessions that passed `call/2` may mount."
  def on_mount(:require_auth, _params, session, socket) do
    if session[@session_key] == true,
      do: {:cont, socket},
      else: {:halt, Phoenix.LiveView.redirect(socket, to: "/")}
  end

  defp deny(conn) do
    conn
    |> put_resp_header("www-authenticate", @realm)
    |> send_resp(401, "")
    |> halt()
  end

  defp header_matches?("Basic " <> encoded, credentials) do
    with {:ok, decoded} <- Base.decode64(String.trim(encoded)),
         [user, password] <- String.split(decoded, ":", parts: 2) do
      Enum.reduce(credentials, false, fn
        {:basic, expected_user, expected_password}, found ->
          # Both comparisons always run so timing does not reveal which part was wrong.
          user_ok = Plug.Crypto.secure_compare(user, expected_user)
          password_ok = Plug.Crypto.secure_compare(password, expected_password)
          (user_ok and password_ok) or found

        _other, found ->
          found
      end)
    else
      _ -> false
    end
  end

  defp header_matches?("Bearer " <> token, credentials) do
    Enum.reduce(credentials, false, fn
      {:bearer, expected}, found -> Plug.Crypto.secure_compare(token, expected) or found
      _other, found -> found
    end)
  end

  defp header_matches?(_header, _credentials), do: false
end

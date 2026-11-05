defmodule Autoangel.HTTP.Control do
  @moduledoc "Plug entrypoint for the authenticated Control API listener."

  use Plug.Builder

  plug(Autoangel.Auth, role: :control_api)

  plug(:not_implemented)

  defp not_implemented(conn, _opts) do
    Plug.Conn.send_resp(conn, 404, "not found")
  end
end

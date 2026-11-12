defmodule Console.Router do
  @moduledoc "Routes. Everything except an explicitly exempted `/healthz` requires authentication."
  use Phoenix.Router

  import Plug.Conn

  pipeline :protected do
    plug(:fetch_session)
    plug(Console.Auth)
  end

  pipeline :health do
    plug(:authenticate_unless_exempt)
  end

  scope "/", Console do
    pipe_through(:health)
    get("/healthz", HealthController, :show)
  end

  scope "/", Console do
    pipe_through(:protected)
    get("/", PageController, :index)
  end

  @doc false
  def authenticate_unless_exempt(conn, _opts) do
    if Application.get_env(:autoangel_console, :health_exempt, false),
      do: conn,
      else: Console.Auth.call(conn, [])
  end
end

defmodule Console.HealthController do
  @moduledoc "Liveness only: the body is a single `status` key set to `ok`."
  use Phoenix.Controller, formats: [:json]

  def show(conn, _params), do: json(conn, %{status: "ok"})
end

defmodule Console.PageController do
  @moduledoc "Placeholder landing page; the LiveView pages arrive with task 27."
  use Phoenix.Controller, formats: [:html]

  def index(conn, _params) do
    html(
      conn,
      "<!DOCTYPE html><html lang=\"en\"><title>Autoangel Console</title><h1>Autoangel Console</h1></html>"
    )
  end
end

defmodule Console.Endpoint do
  @moduledoc "The Console HTTP endpoint (Bandit), with LiveView socket and signed session cookie."
  use Phoenix.Endpoint, otp_app: :autoangel_console

  @session_options [
    store: :cookie,
    key: "_autoangel_console",
    signing_salt: "console-session",
    same_site: "Strict",
    http_only: true
  ]

  socket("/live", Phoenix.LiveView.Socket, websocket: [connect_info: [session: @session_options]])

  plug(Plug.RequestId)
  plug(Plug.Head)
  plug(Plug.Session, @session_options)
  plug(Console.Router)
end

defmodule Console.ErrorView do
  @moduledoc "Error responses carry only the HTTP status text: never a stack trace or data."

  def render(template, _assigns) do
    status = template |> String.split(".") |> hd()

    if String.ends_with?(template, ".json"),
      do: %{error: Phoenix.Controller.status_message_from_template(template)},
      else: "#{status} #{Phoenix.Controller.status_message_from_template(template)}"
  end
end

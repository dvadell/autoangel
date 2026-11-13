defmodule Console.Router do
  @moduledoc "Routes. Everything except an explicitly exempted `/healthz` requires authentication."
  use Phoenix.Router

  import Plug.Conn
  import Phoenix.Controller
  import Phoenix.LiveView.Router

  pipeline :protected do
    plug(:accepts, ["html"])
    plug(:fetch_session)
    plug(Console.Auth)
    plug(:fetch_live_flash)
    plug(:put_root_layout, html: {Console.Layouts, :root})
    plug(:protect_from_forgery)
    plug(:put_secure_browser_headers)
  end

  pipeline :mcp do
    plug(Console.MCP.Auth)
  end

  pipeline :health do
    plug(:authenticate_unless_exempt)
  end

  scope "/", Console do
    pipe_through(:health)
    get("/healthz", HealthController, :show)
  end

  scope "/", Console do
    pipe_through(:mcp)
    match(:*, "/mcp", MCP.Controller, :handle)
  end

  scope "/", Console do
    pipe_through(:protected)

    live_session :console, on_mount: [{Console.Auth, :require_auth}, Console.Live.EngineStatus] do
      live("/", AnomalyListLive)
      live("/series", SeriesLive.Index)
      live("/series/:id", SeriesLive.Show)
      live("/anomalies/:id", AnomalyLive.Show)
    end
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

  # LiveView JavaScript, straight from the deps' priv/static (no asset build).
  plug(Plug.Static,
    at: "/assets/phoenix",
    from: {:phoenix, "priv/static"},
    only: ~w(phoenix.min.js)
  )

  plug(Plug.Static,
    at: "/assets/live",
    from: {:phoenix_live_view, "priv/static"},
    only: ~w(phoenix_live_view.min.js)
  )

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

defmodule Console.AuthTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  @opts Console.Router.init([])
  @session_opts Plug.Session.init(
                  store: :cookie,
                  key: "_t",
                  signing_salt: "console-session"
                )

  setup do
    previous = Application.get_all_env(:autoangel_console)
    Application.put_env(:autoangel_console, :auth, [{:basic, "ops", "pw"}, {:bearer, "tok"}])
    Application.put_env(:autoangel_console, :health_exempt, false)

    on_exit(fn ->
      for {key, value} <- previous, do: Application.put_env(:autoangel_console, key, value)
    end)
  end

  defp request(path, headers \\ []) do
    conn =
      Enum.reduce(headers, conn(:get, path), fn {k, v}, acc -> put_req_header(acc, k, v) end)

    conn = %{conn | secret_key_base: String.duplicate("k", 64)}
    conn = Plug.Session.call(conn, @session_opts)
    Console.Router.call(conn, @opts)
  end

  defp basic(user, pass), do: {"authorization", "Basic " <> Base.encode64("#{user}:#{pass}")}

  test "no credentials: 401 with no body" do
    conn = request("/")
    assert conn.status == 401
    assert conn.resp_body == ""
    assert get_resp_header(conn, "www-authenticate") != []
  end

  test "wrong credentials are denied and return no page content" do
    for header <- [
          basic("ops", "wrong"),
          basic("other", "pw"),
          {"authorization", "Bearer nope"},
          {"authorization", "Basic !!!not-base64"},
          {"authorization", "Digest abc"}
        ] do
      conn = request("/", [header])
      assert conn.status == 401
      assert conn.resp_body == ""
    end
  end

  test "valid basic credentials and bearer token are accepted" do
    assert request("/", [basic("ops", "pw")]).status == 200
    assert request("/", [{"authorization", "Bearer tok"}]).status == 200
  end

  test "a password containing a colon works" do
    Application.put_env(:autoangel_console, :auth, [{:basic, "ops", "a:b"}])
    assert request("/", [basic("ops", "a:b")]).status == 200
  end

  test "/healthz needs authentication by default and returns only status ok" do
    assert request("/healthz").status == 401

    conn = request("/healthz", [basic("ops", "pw")])
    assert conn.status == 200
    assert Jason.decode!(conn.resp_body) == %{"status" => "ok"}
  end

  test "/healthz can be explicitly exempted, other routes stay protected" do
    Application.put_env(:autoangel_console, :health_exempt, true)

    conn = request("/healthz")
    assert conn.status == 200
    assert Jason.decode!(conn.resp_body) == %{"status" => "ok"}
    assert request("/").status == 401
  end

  test "an authenticated request marks the session; a socket without it is refused" do
    conn = request("/", [basic("ops", "pw")])
    assert get_session(conn, "console_authenticated") == true

    socket = %Phoenix.LiveView.Socket{}

    assert {:cont, _} =
             Console.Auth.on_mount(:require_auth, %{}, %{"console_authenticated" => true}, socket)

    assert {:halt, _} = Console.Auth.on_mount(:require_auth, %{}, %{}, socket)
  end
end

defmodule Autoangel.AuthTest do
  use ExUnit.Case, async: true

  import Plug.Conn

  alias Autoangel.Auth
  alias Autoangel.Secret

  @config %{
    ingest: %{
      token: Secret.new("ingest-token"),
      username: "ingest-user",
      password: Secret.new("ingest-pass")
    },
    control_api: %{
      token: Secret.new("control-token"),
      username: "control-user",
      password: Secret.new("control-pass")
    },
    auth_exemptions: ["/healthz", "/readyz"]
  }

  test "accepts bearer and basic credentials only for their own role" do
    assert authorized?("Bearer ingest-token", :ingest)
    assert authorized?(basic("ingest-user", "ingest-pass"), :ingest)
    refute authorized?("Bearer control-token", :ingest)
    refute authorized?(basic("control-user", "control-pass"), :ingest)
    assert authorized?("Bearer control-token", :control_api)
    assert authorized?(basic("control-user", "control-pass"), :control_api)
    refute authorized?("Bearer ingest-token", :control_api)
  end

  test "rejects missing, malformed, wrong, and unconfigured credentials" do
    for header <- [nil, "Bearer", "Bearer ", "bearer ingest-token", "Basic !!!", "Bearer wrong"] do
      conn = request("/api/v1/push", header)
      result = Auth.call(conn, role: :ingest, config: @config)
      assert result.status == 401
      assert result.halted
      assert result.resp_body == "unauthorized"
      assert result.state == :sent
    end

    conn = request("/api/v1/push", "Bearer ingest-token")
    assert Auth.call(conn, role: :ingest, config: %{ingest: %{}}).status == 401
  end

  test "exempts only configured health and readiness GET paths" do
    for path <- ["/healthz", "/readyz"] do
      result = Auth.call(request(path, nil), role: :control_api, config: @config)
      assert result.status == nil
      refute result.halted
    end

    assert Auth.call(request("/healthz", nil), role: :ingest, config: @config).status == 401
    assert Auth.call(request("/metrics", nil), role: :control_api, config: @config).status == 401

    assert Auth.call(%{request("/healthz", nil) | method: "POST"},
             role: :control_api,
             config: @config
           ).status ==
             401

    assert Auth.call(request("/healthz", nil), role: :ingest, config: %{auth_exemptions: []}).status ==
             401
  end

  test "does not expose credentials in an authentication error" do
    result = Auth.call(request("/push", "Bearer wrong"), role: :ingest, config: @config)
    assert result.status == 401
    assert result.resp_body == "unauthorized"
    assert result.state == :sent
    assert get_resp_header(result, "www-authenticate") != []
    refute result.resp_body =~ "ingest-pass"
  end

  defp authorized?(header, role) do
    Auth.authorized?(request("/", header), role, @config)
  end

  defp request(path, authorization) do
    conn = Plug.Test.conn(:get, path)

    if authorization do
      put_req_header(conn, "authorization", authorization)
    else
      conn
    end
  end

  defp basic(username, password), do: "Basic " <> Base.encode64(username <> ":" <> password)
end

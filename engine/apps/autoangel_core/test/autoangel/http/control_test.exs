defmodule Autoangel.HTTP.ControlTest do
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Autoangel.HTTP.Control

  @config %{control_api: %{token: "control-secret"}, auth_exemptions: []}

  setup do
    Autoangel.Boot.set_phase(:starting)
    Autoangel.Metrics.ensure_started()
    :ok
  end

  test "health and readiness require control credentials" do
    assert conn(:get, "/healthz")
           |> Control.call(config: @config)
           |> Map.fetch!(:status) == 401

    assert conn(:get, "/healthz")
           |> put_req_header("authorization", "Bearer control-secret")
           |> Control.call(config: @config)
           |> Map.fetch!(:status) == 200

    assert conn(:get, "/readyz")
           |> put_req_header("authorization", "Bearer control-secret")
           |> Control.call(config: @config)
           |> Map.fetch!(:status) == 503

    Autoangel.Boot.set_phase(:ready)

    assert conn(:get, "/readyz")
           |> put_req_header("authorization", "Bearer control-secret")
           |> Control.call(config: @config)
           |> Map.fetch!(:status) == 200
  end

  test "metrics are valid exposition and include core counters" do
    Autoangel.Metrics.increment(:ingest_samples_accepted, 2)
    Autoangel.Metrics.increment(:samples_load_shed, 1)
    Autoangel.Metrics.increment({:storage, :successful}, 1)
    Autoangel.Metrics.observe(:detection_latency_seconds, 0.01)

    response =
      conn(:get, "/metrics")
      |> put_req_header("authorization", "Bearer control-secret")
      |> Control.call(config: @config, plugins: ["email"])

    assert response.status == 200
    assert [content_type] = get_resp_header(response, "content-type")
    assert content_type =~ "text/plain; version=0.0.4"
    body = response.resp_body
    assert body =~ "autoangel_ingest_rate_samples_per_second"
    assert body =~ "autoangel_samples_load_shed_total"
    assert body =~ "autoangel_detection_latency_seconds_bucket"
    assert body =~ "autoangel_plugin_deliveries_total"
    assert {:ok, _families} = Autoangel.Exposition.parse(body)
  end

  test "metrics survive the short-lived process that initializes the table" do
    task = Task.async(fn -> Autoangel.Metrics.increment(:short_lived_counter, 3) end)
    assert :ok = Task.await(task)
    assert Autoangel.Metrics.counter(:short_lived_counter) == 3
  end
end

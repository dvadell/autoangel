defmodule EnginePluginTelegramTest do
  use ExUnit.Case, async: true

  test "validates required fields without exposing values" do
    assert {:error, [:bot_token, :chat_id]} = EnginePluginTelegram.validate_config(%{})
    assert {:error, [:chat_id]} = EnginePluginTelegram.validate_config(%{bot_token: "secret"})
    assert :ok = EnginePluginTelegram.validate_config(%{bot_token: "secret", chat_id: "123"})
  end

  test "classifies Telegram responses" do
    assert :ok = EnginePluginTelegram.classify_status(200)
    assert :retryable = EnginePluginTelegram.classify_status(408)
    assert :retryable = EnginePluginTelegram.classify_status(429)
    assert :retryable = EnginePluginTelegram.classify_status(503)
    assert :permanent = EnginePluginTelegram.classify_status(400)
    assert :permanent = EnginePluginTelegram.classify_status(401)
    assert :permanent = EnginePluginTelegram.classify_status(403)
    assert :permanent = EnginePluginTelegram.classify_status(404)
  end

  test "delivers a message and includes incident feedback buttons" do
    parent = self()

    client = fn url, body ->
      send(parent, {:request, url, body})
      {:ok, {{~c"HTTP/1.1", 200, ~c"OK"}, [], ~c"{}"}}
    end

    alert = %{metric_name: "job_failures", incident_id: "inc-1", score: 88}
    config = %{bot_token: Autoangel.Secret.new("token"), chat_id: 123, http_client: client}

    assert :ok = EnginePluginTelegram.deliver(alert, config)
    assert_receive {:request, "https://api.telegram.org/bottoken/sendMessage", body}
    assert body =~ ~s("chat_id":"123")
    assert body =~ ~s("callback_data":"m:inc-1")
    assert body =~ ~s("callback_data":"n:inc-1")
  end

  test "maps network errors to retryable outcomes" do
    client = fn _url, _body -> {:error, :timeout} end

    assert {:retryable, {:http_error, :timeout}} =
             EnginePluginTelegram.deliver(%{text: "alert"}, %{
               bot_token: "token",
               chat_id: "123",
               http_client: client
             })
  end

  test "renders the full alert and series-key candidate labels" do
    parent = self()

    client = fn _url, body ->
      send(parent, {:body, body})
      {:ok, {{~c"HTTP/1.1", 200, ~c"OK"}, [], ~c"{}"}}
    end

    alert = %Autoangel.Alert{
      name: "requests_total",
      labels: %{"job" => "api"},
      observed: 12.5,
      range: %{lower: 1.0, upper: 2.0},
      score: 88,
      start_time: 1_700_000_000_000,
      candidates: [%{series_key: {"cpu", [{"host", "one"}]}, rank: 1, weak?: true}]
    }

    assert :ok =
             EnginePluginTelegram.deliver(alert, %{
               bot_token: "token",
               chat_id: 123,
               http_client: client
             })

    assert_receive {:body, body}
    assert body =~ "requests_total"
    assert body =~ "job=api"
    assert body =~ "expected range lower: 1.0"
    assert body =~ "expected range upper: 2.0"
    assert body =~ "anomaly score: 88"
    assert body =~ "host=one"
    assert body =~ "weak evidence"
  end
end

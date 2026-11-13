defmodule EnginePluginWebhookTest do
  @moduledoc "Task 39.2: the webhook plugin validates its destination and classifies replies."
  use ExUnit.Case, async: true

  alias Autoangel.Test.FakeHttpServer

  # Statuses and the outcome class each must give.
  @classes [
    {200, :ok},
    {201, :ok},
    {204, :ok},
    {408, :retryable},
    {425, :retryable},
    {429, :retryable},
    {500, :retryable},
    {502, :retryable},
    {503, :retryable},
    {301, :permanent},
    {400, :permanent},
    {401, :permanent},
    {403, :permanent},
    {404, :permanent},
    {410, :permanent}
  ]

  defp alert(overrides \\ []) do
    struct(
      %Autoangel.Alert{
        name: "cpu_load",
        labels: %{"host" => "web1"},
        score: 91.5,
        start_time: 1_700_000_000_000,
        start_time_iso: "2023-11-14T22:13:20Z",
        anomaly_id: "a1",
        incident_id: 5
      },
      overrides
    )
  end

  defp client(status, test) do
    fn url, headers, body ->
      send(test, {:sent, url, headers, body})
      {:ok, {{~c"HTTP/1.1", status, ~c"X"}, [], ~c"ok"}}
    end
  end

  defp outcome_class(:ok), do: :ok
  defp outcome_class({:retryable, _}), do: :retryable
  defp outcome_class({:permanent, _}), do: :permanent

  @url "https://hooks.example.com/alerts?token=SECRET-TOKEN"

  test "validates the destination and names only fields" do
    assert :ok = EnginePluginWebhook.validate_config(%{url: @url})

    assert :ok =
             EnginePluginWebhook.validate_config(%{
               url: "http://127.0.0.1:9/x",
               headers: %{"x-key" => "v"}
             })

    assert :ok = EnginePluginWebhook.validate_config(%{url: Autoangel.Secret.new(@url)})

    for bad <- [
          %{},
          %{url: nil},
          %{url: ""},
          %{url: "ftp://x.example"},
          %{url: "not a url"},
          %{url: "https://"},
          %{url: 5},
          %{url: "https://a b.example"}
        ] do
      assert {:error, [:url]} = EnginePluginWebhook.validate_config(bad), inspect(bad)
    end

    assert {:error, [:headers]} =
             EnginePluginWebhook.validate_config(%{url: @url, headers: %{"bad name" => "v"}})

    assert {:error, [:headers]} =
             EnginePluginWebhook.validate_config(%{url: @url, headers: %{"x" => "a\nb"}})

    assert {:error, [:headers]} =
             EnginePluginWebhook.validate_config(%{url: @url, headers: [{"x", "y"}]})

    assert {:error, [:url]} = EnginePluginWebhook.validate_config("nope")
  end

  test "an invalid configuration is a permanent failure that names fields only" do
    assert {:permanent, {:invalid_config, [:url]}} =
             EnginePluginWebhook.deliver(alert(), %{url: "x"})

    assert {:permanent, {:invalid_config, [:url]}} = EnginePluginWebhook.deliver(alert(), :nope)
  end

  test "every reply class gives its outcome" do
    for {status, class} <- @classes do
      config = %{url: @url, http_client: client(status, self())}

      assert outcome_class(EnginePluginWebhook.deliver(alert(), config)) == class,
             "status #{status}"
    end
  end

  test "a network error is retryable and does not leak the URL" do
    failing = fn _url, _headers, _body ->
      {:error, {:failed_connect, [{:to_address, {~c"hooks.example.com", 443}}]}}
    end

    assert {:retryable, {:http_error, reason}} =
             EnginePluginWebhook.deliver(alert(), %{url: @url, http_client: failing})

    refute inspect(reason) =~ "SECRET-TOKEN"

    assert {:retryable, :invalid_http_response} =
             EnginePluginWebhook.deliver(alert(), %{
               url: @url,
               http_client: fn _, _, _ -> :weird end
             })
  end

  test "posts a JSON body with the alert fields and the extra headers" do
    config = %{
      url: @url,
      headers: %{"x-api-key" => Autoangel.Secret.new("k1")},
      http_client: client(200, self())
    }

    assert :ok = EnginePluginWebhook.deliver(alert(), config)
    assert_receive {:sent, @url, headers, body}
    assert {"content-type", "application/json"} in headers
    assert {"x-api-key", "k1"} in headers
    decoded = JSON.decode!(body)
    assert decoded["name"] == "cpu_load"
    assert decoded["labels"] == %{"host" => "web1"}
    assert decoded["score"] == 91.5
    assert decoded["incident_id"] == 5
    assert decoded["resolved"] == false
    assert decoded["text"] =~ "metric: cpu_load"
  end

  test "a resolution is marked resolved" do
    config = %{url: @url, http_client: client(200, self())}
    assert :ok = EnginePluginWebhook.deliver(alert(resolved?: true), config)
    assert_receive {:sent, _, _, body}
    assert %{"resolved" => true, "text" => text} = JSON.decode!(body)
    assert text =~ "status: resolved"
  end

  test "over real HTTP: the request arrives and the status decides" do
    {port, server} = FakeHttpServer.start(self(), 200)
    on_exit(fn -> FakeHttpServer.stop(server) end)
    config = %{url: "http://127.0.0.1:#{port}/hook", timeout: 2_000}

    assert :ok = EnginePluginWebhook.deliver(alert(), config)
    assert_receive {:request, "POST", "/hook", headers, body}
    assert headers["content-type"] == "application/json"
    assert JSON.decode!(body)["name"] == "cpu_load"

    FakeHttpServer.set_status(server, 503)
    assert {:retryable, {:http_status, 503}} = EnginePluginWebhook.deliver(alert(), config)
    FakeHttpServer.set_status(server, 404)
    assert {:permanent, {:http_status, 404}} = EnginePluginWebhook.deliver(alert(), config)
  end

  test "a refused connection is retryable" do
    {:ok, listen} = :gen_tcp.listen(0, ip: {127, 0, 0, 1})
    {:ok, port} = :inet.port(listen)
    :gen_tcp.close(listen)

    assert {:retryable, {:http_error, _}} =
             EnginePluginWebhook.deliver(alert(), %{
               url: "http://127.0.0.1:#{port}/x",
               timeout: 1_000
             })
  end
end

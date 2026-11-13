defmodule EnginePluginNtfyTest do
  @moduledoc "Task 39.2: the ntfy plugin validates its destination and classifies replies."
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

  test "validates topic, server, token and priority" do
    assert :ok = EnginePluginNtfy.validate_config(%{topic: "alerts"})

    assert :ok =
             EnginePluginNtfy.validate_config(%{
               topic: "my_Alerts-1",
               server: "http://ntfy.local:8080",
               token: "tk_x",
               priority: 5
             })

    assert :ok =
             EnginePluginNtfy.validate_config(%{topic: "t", token: Autoangel.Secret.new("tk_x")})

    for topic <- [nil, "", "has space", "a/b", "über", String.duplicate("a", 65), 5] do
      assert {:error, [:topic]} = EnginePluginNtfy.validate_config(%{topic: topic}), inspect(topic)
    end

    assert {:error, [:server]} = EnginePluginNtfy.validate_config(%{topic: "t", server: "ntfy.sh"})
    assert {:error, [:token]} = EnginePluginNtfy.validate_config(%{topic: "t", token: ""})
    assert {:error, [:priority]} = EnginePluginNtfy.validate_config(%{topic: "t", priority: 6})
    assert {:error, [:topic, :priority]} = EnginePluginNtfy.validate_config(%{priority: 0})
    assert {:error, [:topic]} = EnginePluginNtfy.validate_config(nil)
  end

  test "every reply class gives its outcome" do
    for {status, class} <- @classes do
      config = %{topic: "alerts", http_client: client(status, self())}
      assert outcome_class(EnginePluginNtfy.deliver(alert(), config)) == class, "status #{status}"
    end
  end

  test "an invalid configuration is permanent" do
    assert {:permanent, {:invalid_config, [:topic]}} = EnginePluginNtfy.deliver(alert(), %{})
  end

  test "posts the alert text to server/topic with title, priority and token" do
    config = %{
      topic: "alerts",
      server: "https://ntfy.example/",
      token: Autoangel.Secret.new("tk_1"),
      priority: 4,
      http_client: client(200, self())
    }

    assert :ok = EnginePluginNtfy.deliver(alert(), config)
    assert_receive {:sent, "https://ntfy.example/alerts", headers, body}
    assert {"title", "cpu_load"} in headers
    assert {"priority", "4"} in headers
    assert {"authorization", "Bearer tk_1"} in headers
    assert {"tags", "warning"} in headers
    assert body =~ "metric: cpu_load"
  end

  test "the default server is ntfy.sh and no token means no authorization" do
    assert :ok = EnginePluginNtfy.deliver(alert(), %{topic: "t", http_client: client(200, self())})
    assert_receive {:sent, "https://ntfy.sh/t", headers, _body}
    refute Enum.any?(headers, fn {k, _} -> k == "authorization" end)
  end

  test "a title with line breaks or non-ASCII text stays a safe header" do
    assert :ok =
             EnginePluginNtfy.deliver(alert(name: "caf\u00e9\nmetric"), %{
               topic: "t",
               http_client: client(200, self())
             })

    assert_receive {:sent, _, headers, _}
    {"title", title} = List.keyfind(headers, "title", 0)
    refute title =~ ~r/[\r\n]/
    assert String.printable?(title) and title =~ ~r/\A[\x20-\x7E]+\z/
  end

  test "a resolution is titled and tagged as resolved" do
    assert :ok =
             EnginePluginNtfy.deliver(alert(resolved?: true), %{
               topic: "t",
               http_client: client(200, self())
             })

    assert_receive {:sent, _, headers, body}
    assert {"title", "resolved: cpu_load"} in headers
    assert {"tags", "white_check_mark"} in headers
    assert body =~ "status: resolved"
  end

  test "over real HTTP" do
    {port, server} = FakeHttpServer.start(self(), 200)
    on_exit(fn -> FakeHttpServer.stop(server) end)
    config = %{topic: "alerts", server: "http://127.0.0.1:#{port}", timeout: 2_000, token: "tk_9"}

    assert :ok = EnginePluginNtfy.deliver(alert(), config)
    assert_receive {:request, "POST", "/alerts", headers, body}
    assert headers["authorization"] == "Bearer tk_9"
    assert headers["title"] == "cpu_load"
    assert body =~ "cpu_load"

    FakeHttpServer.set_status(server, 429)
    assert {:retryable, {:http_status, 429}} = EnginePluginNtfy.deliver(alert(), config)
    FakeHttpServer.set_status(server, 403)
    assert {:permanent, {:http_status, 403}} = EnginePluginNtfy.deliver(alert(), config)
  end
end

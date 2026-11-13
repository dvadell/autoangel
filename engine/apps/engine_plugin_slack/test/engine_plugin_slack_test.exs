defmodule EnginePluginSlackTest do
  @moduledoc "Task 39.2: the slack plugin validates its destination and classifies replies."
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

  @url "https://hooks.slack.com/services/T000/B000/XXXX"

  test "validates the webhook URL and names only the field" do
    assert :ok = EnginePluginSlack.validate_config(%{webhook_url: @url})
    assert :ok = EnginePluginSlack.validate_config(%{webhook_url: Autoangel.Secret.new(@url)})

    for bad <- [
          %{},
          %{webhook_url: nil},
          %{webhook_url: ""},
          %{webhook_url: "http://hooks.slack.com/services/T000/B000/XXXX"},
          %{webhook_url: "ftp://x"},
          %{webhook_url: "https://"},
          %{webhook_url: "https://a b"},
          %{webhook_url: 7}
        ] do
      assert {:error, [:webhook_url]} = EnginePluginSlack.validate_config(bad), inspect(bad)
    end

    assert {:error, [:webhook_url]} = EnginePluginSlack.validate_config(:nope)
  end

  test "an invalid configuration is permanent and names fields only" do
    assert {:permanent, {:invalid_config, [:webhook_url]}} = EnginePluginSlack.deliver(alert(), %{})
  end

  test "every reply class gives its outcome" do
    for {status, class} <- @classes do
      config = %{webhook_url: @url, http_client: client(status, self())}
      assert outcome_class(EnginePluginSlack.deliver(alert(), config)) == class, "status #{status}"
    end
  end

  test "a network error is retryable and does not leak the URL" do
    failing = fn _url, _headers, _body ->
      {:error, {:failed_connect, [{:to_address, {~c"hooks.slack.com", 443}}]}}
    end

    assert {:retryable, {:http_error, reason}} =
             EnginePluginSlack.deliver(alert(), %{webhook_url: @url, http_client: failing})

    refute inspect(reason) =~ "TOKEN"
    refute inspect(reason) =~ "XXXX"
  end

  test "posts a JSON message holding the alert text as a code block" do
    assert :ok =
             EnginePluginSlack.deliver(alert(), %{
               webhook_url: @url,
               http_client: client(200, self())
             })

    assert_receive {:sent, @url, headers, body}
    assert {"content-type", "application/json"} in headers
    text = JSON.decode!(body)["text"]
    assert text =~ "metric: cpu_load"
    assert String.starts_with?(text, "```\n") and String.ends_with?(text, "\n```")
  end

  test "a label cannot close the code block" do
    sneaky = alert(labels: %{"host" => "```@everyone"})

    assert :ok =
             EnginePluginSlack.deliver(sneaky, %{
               webhook_url: @url,
               http_client: client(200, self())
             })

    assert_receive {:sent, _, _, body}
    text = JSON.decode!(body)["text"]
    inner = text |> String.replace_prefix("```\n", "") |> String.replace_suffix("\n```", "")
    refute inner =~ "```"
  end

  test "a long alert is cut to the service limit" do
    long = alert(labels: Map.new(1..400, fn i -> {"label_#{i}", String.duplicate("v", 40)} end))

    assert :ok =
             EnginePluginSlack.deliver(long, %{webhook_url: @url, http_client: client(200, self())})

    assert_receive {:sent, _, _, body}
    text = JSON.decode!(body)["text"]
    assert String.length(text) <= 3100
  end

  test "a resolution reads as resolved" do
    assert :ok =
             EnginePluginSlack.deliver(alert(resolved?: true), %{
               webhook_url: @url,
               http_client: client(200, self())
             })

    assert_receive {:sent, _, _, body}
    assert JSON.decode!(body)["text"] =~ "status: resolved"
  end
end

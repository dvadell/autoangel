defmodule EnginePluginTelegram.PollerTest do
  @moduledoc "Task 37.3: feedback buttons are polled and passed to Feedback."
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias EnginePluginTelegram.Poller

  @config %{bot_token: "123:SECRET-TOKEN", chat_id: 42}

  # A fake Bot API: `getUpdates` answers from a queue (then waits, like a long
  # poll) and every call is reported to the test.
  defp fake_client(test, updates_queue) do
    {:ok, queue} = Agent.start_link(fn -> updates_queue end)

    fn method, params ->
      send(test, {:api, method, params})

      if method == "getUpdates",
        do: next_updates(queue),
        else: {:ok, %{"ok" => true, "result" => true}}
    end
  end

  defp next_updates(queue) do
    case Agent.get_and_update(queue, fn
           [] -> {:empty, []}
           [head | rest] -> {head, rest}
         end) do
      :empty ->
        Process.sleep(30)
        {:ok, %{"ok" => true, "result" => []}}

      {:error, reason} ->
        {:error, reason}

      updates ->
        {:ok, %{"ok" => true, "result" => updates}}
    end
  end

  defp callback(update_id, data, chat \\ 42),
    do: %{
      "update_id" => update_id,
      "callback_query" => %{
        "id" => "q#{update_id}",
        "data" => data,
        "message" => %{"chat" => %{"id" => chat}}
      }
    }

  defp start(queue, opts \\ []) do
    test = self()

    feedback =
      Keyword.get(opts, :feedback, fn action, target ->
        send(test, {:feedback, action, target}) && {:ok, :applied}
      end)

    start_supervised!(
      {Poller,
       Keyword.merge(
         [
           config: @config,
           client: fake_client(test, queue),
           feedback: feedback,
           retry_ms: 20,
           poll_timeout: 1
         ],
         opts
       )}
    )
  end

  test "callback data is parsed strictly" do
    assert {:ok, "mute", 5} = Poller.parse_callback("m:5")
    assert {:ok, "this was normal", 12} = Poller.parse_callback("n:12")

    for bad <- ["m:", "m:0", "m:-1", "m:x", "x:5", "m5", "", nil, 5, "m:5:6", "m: 5", "M:5"] do
      assert :error = Poller.parse_callback(bad), inspect(bad)
    end
  end

  test "a mute press goes to Feedback with the Incident and is answered" do
    start([[callback(10, "m:5")]])
    assert_receive {:feedback, "mute", %{"incident_id" => 5}}, 1_000

    assert_receive {:api, "answerCallbackQuery", %{"callback_query_id" => "q10", "text" => "done"}},
                   1_000
  end

  test "a 'this was normal' press goes to Feedback" do
    start([[callback(11, "n:7")]])
    assert_receive {:feedback, "this was normal", %{"incident_id" => 7}}, 1_000
  end

  test "the next poll asks only for newer updates" do
    start([[callback(10, "m:5"), callback(12, "n:6")]])
    assert_receive {:api, "getUpdates", first}, 1_000
    refute Map.has_key?(first, "offset")
    assert first["allowed_updates"] == ["callback_query"]
    assert_receive {:api, "getUpdates", %{"offset" => 13}}, 1_000
  end

  test "presses from another chat, malformed data and other updates are ignored" do
    start([
      [
        callback(1, "m:5", 999),
        callback(2, "garbage"),
        %{"update_id" => 3, "message" => %{"text" => "hello"}},
        %{"update_id" => 4, "callback_query" => %{"id" => "q4"}},
        %{"no_id" => true}
      ]
    ])

    assert_receive {:api, "getUpdates", %{"offset" => 5}}, 1_000
    refute_received {:feedback, _, _}
    refute_received {:api, "answerCallbackQuery", _}
  end

  test "an Incident Feedback cannot find gets a gentle answer" do
    feedback = fn _action, _target -> {:error, :invalid, :incident_id} end
    start([[callback(20, "m:99")]], feedback: feedback)
    assert_receive {:api, "answerCallbackQuery", %{"text" => "this alert is no longer open"}}, 1_000
  end

  test "a Feedback that is not running does not stop the poller" do
    feedback = fn _action, _target -> exit(:noproc) end
    start([[callback(30, "m:1")], [callback(31, "n:2")]], feedback: feedback)
    assert_receive {:api, "answerCallbackQuery", %{"callback_query_id" => "q30"}}, 1_000

    assert_receive {:api, "answerCallbackQuery", %{"callback_query_id" => "q31", "text" => text}},
                   1_000

    assert text =~ "could not apply"
  end

  test "a failed poll is retried, and the token is never logged" do
    log =
      capture_log(fn ->
        start([
          {:error, {:http_error, "https://api.telegram.org/bot123:SECRET-TOKEN/getUpdates"}},
          [callback(40, "m:3")]
        ])

        assert_receive {:feedback, "mute", %{"incident_id" => 3}}, 2_000
      end)

    assert log =~ "telegram poll failed"
    refute log =~ "SECRET-TOKEN"
  end

  test "the plugin supervises the poller unless buttons are turned off" do
    assert [{Poller, config: @config}] = EnginePluginTelegram.children(@config)
    assert [{Poller, _}] = EnginePluginTelegram.children(Map.put(@config, :feedback_buttons, true))
    assert [] = EnginePluginTelegram.children(Map.put(@config, :feedback_buttons, false))
  end

  describe "buttons on alerts" do
    defp sent_body(alert) do
      parent = self()

      client = fn _url, body ->
        send(parent, {:body, body})
        {:ok, {{~c"HTTP/1.1", 200, ~c"OK"}, [], ~c"{}"}}
      end

      assert :ok =
               EnginePluginTelegram.deliver(alert, %{
                 bot_token: "t",
                 chat_id: 42,
                 http_client: client
               })

      assert_receive {:body, body}
      body
    end

    test "an alert carries mute and normal buttons for its Incident number" do
      body = sent_body(%Autoangel.Alert{name: "cpu", incident_id: 5, anomaly_id: "a1"})
      assert body =~ ~s("callback_data":"m:5")
      assert body =~ ~s("callback_data":"n:5")
      assert body =~ ~s("text":"mute")
      assert body =~ ~s("text":"this was normal")
    end

    test "a resolution has no buttons" do
      body = sent_body(%Autoangel.Alert{name: "cpu", incident_id: 5, resolved?: true})
      refute body =~ "reply_markup"
      assert body =~ "status: resolved"
    end
  end
end

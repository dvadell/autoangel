defmodule Console.MCPTest do
  @moduledoc """
  Task 31.15 and the example-based checks of 31.1 to 31.8: the HTTP endpoint and
  its credential, the tool list with write tools on and off, `give_feedback`
  against a fake Control_API per response class, the stdio forwarder with the
  endpoint down, and credential rotation.
  """
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Console.MCP.Stdio
  alias Console.Test.{FakeData, FakeEngine}

  @token "mcp-secret-token"
  @series %{
    id: "11111111-1111-1111-1111-111111111111",
    name: "http_requests_total",
    labels: %{"job" => "api"},
    kind: "counter",
    status: "anomalous"
  }
  @other %{
    id: "22222222-2222-2222-2222-222222222222",
    name: "cpu_load",
    labels: %{"host" => "Web1"},
    kind: "gauge",
    status: "healthy"
  }
  @anomaly_id "aaaaaaaa-0000-0000-0000-000000000001"

  setup do
    previous = Application.get_all_env(:autoangel_console)
    Application.put_env(:autoangel_console, :auth, [{:basic, "ops", "pw"}, {:bearer, "web-tok"}])
    Application.put_env(:autoangel_console, :mcp_token, @token)
    Application.put_env(:autoangel_console, :mcp_write_tools, false)
    Application.put_env(:autoangel_console, :health_exempt, false)

    FakeData.put(
      series: [@series, @other],
      anomalies: [anomaly()],
      heartbeat: DateTime.utc_now()
    )

    FakeEngine.put(ready: true, test_pid: self())

    on_exit(fn ->
      for {key, value} <- previous, do: Application.put_env(:autoangel_console, key, value)
    end)
  end

  defp anomaly(overrides \\ []) do
    Map.merge(
      %{
        id: @anomaly_id,
        series_id: @series.id,
        series: @series,
        start_time: DateTime.add(DateTime.utc_now(), -600, :second),
        end_time: nil,
        score: 91.5,
        observed: 120.0,
        expected_lower: 10.0,
        expected_upper: 20.0,
        baseline_type: "rolling",
        status: "open",
        kind: "value",
        classification: nil,
        learning: false
      },
      Map.new(overrides)
    )
  end

  defp post(body, headers \\ [{"authorization", "Bearer " <> @token}]) do
    headers
    |> Enum.reduce(conn(:post, "/mcp", body), fn {k, v}, c -> put_req_header(c, k, v) end)
    |> Console.Endpoint.call(Console.Endpoint.init([]))
  end

  defp rpc(method, params \\ nil, id \\ 1) do
    message = %{"jsonrpc" => "2.0", "id" => id, "method" => method}
    Jason.encode!(if params, do: Map.put(message, "params", params), else: message)
  end

  defp call_tool(name, arguments) do
    conn = post(rpc("tools/call", %{"name" => name, "arguments" => arguments}))
    assert conn.status == 200
    %{"result" => result} = Jason.decode!(conn.resp_body)
    {Jason.decode!(hd(result["content"])["text"]), result["isError"]}
  end

  describe "credential (Requirements 64.1 to 64.3, 74.1, 74.2, 74.7)" do
    test "every request without the MCP credential gets a bare 401" do
      for headers <- [
            [],
            [{"authorization", "Bearer wrong"}],
            [{"authorization", "Bearer "}],
            [{"authorization", "Basic " <> Base.encode64("ops:pw")}],
            [{"authorization", "Bearer web-tok"}],
            [{"authorization", @token}],
            [{"authorization", "bearer " <> @token}]
          ] do
        conn = post(rpc("tools/list"), headers)
        assert conn.status == 401, inspect(headers)
        assert conn.resp_body == ""
      end
    end

    test "with no MCP credential configured, nothing opens /mcp" do
      Application.put_env(:autoangel_console, :mcp_token, nil)
      assert post(rpc("tools/list")).status == 401
      assert post(rpc("tools/list"), [{"authorization", "Bearer "}]).status == 401
    end

    test "the MCP credential does not open a web route" do
      conn =
        conn(:get, "/")
        |> put_req_header("authorization", "Bearer " <> @token)
        |> Console.Endpoint.call(Console.Endpoint.init([]))

      assert conn.status == 401
      assert conn.resp_body == ""
    end

    test "after a restart with a new credential, only the new one works" do
      assert post(rpc("ping")).status == 200
      Application.put_env(:autoangel_console, :mcp_token, "rotated-token")
      assert post(rpc("ping")).status == 401
      assert post(rpc("ping"), [{"authorization", "Bearer rotated-token"}]).status == 200
    end

    test "the configuration refuses an MCP token equal to a web credential" do
      env = %{
        "AUTOANGEL_CONSOLE_TOKEN" => "same",
        "AUTOANGEL_CONSOLE_MCP_TOKEN" => "same",
        "AUTOANGEL_CONSOLE_SECRET_KEY_BASE" => String.duplicate("k", 64)
      }

      assert {:error, problems} = Console.Config.from_env(env)
      assert Enum.any?(problems, &(&1 =~ "MCP_TOKEN"))

      assert {:ok, %{mcp_token: "other"}} =
               Console.Config.from_env(%{env | "AUTOANGEL_CONSOLE_MCP_TOKEN" => "other"})
    end
  end

  describe "transport (Requirements 59.1, 59.5)" do
    test "GET and DELETE answer 405 after authentication" do
      for method <- [:get, :delete] do
        conn =
          conn(method, "/mcp")
          |> put_req_header("authorization", "Bearer " <> @token)
          |> Console.Endpoint.call(Console.Endpoint.init([]))

        assert conn.status == 405
        assert get_resp_header(conn, "allow") == ["POST"]
      end

      assert conn(:get, "/mcp")
             |> Console.Endpoint.call(Console.Endpoint.init([]))
             |> Map.get(:status) ==
               401
    end

    test "an Origin from another host is refused" do
      headers = [{"authorization", "Bearer " <> @token}, {"origin", "http://evil.example"}]
      assert post(rpc("ping"), headers).status == 403
    end

    test "initialize, ping and a notification" do
      conn = post(rpc("initialize", %{"protocolVersion" => "2025-03-26"}))
      assert conn.status == 200

      assert %{"result" => %{"protocolVersion" => "2025-03-26", "capabilities" => %{"tools" => _}}} =
               Jason.decode!(conn.resp_body)

      assert %{"result" => %{}} = Jason.decode!(post(rpc("ping")).resp_body)

      note = Jason.encode!(%{"jsonrpc" => "2.0", "method" => "notifications/initialized"})
      conn = post(note)
      assert conn.status == 202
      assert conn.resp_body == ""
    end

    test "protocol errors" do
      assert %{"error" => %{"code" => -32_700}} = Jason.decode!(post("{not json").resp_body)
      assert %{"error" => %{"code" => -32_600}} = Jason.decode!(post("[1]").resp_body)
      assert %{"error" => %{"code" => -32_601}} = Jason.decode!(post(rpc("nope")).resp_body)

      assert %{"error" => %{"code" => -32_602}} =
               Jason.decode!(post(rpc("tools/call", %{"name" => "delete_everything"})).resp_body)
    end
  end

  describe "tool list and write-tool gating (Requirements 63.1 to 63.3)" do
    defp tool_names do
      %{"result" => %{"tools" => tools}} = Jason.decode!(post(rpc("tools/list")).resp_body)
      Enum.map(tools, & &1["name"]) |> Enum.sort()
    end

    test "only read tools while write tools are off" do
      assert tool_names() == ~w(explain_anomaly list_anomalies list_series query_series)
    end

    test "give_feedback is listed only with write tools on" do
      Application.put_env(:autoangel_console, :mcp_write_tools, true)
      assert "give_feedback" in tool_names()
    end

    test "calling a write tool while disabled never reaches the Control_API" do
      {payload, error?} = call_tool("give_feedback", %{"id" => @anomaly_id, "verdict" => "mute"})
      assert error? == true
      assert payload["error"]["code"] == "write_tools_disabled"
      assert payload["error"]["message"] == "write tools are disabled"
      refute_receive {:feedback, _, _}, 50
    end
  end

  describe "give_feedback (Requirements 62.1 to 62.5)" do
    setup do
      Application.put_env(:autoangel_console, :mcp_write_tools, true)
    end

    test "mute sends one request and reports success" do
      {payload, error?} = call_tool("give_feedback", %{"id" => @anomaly_id, "verdict" => "mute"})
      assert error? == false
      assert payload["applied"] == "mute"
      assert_receive {:feedback, "mute", %{"series" => %{"name" => "http_requests_total"}}}
      refute_receive {:feedback, _, _}, 50
    end

    test "normal maps to the Control_API action" do
      {_payload, false} = call_tool("give_feedback", %{"id" => @anomaly_id, "verdict" => "normal"})
      assert_receive {:feedback, "this was normal", _target}
    end

    test "each response class gives its own error code" do
      for {result, code} <- [
            {{:error, "the Engine refused the series"}, "engine_rejected"},
            {{:error, "the Engine answered 400: bad"}, "engine_rejected"},
            {{:error, "the Engine answered 503: engine starting"}, "engine_unavailable"},
            {{:error, "the Engine could not be reached"}, "engine_unavailable"},
            {{:error, "the Engine address is not configured"}, "engine_unavailable"}
          ] do
        FakeEngine.put(ready: true, test_pid: self(), feedback_result: result)
        {payload, error?} = call_tool("give_feedback", %{"id" => @anomaly_id, "verdict" => "mute"})
        assert error?
        assert payload["error"]["code"] == code
      end
    end

    test "invalid id and verdict are named, and no request is sent" do
      {payload, true} = call_tool("give_feedback", %{"id" => "nope", "verdict" => "maybe"})
      assert payload["error"]["parameters"] == ["id", "verdict"]
      {payload, true} = call_tool("give_feedback", %{})
      assert payload["error"]["parameters"] == ["id", "verdict"]
      refute_receive {:feedback, _, _}, 50
    end

    test "an unknown Anomaly is not found" do
      {payload, true} =
        call_tool("give_feedback", %{
          "id" => "bbbbbbbb-0000-0000-0000-000000000009",
          "verdict" => "mute"
        })

      assert payload["error"]["code"] == "not_found"
      refute_receive {:feedback, _, _}, 50
    end
  end

  describe "read tools" do
    test "list_anomalies returns marked values and the anomaly score" do
      {payload, false} = call_tool("list_anomalies", %{})
      assert payload["count"] == 1
      [item] = payload["anomalies"]
      assert item["id"] == @anomaly_id
      assert item["status"] == "open"
      assert item["anomaly_score"] == 91.5
      assert item["series"]["name"] == "<untrusted>http_requests_total</untrusted>"
      refute Map.has_key?(payload, "truncated")
    end

    test "list_anomalies filters by min_score, filter and since" do
      FakeData.put(
        series: [@series, @other],
        anomalies: [
          anomaly(),
          anomaly(
            id: "aaaaaaaa-0000-0000-0000-000000000002",
            series: @other,
            series_id: @other.id,
            score: 40.0,
            end_time: DateTime.add(DateTime.utc_now(), -100, :second)
          )
        ],
        heartbeat: DateTime.utc_now()
      )

      {all, false} = call_tool("list_anomalies", %{})
      assert all["count"] == 2

      {high, false} = call_tool("list_anomalies", %{"min_score" => 50})
      assert Enum.map(high["anomalies"], & &1["id"]) == [@anomaly_id]

      {web, false} = call_tool("list_anomalies", %{"filter" => "Web1"})
      assert web["count"] == 1

      {none, false} = call_tool("list_anomalies", %{"filter" => "web1"})
      assert none["count"] == 0

      since = DateTime.utc_now() |> DateTime.add(-30, :second) |> DateTime.to_iso8601()
      {recent, false} = call_tool("list_anomalies", %{"since" => since})
      assert Enum.map(recent["anomalies"], & &1["id"]) == [@anomaly_id]
    end

    test "explain_anomaly with candidates in rank order, and without" do
      FakeData.put(
        series: [@series, @other],
        anomalies: [anomaly()],
        candidates: [
          %{
            anomaly_id: @anomaly_id,
            rank: 2,
            weak: true,
            series: @series,
            evidence: %{"lag" => "5s"}
          },
          %{anomaly_id: @anomaly_id, rank: 1, weak: false, series: @other, evidence: %{}}
        ]
      )

      {payload, false} = call_tool("explain_anomaly", %{"id" => @anomaly_id})
      assert Enum.map(payload["candidates"], & &1["rank"]) == [1, 2]
      assert Enum.at(payload["candidates"], 1)["weak_evidence"] == true
      refute Map.has_key?(payload, "note")

      FakeData.put(series: [@series], anomalies: [anomaly()])
      {payload, false} = call_tool("explain_anomaly", %{"id" => @anomaly_id})
      assert payload["candidates"] == []
      assert payload["note"] == "no candidates found inside the Correlation_Window"
    end

    test "explain_anomaly: not found and invalid id" do
      {payload, true} =
        call_tool("explain_anomaly", %{"id" => "bbbbbbbb-0000-0000-0000-000000000009"})

      assert payload["error"]["code"] == "not_found"
      {payload, true} = call_tool("explain_anomaly", %{"id" => 5})
      assert payload["error"]["parameters"] == ["id"]
    end

    test "query_series returns resolution_used and at most 500 points" do
      now = DateTime.utc_now()
      points = for i <- 0..1_999, do: %{ts: DateTime.add(now, -i, :second), value: i * 1.0}

      FakeData.put(
        series: [@series],
        chart: %{tier: :minute, points: points, bands: []}
      )

      {payload, false} = call_tool("query_series", %{"series_id" => @series.id})
      assert payload["resolution_used"] == "1m"
      assert length(payload["points"]) <= 500
      assert payload["count"] == length(payload["points"])

      {small, false} = call_tool("query_series", %{"series_id" => @series.id, "max_points" => 10})
      assert length(small["points"]) <= 10
    end

    test "query_series: unknown series and bad range" do
      {payload, true} =
        call_tool("query_series", %{"series_id" => "bbbbbbbb-0000-0000-0000-000000000009"})

      assert payload["error"]["code"] == "not_found"

      now = DateTime.utc_now()

      {payload, true} =
        call_tool("query_series", %{
          "series_id" => @series.id,
          "from" => DateTime.to_iso8601(now),
          "to" => DateTime.to_iso8601(DateTime.add(now, -60, :second))
        })

      assert payload["error"]["parameters"] == ["from", "to"]
    end

    test "list_series lists stored status and filters" do
      {payload, false} = call_tool("list_series", %{})
      assert payload["count"] == 2
      assert Enum.any?(payload["series"], &(&1["status"] == "anomalous"))

      {payload, false} = call_tool("list_series", %{"filter" => "cpu"})
      assert [%{"id" => id}] = payload["series"]
      assert id == @other.id
    end

    test "a database failure gives a stable error with no partial data" do
      FakeData.put(series: [@series], anomalies: [anomaly()], fail: true)

      for {tool, args} <- [
            {"list_anomalies", %{}},
            {"explain_anomaly", %{"id" => @anomaly_id}},
            {"query_series", %{"series_id" => @series.id}},
            {"list_series", %{}}
          ] do
        {payload, true} = call_tool(tool, args)

        assert payload == %{
                 "error" => %{
                   "code" => "store_unavailable",
                   "message" => "the database could not answer"
                 }
               }
      end
    end
  end

  describe "stdio forwarder (Requirements 59.2 to 59.4)" do
    defp run_stdio(lines, post) do
      {:ok, input} = Agent.start_link(fn -> lines end)
      {:ok, output} = Agent.start_link(fn -> [] end)

      read = fn ->
        Agent.get_and_update(input, fn
          [] -> {:eof, []}
          [line | rest] -> {line, rest}
        end)
      end

      write = fn line -> Agent.update(output, &[line | &1]) end
      :ok = Stdio.run(read, write, post)
      Agent.get(output, &Enum.reverse/1)
    end

    test "an unreachable endpoint gives a JSON-RPC error per request and the loop continues" do
      down = fn _body -> {:error, :econnrefused} end

      out =
        run_stdio(
          [rpc("tools/list", nil, 1) <> "\n", rpc("ping", nil, "two") <> "\n"],
          down
        )

      assert [first, second] = Enum.map(out, &Jason.decode!/1)
      assert %{"id" => 1, "error" => %{"code" => -32_000}} = first
      assert %{"id" => "two", "error" => %{"message" => message}} = second
      assert message =~ "unreachable"
    end

    test "a notification gets no answer even when the endpoint is down" do
      down = fn _body -> {:error, :econnrefused} end
      note = Jason.encode!(%{"jsonrpc" => "2.0", "method" => "notifications/initialized"})
      assert run_stdio([note <> "\n"], down) == []
    end

    test "a refusal is reported with its status" do
      refuse = fn _body -> {:ok, 401, ""} end
      assert [line] = run_stdio([rpc("ping") <> "\n"], refuse)
      assert %{"error" => %{"message" => "the Console answered 401"}} = Jason.decode!(line)
    end

    test "the escript forwards the request line unchanged" do
      parent = self()

      echo = fn body ->
        send(parent, {:posted, body})
        {:ok, 200, ~s({"jsonrpc":"2.0","id":1,"result":{}})}
      end

      line = ~s({"jsonrpc":"2.0",  "id":1, "method":"ping"})
      assert [~s({"jsonrpc":"2.0","id":1,"result":{}})] = run_stdio([line <> "\r\n"], echo)
      assert_receive {:posted, ^line}
    end
  end
end

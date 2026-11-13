defmodule Console.MCPPropertyTest do
  @moduledoc "Properties 58 to 63 (tasks 31.9 to 31.14)."
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Plug.Conn
  import Plug.Test

  alias Console.MCP.{Stdio, Tools, Untrusted}
  alias Console.Test.FakeData

  @token "mcp-property-token"
  @now_series_id "11111111-1111-1111-1111-111111111111"

  setup do
    previous = Application.get_all_env(:autoangel_console)
    Application.put_env(:autoangel_console, :auth, [{:bearer, "web-tok"}])
    Application.put_env(:autoangel_console, :mcp_token, @token)
    Application.put_env(:autoangel_console, :mcp_write_tools, false)

    on_exit(fn ->
      for {key, value} <- previous, do: Application.put_env(:autoangel_console, key, value)
    end)
  end

  defp http(body) do
    conn(:post, "/mcp", body)
    |> put_req_header("authorization", "Bearer " <> @token)
    |> Console.Endpoint.call(Console.Endpoint.init([]))
  end

  defp call_json(name, arguments),
    do:
      Jason.encode!(%{
        "jsonrpc" => "2.0",
        "id" => 1,
        "method" => "tools/call",
        "params" => %{"name" => name, "arguments" => arguments}
      })

  defp payload(name, arguments) do
    %{"result" => %{"content" => [%{"text" => text}], "isError" => error?}} =
      name |> call_json(arguments) |> http() |> Map.fetch!(:resp_body) |> Jason.decode!()

    {Jason.decode!(text), error?}
  end

  defp name_gen, do: string(:alphanumeric, min_length: 1, max_length: 12)

  defp series_gen do
    gen all(
          id <- integer(1..9_999),
          name <- name_gen(),
          label <- name_gen(),
          value <- name_gen()
        ) do
      %{
        id: "00000000-0000-0000-0000-#{String.pad_leading(Integer.to_string(id), 12, "0")}",
        name: name,
        labels: %{label => value},
        kind: "gauge",
        status: "healthy"
      }
    end
  end

  defp anomaly_gen(series_pool) do
    gen all(
          index <- integer(1..9_999),
          series <- member_of(series_pool),
          age <- integer(1..(3 * 86_400)),
          open? <- boolean(),
          length <- integer(0..86_400),
          score <- one_of([constant(nil), float(min: 0.0, max: 100.0)])
        ) do
      start = DateTime.add(DateTime.utc_now(), -age, :second)
      ended = if open?, do: nil, else: DateTime.add(start, min(length, age - 1), :second)

      %{
        id: "aaaaaaaa-0000-0000-0000-#{String.pad_leading(Integer.to_string(index), 12, "0")}",
        series_id: series.id,
        series: series,
        start_time: start,
        end_time: ended,
        score: score,
        observed: 1.0,
        expected_lower: 0.0,
        expected_upper: 2.0,
        baseline_type: "rolling",
        status: if(open?, do: "open", else: "resolved"),
        kind: "value",
        classification: nil,
        learning: false
      }
    end
  end

  # Feature: v1, Property 58: stdio and HTTP MCP transports are equivalent
  property "the stdio forwarder gives the same answer as a direct HTTP request" do
    FakeData.put(series: [], anomalies: [], heartbeat: DateTime.utc_now())

    post = fn body ->
      conn = http(body)
      {:ok, conn.status, conn.resp_body}
    end

    message =
      one_of([
        constant(%{"method" => "initialize", "params" => %{"protocolVersion" => "2025-03-26"}}),
        constant(%{"method" => "ping"}),
        constant(%{"method" => "tools/list"}),
        constant(%{"method" => "nope"}),
        map(name_gen(), &%{"method" => "tools/call", "params" => %{"name" => &1}}),
        map(
          fixed_map(%{since: name_gen(), min_score: integer(-500..500)}),
          &%{
            "method" => "tools/call",
            "params" => %{
              "name" => "list_anomalies",
              "arguments" => %{"since" => &1.since, "min_score" => &1.min_score}
            }
          }
        )
      ])

    check all(
            base <- message,
            id <- one_of([integer(), name_gen()]),
            notification? <- boolean()
          ) do
      request = Map.put(base, "jsonrpc", "2.0")
      request = if notification?, do: request, else: Map.put(request, "id", id)
      line = Jason.encode!(request)

      {:ok, input} = Agent.start_link(fn -> [line <> "\n"] end)
      {:ok, output} = Agent.start_link(fn -> [] end)

      read = fn ->
        Agent.get_and_update(input, fn
          [] -> {:eof, []}
          [l | r] -> {l, r}
        end)
      end

      write = fn out -> Agent.update(output, &[out | &1]) end
      :ok = Stdio.run(read, write, post)
      via_stdio = Agent.get(output, &Enum.reverse/1)

      direct = http(line)

      case direct.status do
        202 -> assert via_stdio == []
        200 -> assert via_stdio == [direct.resp_body]
      end
    end
  end

  # Feature: v1, Property 59: MCP list filters
  property "list_anomalies and list_series return exactly the items the filters select" do
    check all(
            pool <- list_of(series_gen(), min_length: 1, max_length: 6),
            anomalies <- list_of(anomaly_gen(pool), max_length: 12),
            min_score <- one_of([constant(nil), integer(0..100)]),
            window <- integer(60..(3 * 86_400)),
            use_filter? <- boolean(),
            term_source <- member_of(pool),
            term_kind <- member_of([:name, :label, :value, :missing])
          ) do
      anomalies = Enum.uniq_by(anomalies, & &1.id)
      pool = Enum.uniq_by(pool, & &1.id)
      FakeData.put(series: pool, anomalies: anomalies, heartbeat: DateTime.utc_now())

      term =
        case term_kind do
          :name -> term_source.name
          :label -> term_source.labels |> Map.keys() |> hd()
          :value -> term_source.labels |> Map.values() |> hd()
          :missing -> "~no-such-text~"
        end

      since = DateTime.utc_now() |> DateTime.add(-window, :second)
      args = %{"since" => DateTime.to_iso8601(since)}
      args = if min_score, do: Map.put(args, "min_score", min_score), else: args
      args = if use_filter?, do: Map.put(args, "filter", term), else: args

      {result, false} = payload("list_anomalies", args)

      # The `since` instant moves while the request runs, so compare with a margin
      # of a few seconds around items that sit on the boundary.
      expected =
        anomalies
        |> Enum.filter(fn a ->
          (is_nil(a.end_time) or DateTime.diff(a.end_time, since, :second) > 5) and
            (is_nil(min_score) or (a.score != nil and a.score >= min_score)) and
            (not use_filter? or matches?(a.series, term))
        end)
        |> Enum.map(& &1.id)

      returned = Enum.map(result["anomalies"], & &1["id"])
      assert expected -- returned == []
      assert Enum.all?(returned, &(&1 in Enum.map(anomalies, fn a -> a.id end)))

      for item <- result["anomalies"] do
        a = Enum.find(anomalies, &(&1.id == item["id"]))
        assert is_nil(min_score) or (a.score != nil and a.score >= min_score)
        assert not use_filter? or matches?(a.series, term)
      end

      {series_result, false} =
        payload("list_series", if(use_filter?, do: %{"filter" => term}, else: %{}))

      expected_series =
        pool |> Enum.filter(&(not use_filter? or matches?(&1, term))) |> Enum.map(& &1.id)

      assert Enum.sort(Enum.map(series_result["series"], & &1["id"])) == Enum.sort(expected_series)
    end
  end

  defp matches?(%{name: name, labels: labels}, term) do
    String.contains?(name, term) or
      Enum.any?(labels, fn {k, v} -> String.contains?(k, term) or String.contains?(v, term) end)
  end

  # Feature: v1, Property 60: query_series respects the point cap
  property "query_series never returns more than the cap, and never more than max_points" do
    check all(
            count <- integer(0..2_500),
            max_points <- one_of([constant(nil), integer(1..500)]),
            tier <- member_of([:raw, :minute, :hour])
          ) do
      now = DateTime.utc_now()
      points = for i <- 1..count//1, do: %{ts: DateTime.add(now, -i, :second), value: i * 1.0}
      series = %{id: @now_series_id, name: "m", labels: %{}, kind: "gauge", status: "healthy"}
      FakeData.put(series: [series], chart: %{tier: tier, points: points, bands: []})

      args = %{"series_id" => @now_series_id}
      args = if max_points, do: Map.put(args, "max_points", max_points), else: args
      {result, false} = payload("query_series", args)

      cap = max_points || 500
      assert length(result["points"]) <= min(cap, 500)
      assert result["count"] == length(result["points"])
      assert result["resolution_used"] == %{raw: "raw", minute: "1m", hour: "1h"}[tier]
      if count > 0, do: assert(result["points"] != [])
    end
  end

  # Feature: v1, Property 61: MCP output budget
  property "results never exceed 32 KiB, keep whole items, and flag truncation only when items are left out" do
    check all(
            count <- integer(0..400),
            name_length <- integer(1..256)
          ) do
      pool =
        for i <- 1..max(count, 1) do
          %{
            id: "00000000-0000-0000-0000-#{String.pad_leading(Integer.to_string(i), 12, "0")}",
            name: String.duplicate("n", name_length),
            labels: %{"l" => String.duplicate("v", name_length)},
            kind: "gauge",
            status: "healthy"
          }
        end

      FakeData.put(series: Enum.take(pool, count))
      conn = http(call_json("list_series", %{}))
      %{"result" => %{"content" => [%{"text" => text}]}} = Jason.decode!(conn.resp_body)

      assert byte_size(text) <= Tools.budget_bytes()
      result = Jason.decode!(text)
      assert result["count"] == length(result["series"])
      assert result["count"] <= count

      # Whole items, in order, as a prefix of the full answer.
      ids = Enum.take(Enum.map(pool, & &1.id), result["count"])
      assert Enum.map(result["series"], & &1["id"]) == ids

      if result["count"] < count,
        do: assert(result["truncated"] == true),
        else: refute(Map.has_key?(result, "truncated"))

      # Truncation is tight: the next item (all have the same size) would not fit.
      if result["truncated"] == true and result["count"] > 0 do
        item_size = result["series"] |> List.last() |> Jason.encode!() |> byte_size()
        assert byte_size(text) + item_size + 1 > Tools.budget_bytes()
      end
    end
  end

  # Feature: v1, Property 62: Invalid MCP parameters are named
  property "every invalid parameter is named in the error, and no valid one is" do
    bad = %{
      "since" => ["not a time", 5, "2999-01-01T00:00:00Z", nil, true],
      "min_score" => [-1, 101, "high", nil, [1]],
      "filter" => ["", 12, String.duplicate("x", 257), nil]
    }

    good = %{
      "since" => "2020-01-01T00:00:00Z",
      "min_score" => 10,
      "filter" => "ok"
    }

    check all(
            broken <- list_of(member_of(Map.keys(bad)), max_length: 3),
            values <- list_of(integer(0..4), length: 3),
            extra? <- boolean()
          ) do
      broken = Enum.uniq(broken)

      args =
        good
        |> Map.new(fn {name, value} ->
          if name in broken do
            options = Map.fetch!(bad, name)

            {name,
             Enum.at(
               options,
               Enum.at(values, Enum.find_index(Map.keys(bad), &(&1 == name)))
               |> rem(length(options))
             )}
          else
            {name, value}
          end
        end)

      args = if extra?, do: Map.put(args, "bogus", 1), else: args
      expected = Enum.sort(broken ++ if(extra?, do: ["bogus"], else: []))

      FakeData.put(series: [], anomalies: [], heartbeat: DateTime.utc_now())
      {result, error?} = payload("list_anomalies", args)

      if expected == [] do
        assert error? == false
        assert is_list(result["anomalies"])
      else
        assert error? == true
        assert result["error"]["code"] == "invalid_parameters"
        assert Enum.sort(result["error"]["parameters"]) == expected
        for name <- expected, do: assert(result["error"]["message"] =~ name)
      end
    end
  end

  # Feature: v1, Property 63: Untrusted values cannot escape their marking
  property "a marked value holds one open tag, one close tag, and nothing that could form a tag" do
    tricky =
      one_of([
        string(:printable),
        map(
          list_of(member_of(["<", ">", "&", "</untrusted>", "<untrusted>", "x", "\n", "é"])),
          &Enum.join/1
        ),
        map(integer(250..300), &String.duplicate("<", &1))
      ])

    check all(value <- tricky) do
      marked = Untrusted.mark(value)

      assert String.starts_with?(marked, "<untrusted>")
      assert String.ends_with?(marked, "</untrusted>")

      inner =
        marked
        |> String.replace_prefix("<untrusted>", "")
        |> String.replace_suffix("</untrusted>", "")

      refute String.contains?(inner, "<")
      refute String.contains?(inner, ">")
      assert length(Regex.scan(~r/<untrusted>/, marked)) == 1
      assert length(Regex.scan(~r/<\/untrusted>/, marked)) == 1

      unescaped =
        inner
        |> String.replace("&lt;", "<")
        |> String.replace("&gt;", ">")
        |> String.replace("&amp;", "&")

      assert unescaped == String.slice(value, 0, 256)
    end
  end

  property "values reach the client only through the marking" do
    check all(
            name <- string(:printable, max_length: 40),
            label <- string(:printable, max_length: 40)
          ) do
      series = %{
        id: @now_series_id,
        name: name,
        labels: %{label => name},
        kind: "gauge",
        status: "healthy"
      }

      FakeData.put(series: [series])
      {result, false} = payload("list_series", %{})
      [item] = result["series"]
      assert item["name"] == Untrusted.mark(name)
      assert item["labels"] == [%{"name" => Untrusted.mark(label), "value" => Untrusted.mark(name)}]
    end
  end
end

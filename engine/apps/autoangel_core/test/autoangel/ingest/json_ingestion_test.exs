defmodule Autoangel.Ingest.JsonIngestionTest do
  @moduledoc """
  Tasks 37.1 and 37.2: `POST /api/v1/push/json` (Property 4).
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Plug.Conn

  alias Autoangel.Exposition.Sample
  alias Autoangel.HTTP.Ingest, as: HTTPIngest
  alias Autoangel.Ingest.JsonSample
  alias Autoangel.SeriesRegistry

  setup_all do
    Application.put_env(:plug, :validate_header_keys_during_test, false)
    :ok
  end

  defp metric_name do
    gen all(
          first <- member_of(Enum.to_list(?a..?z) ++ [?_, ?:]),
          rest <- string([?a..?z, ?0..?9, ?_, ?:], max_length: 10)
        ) do
      <<first>> <> rest
    end
  end

  defp label_name do
    gen all(
          first <- member_of(Enum.to_list(?a..?z) ++ [?_]),
          rest <- string([?a..?z, ?0..?9, ?_], max_length: 8)
        ) do
      <<first>> <> rest
    end
  end

  defp labels, do: map_of(label_name(), string(:printable, max_length: 12), max_length: 4)

  defp valid_object do
    gen all(
          name <- metric_name(),
          value <- one_of([integer(-1_000_000..1_000_000), float(min: -1.0e9, max: 1.0e9)]),
          timestamp <- one_of([constant(:absent), constant(nil), integer(0..4_000_000_000_000)]),
          labels <- one_of([constant(:absent), constant(nil), labels()])
        ) do
      %{"name" => name, "value" => value}
      |> put_unless(timestamp, "timestamp")
      |> put_unless(labels, "labels")
    end
  end

  defp put_unless(object, :absent, _key), do: object
  defp put_unless(object, value, key), do: Map.put(object, key, value)

  # Feature: v1, Property 4: JSON ingestion validation
  property "a valid object becomes one Sample with exactly its fields" do
    check all(object <- valid_object()) do
      assert {:ok, %Sample{} = sample} = object |> JSON.encode!() |> JsonSample.parse()
      assert sample.name == object["name"]
      assert sample.value == object["value"] * 1.0
      assert sample.labels == (object["labels"] || %{})
      assert sample.timestamp == object["timestamp"]
    end
  end

  property "a body that is not one JSON object is not_a_json_object" do
    scalar =
      one_of([
        integer(),
        float(),
        boolean(),
        constant(nil),
        string(:printable),
        list_of(integer(), max_length: 3),
        list_of(valid_object(), max_length: 2)
      ])

    check all(value <- scalar) do
      assert {:error, :not_a_json_object} = value |> JSON.encode!() |> JsonSample.parse()
    end

    for body <- ["", "{", "{\"name\":", "not json", "\u0000", "{} {}"] do
      assert {:error, :not_a_json_object} = JsonSample.parse(body)
    end
  end

  property "a missing or null name or value is missing_field naming it" do
    check all(
            object <- valid_object(),
            field <- member_of(["name", "value"]),
            null? <- boolean()
          ) do
      broken = if null?, do: Map.put(object, field, nil), else: Map.delete(object, field)
      assert {:error, {:missing_field, ^field}} = broken |> JSON.encode!() |> JsonSample.parse()
    end
  end

  property "a wrong field is invalid_field naming it" do
    bad = %{
      "name" => [1, "", "9lives", "has space", "bad-dash", ["x"], true, %{}],
      "value" => ["1", "NaN", true, [1], %{}, "x"],
      "timestamp" => [
        1.5,
        "1700000000",
        true,
        [1],
        %{},
        9_223_372_036_854_775_808,
        -9_223_372_036_854_775_809
      ],
      "labels" => [
        "x",
        1,
        [1],
        true,
        %{"ok" => 1},
        %{"bad name" => "v"},
        %{"1x" => "v"},
        %{"a" => nil}
      ]
    }

    check all(
            object <- valid_object(),
            field <- member_of(Map.keys(bad)),
            index <- integer(0..20)
          ) do
      options = Map.fetch!(bad, field)
      broken = Map.put(object, field, Enum.at(options, rem(index, length(options))))

      assert {:error, {:invalid_field, ^field}} =
               broken |> JSON.encode!() |> JsonSample.parse()
    end
  end

  test "an object with a bad name and a bad value reports the name first" do
    assert {:error, {:invalid_field, "name"}} =
             JsonSample.parse(~s({"name":"1x","value":"x"}))

    assert {:error, {:missing_field, "name"}} = JsonSample.parse("{}")
    assert {:error, {:missing_field, "value"}} = JsonSample.parse(~s({"name":"m"}))
  end

  test "an absent or null optional field is not given" do
    assert {:ok, %Sample{timestamp: nil, labels: %{}}} =
             JsonSample.parse(~s({"name":"m","value":1}))

    assert {:ok, %Sample{timestamp: nil, labels: %{}}} =
             JsonSample.parse(~s({"name":"m","value":1,"timestamp":null,"labels":null}))
  end

  test "a number too large for a float is an invalid value" do
    assert {:error, {:invalid_field, "value"}} =
             JsonSample.parse(~s({"name":"m","value":1#{String.duplicate("0", 400)}}))
  end

  describe "over HTTP" do
    @token "json-ingest-token"

    setup do
      if pid = Process.whereis(SeriesRegistry), do: GenServer.stop(pid)
      {:ok, registry} = SeriesRegistry.start_link(per_metric_limit: 100, total_limit: 1_000)
      Process.unlink(registry)

      on_exit(fn ->
        if Process.alive?(registry), do: GenServer.stop(registry)
      end)

      opts = [
        role: :ingest,
        config: %{ingest: %{token: @token}, ingest_rate_limit: %{rate: 1_000, burst: 1_000}},
        table: String.to_atom("json_ingest_#{System.unique_integer([:positive])}"),
        worker_fun: fn _index -> self() end,
        storage_writer: self(),
        receive_time: 5_000
      ]

      {:ok, pipeline: HTTPIngest.init(opts)}
    end

    defp post_json(body, pipeline, token \\ @token) do
      conn = Plug.Test.conn(:post, "/api/v1/push/json", body)
      conn = if token, do: put_req_header(conn, "authorization", "Bearer " <> token), else: conn
      HTTPIngest.call(put_req_header(conn, "content-type", "application/json"), pipeline)
    end

    test "a valid object is accepted and reaches the Workers and the Storage_Writer", %{
      pipeline: pipeline
    } do
      conn = post_json(~s({"name":"json_metric","value":4.5,"labels":{"host":"a"}}), pipeline)
      assert conn.status == 202
      assert JSON.decode!(conn.resp_body) == %{"accepted" => 1, "rejected_cardinality" => 0}

      assert_receive {:"$gen_cast", {:samples, [%Sample{name: "json_metric", value: 4.5} = sample]}}
      assert sample.labels == %{"host" => "a"}
      # No timestamp was given: the receive time stands in (the same as a push).
      assert sample.received_at == 5_000
      assert_receive {:"$gen_cast", {:samples, [%Sample{name: "json_metric"}]}}
    end

    test "a given timestamp is kept", %{pipeline: pipeline} do
      conn = post_json(~s({"name":"json_ts","value":1,"timestamp":1700000000123}), pipeline)
      assert conn.status == 202
      assert_receive {:"$gen_cast", {:samples, [%Sample{timestamp: 1_700_000_000_123}]}}
    end

    test "errors are 400 with the error body and nothing is ingested", %{pipeline: pipeline} do
      before = SeriesRegistry.count()

      for {body, expected} <- [
            {"[1]", %{"error" => "not_a_json_object"}},
            {"{", %{"error" => "not_a_json_object"}},
            {~s({"value":1}), %{"error" => "missing_field", "field" => "name"}},
            {~s({"name":"m"}), %{"error" => "missing_field", "field" => "value"}},
            {~s({"name":"m","value":"x"}), %{"error" => "invalid_field", "field" => "value"}},
            {~s({"name":"m","value":1,"timestamp":1.5}),
             %{"error" => "invalid_field", "field" => "timestamp"}},
            {~s({"name":"m","value":1,"labels":{"a":1}}),
             %{"error" => "invalid_field", "field" => "labels"}}
          ] do
        conn = post_json(body, pipeline)
        assert conn.status == 400
        assert JSON.decode!(conn.resp_body) == expected
        assert get_resp_header(conn, "content-type") |> List.first() =~ "application/json"
      end

      refute_received {:"$gen_cast", {:samples, _}}
      assert SeriesRegistry.count() == before
    end

    test "the same credentials and limits apply as for a push", %{pipeline: pipeline} do
      assert post_json(~s({"name":"m","value":1}), pipeline, nil).status == 401
      assert post_json(~s({"name":"m","value":1}), pipeline, "wrong").status == 401
      refute_received {:"$gen_cast", {:samples, _}}
    end

    test "a body over the size limit is 413", %{pipeline: _pipeline} do
      pipeline =
        HTTPIngest.init(
          role: :ingest,
          config: %{ingest: %{token: @token}, ingest_rate_limit: %{rate: 1_000, burst: 1_000}},
          table: String.to_atom("json_ingest_big_#{System.unique_integer([:positive])}"),
          worker_fun: fn _index -> self() end,
          max_body_size: 50
        )

      body = ~s({"name":"m","value":1,"labels":{"a":"#{String.duplicate("x", 200)}"}})
      assert post_json(body, pipeline).status == 413
    end
  end
end

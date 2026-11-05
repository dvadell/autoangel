defmodule Autoangel.Ingest.RateLimitTest do
  use ExUnit.Case, async: false

  import Plug.Conn

  alias Autoangel.Ingest.RateLimit

  setup_all do
    Application.put_env(:plug, :validate_header_keys_during_test, false)
    :ok
  end

  test "allows the configured burst and rejects the next request" do
    table = unique_table()
    opts = [table: table, config: %{ingest_rate_limit: %{rate: 1, burst: 2}}, clock: fn -> 0 end]
    limiter = RateLimit.init(opts)
    conn = request("credential-a")

    assert RateLimit.call(conn, limiter).status == nil
    assert RateLimit.call(conn, limiter).status == nil
    limited = RateLimit.call(conn, limiter)
    assert limited.status == 429
    assert limited.halted
    assert get_resp_header(limited, "retry-after") == ["1"]
  end

  test "refills tokens lazily" do
    table = unique_table()
    clock = Agent.start_link(fn -> 0 end) |> elem(1)

    opts = [
      table: table,
      config: %{ingest_rate_limit: %{rate: 1, burst: 1}},
      clock: fn -> Agent.get(clock, & &1) end
    ]

    limiter = RateLimit.init(opts)
    assert RateLimit.call(request("credential-a"), limiter).status == nil
    assert RateLimit.call(request("credential-a"), limiter).status == 429
    Agent.update(clock, &(&1 + 1_000_000))
    assert RateLimit.call(request("credential-a"), limiter).status == nil
  end

  test "buckets are independent for each credential" do
    table = unique_table()
    opts = [table: table, rate: 1, burst: 1, clock: fn -> 0 end]
    limiter = RateLimit.init(opts)

    assert RateLimit.call(request("credential-a"), limiter).status == nil
    assert RateLimit.call(request("credential-a"), limiter).status == 429
    assert RateLimit.call(request("credential-b"), limiter).status == nil
  end

  defp request(credential_id) do
    Plug.Test.conn(:post, "/api/v1/push")
    |> assign(:credential_id, credential_id)
  end

  defp unique_table do
    String.to_atom("rate_limit_test_#{System.unique_integer([:positive])}")
  end
end

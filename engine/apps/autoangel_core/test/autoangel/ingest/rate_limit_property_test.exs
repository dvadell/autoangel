defmodule Autoangel.Ingest.RateLimitPropertyTest do
  @moduledoc """
  Property 51 (Requirements 74.4, 74.5): per-credential rate limits.

  "For any interleaving of requests from several ingest credentials, a request
  is rejected with 429 exactly when its own credential's token bucket is empty,
  a rejected request ingests nothing and creates no Series, and the decisions
  for each credential equal those it would get if the other credentials sent
  nothing."

  The rate limiter (`Autoangel.Ingest.RateLimit`) is a per-credential token
  bucket keyed on `conn.assigns[:credential_id]`. Each request costs exactly one
  token; refill is proportional to elapsed clock time.

  To prove the bucket semantics without fighting microtoken rounding, the clock
  is held FIXED inside each burst window so that no refill happens. With a fixed
  clock an independent oracle is trivially exact: for a given credential the
  first `burst` requests are allowed and every request after that is rejected,
  regardless of what other credentials do. A separate deterministic (non-random)
  test advances an Agent clock to prove refill restores capacity.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Plug.Conn
  import StreamData

  alias Autoangel.HTTP.Ingest, as: HTTPIngest
  alias Autoangel.Ingest.RateLimit
  alias Autoangel.SeriesRegistry

  setup_all do
    Application.put_env(:plug, :validate_header_keys_during_test, false)
    :ok
  end

  # ---------------------------------------------------------------------------
  # Property 51: per-credential independence + exact-empty rejection.
  # ---------------------------------------------------------------------------

  # The two properties below exercise the SAME Property 51 guarantees against
  # the TWO distinct bucket-keying paths in production
  # (`Autoangel.Ingest.RateLimit.credential_id/1`):
  #
  #   * the `conn.assigns[:credential_id]` path (set downstream of auth), and
  #   * the `{:credential, sha256(authorization_header)}` fallback used when no
  #     assign is present.
  #
  # Both share the interleave/oracle/independence logic in
  # `assert_property_51/5`; they differ only in how each identity's `conn` is
  # built. The identities are opaque to the shared logic -- it only cares that
  # distinct identities map to distinct buckets.

  property "429 exactly when an assign-keyed credential's own bucket is empty, independent of others" do
    check all(
            identities <- identities(),
            burst <- integer(1..5),
            rate <- member_of([1, 5, 50]),
            sequence <- request_sequence(identities),
            max_runs: 60
          ) do
      assert_property_51(identities, burst, rate, sequence, &assign_conn/1)
    end
  end

  # Same guarantees, but each identity is a DISTINCT authorization header and no
  # `:credential_id` assign is set. This drives the production fallback that
  # keys the bucket on `sha256(authorization_header)`, proving that path also
  # isolates credentials. The header values are arbitrary distinct strings:
  # `RateLimit.call/2` only hashes the header, it does not authenticate.
  property "429 exactly when a header-keyed credential's own bucket is empty, independent of others" do
    check all(
            identities <- authorization_headers(),
            burst <- integer(1..5),
            rate <- member_of([1, 5, 50]),
            sequence <- request_sequence(identities),
            max_runs: 60
          ) do
      assert_property_51(identities, burst, rate, sequence, &header_conn/1)
    end
  end

  # Shared Property 51 oracle + independence check. `build_conn` maps an opaque
  # identity to a request `conn`; distinct identities must land in distinct
  # buckets. The clock is held FIXED so no refill happens within a run: a given
  # identity's first `burst` requests are allowed and every later one is 429'd,
  # regardless of what other identities do.
  defp assert_property_51(identities, burst, rate, sequence, build_conn) do
    clock = fn -> 0 end

    # --- Interleaved run: all identities share one bucket table. ---
    interleaved_table = unique_table()
    interleaved_opts = [table: interleaved_table, rate: rate, burst: burst, clock: clock]
    interleaved_limiter = RateLimit.init(interleaved_opts)

    {interleaved_decisions, _} =
      Enum.map_reduce(sequence, %{}, fn identity, counts ->
        conn = RateLimit.call(build_conn.(identity), interleaved_limiter)
        seen = Map.get(counts, identity, 0)

        # Assertion 1: an identity is 429'd exactly when its own bucket is
        # empty. With a fixed clock the bucket empties after `burst` allowed
        # requests, so the oracle decision depends ONLY on how many requests
        # THIS identity has already made -- never on the others.
        oracle = if seen < burst, do: :allow, else: :reject
        assert decision(conn) == oracle

        if oracle == :reject do
          # Assertion 4: 429 responses carry an integer retry-after header.
          assert conn.status == 429
          assert conn.halted
          assert [retry_after] = get_resp_header(conn, "retry-after")
          assert {value, ""} = Integer.parse(retry_after)
          assert value >= 1
        end

        {{identity, decision(conn)}, Map.put(counts, identity, seen + 1)}
      end)

    # --- Assertion 2 (INDEPENDENCE): replay each identity's subsequence ALONE
    # against a fresh table and assert identical decisions. This is the "as if
    # the other credentials sent nothing" clause of Property 51. ---
    for identity <- identities do
      alone_table = unique_table()
      alone_opts = [table: alone_table, rate: rate, burst: burst, clock: clock]
      alone_limiter = RateLimit.init(alone_opts)

      alone_decisions =
        sequence
        |> Enum.filter(&(&1 == identity))
        |> Enum.map(fn ^identity ->
          {identity, decision(RateLimit.call(build_conn.(identity), alone_limiter))}
        end)

      from_interleaved =
        Enum.filter(interleaved_decisions, fn {id, _} -> id == identity end)

      assert alone_decisions == from_interleaved
    end
  end

  # ---------------------------------------------------------------------------
  # Property 51 (continued): a rejected request ingests nothing / creates no
  # Series, while an allowed request delivers samples. Driven through the full
  # `Autoangel.HTTP.Ingest` pipeline (auth + rate limit + ingest).
  # ---------------------------------------------------------------------------

  test "a 429 rejected request ingests nothing and creates no Series; an allowed request does" do
    registry = start_registry(per_metric_limit: 100, total_limit: 1_000)
    Process.unlink(registry)
    on_exit(fn -> safe_stop(registry) end)

    table = unique_table()
    token = "ingest-secret-token"

    config = %{
      ingest: %{token: token},
      ingest_rate_limit: %{rate: 1, burst: 1}
    }

    opts = [
      role: :ingest,
      config: config,
      table: table,
      clock: fn -> 0 end,
      worker_fun: fn _index -> self() end,
      storage_writer: self()
    ]

    pipeline = HTTPIngest.init(opts)
    body = "rate_limit_prop_metric 1\n"

    before_count = SeriesRegistry.count()

    # First request: bucket has one token -> allowed. The pipeline delivers the
    # samples to both worker_fun pid and storage_writer (this test process).
    allowed = HTTPIngest.call(push_conn(token, body), pipeline)
    assert allowed.status == 202
    refute allowed.halted
    # Both worker_fun and storage_writer receive the samples via GenServer.cast,
    # which arrives as {:"$gen_cast", {:samples, ...}}.
    assert_receive {:"$gen_cast", {:samples, [_ | _]}}
    # Drain the second cast (worker vs storage ordering is irrelevant).
    assert_receive {:"$gen_cast", {:samples, _}}, 200

    after_allowed_count = SeriesRegistry.count()
    assert after_allowed_count == before_count + 1

    # Second request at the same clock value: bucket empty -> 429. It must halt
    # before any ingest work: no samples delivered and no new Series created.
    rejected = HTTPIngest.call(push_conn(token, body), pipeline)
    assert rejected.status == 429
    assert rejected.halted
    assert [retry_after] = get_resp_header(rejected, "retry-after")
    assert {value, ""} = Integer.parse(retry_after)
    assert value >= 1

    refute_receive {:"$gen_cast", {:samples, _}}, 100
    assert SeriesRegistry.count() == after_allowed_count
  end

  # ---------------------------------------------------------------------------
  # Deterministic refill: advancing the clock restores capacity. Uses an Agent
  # clock advanced by whole-token steps to keep the microtoken math exact.
  # ---------------------------------------------------------------------------

  test "advancing the clock refills exactly the expected number of tokens" do
    {:ok, clock_agent} = Agent.start_link(fn -> 0 end)
    on_exit(fn -> stop(clock_agent) end)

    rate = 5
    burst = 3
    table = unique_table()

    opts = [
      table: table,
      rate: rate,
      burst: burst,
      clock: fn -> Agent.get(clock_agent, & &1) end
    ]

    limiter = RateLimit.init(opts)
    credential = "refill-credential"

    # Exhaust the burst.
    for _ <- 1..burst do
      assert decision(RateLimit.call(assign_conn(credential), limiter)) == :allow
    end

    assert decision(RateLimit.call(assign_conn(credential), limiter)) == :reject

    # Advancing by K whole tokens' worth of time (each token is 1 / rate
    # seconds, i.e. 1_000_000 / rate microtoken-clock units) refills exactly K
    # tokens -> exactly K more requests are allowed, then rejection resumes.
    refill_k = 2
    Agent.update(clock_agent, &(&1 + div(1_000_000, rate) * refill_k))

    for _ <- 1..refill_k do
      assert decision(RateLimit.call(assign_conn(credential), limiter)) == :allow
    end

    assert decision(RateLimit.call(assign_conn(credential), limiter)) == :reject
  end

  # ---------------------------------------------------------------------------
  # Generators and helpers.
  # ---------------------------------------------------------------------------

  defp identities do
    gen all(
          count <- integer(2..4),
          ids <-
            list_of(credential_id_string(), length: count)
            |> filter(fn ids -> length(Enum.uniq(ids)) == count end)
        ) do
      Enum.uniq(ids)
    end
  end

  defp credential_id_string do
    map(integer(1..1_000_000), fn n -> "cred-#{n}" end)
  end

  # Distinct authorization header values. They need not be valid against any
  # config: RateLimit.call/2 only hashes the header to key the bucket.
  defp authorization_headers do
    gen all(
          count <- integer(2..4),
          headers <-
            list_of(authorization_header_string(), length: count)
            |> filter(fn hs -> length(Enum.uniq(hs)) == count end)
        ) do
      Enum.uniq(headers)
    end
  end

  defp authorization_header_string do
    map(integer(1..1_000_000), fn n -> "Bearer token-#{n}" end)
  end

  defp request_sequence(identities) do
    list_of(member_of(identities), min_length: 1, max_length: 20)
  end

  defp decision(conn) do
    if conn.halted or conn.status == 429, do: :reject, else: :allow
  end

  # Conn builder for the assign-keyed bucket path.
  defp assign_conn(credential_id) do
    Plug.Test.conn(:post, "/api/v1/push")
    |> assign(:credential_id, credential_id)
  end

  # Conn builder for the header-keyed (sha256) fallback path: set the
  # authorization header and DO NOT set the credential_id assign.
  defp header_conn(authorization) do
    Plug.Test.conn(:post, "/api/v1/push")
    |> put_req_header("authorization", authorization)
  end

  defp push_conn(token, body) do
    Plug.Test.conn(:post, "/api/v1/push", body)
    |> put_req_header("authorization", "Bearer " <> token)
    |> put_req_header("content-type", "text/plain")
  end

  defp unique_table do
    String.to_atom("rl_prop_#{System.unique_integer([:positive])}")
  end

  defp stop(pid) do
    if is_pid(pid) and Process.alive?(pid) do
      ref = Process.monitor(pid)
      Process.exit(pid, :normal)

      receive do
        {:DOWN, ^ref, :process, ^pid, _} -> :ok
      after
        1_000 -> :ok
      end
    end
  end

  # Start the named SeriesRegistry robustly: stop any already-running instance
  # first (a prior async: false file/iteration may have leaked one whose on_exit
  # has not run yet), and if we still race with a concurrent start, stop the
  # winner and retry once.
  defp start_registry(opts) do
    if pid = Process.whereis(SeriesRegistry), do: GenServer.stop(pid)

    case SeriesRegistry.start_link(opts) do
      {:ok, pid} ->
        pid

      {:error, {:already_started, running}} ->
        GenServer.stop(running)
        {:ok, pid} = SeriesRegistry.start_link(opts)
        pid
    end
  end

  # Stop a process tolerantly: the registry may already be gone, so swallow the
  # :exit raised when stopping a dead pid.
  defp safe_stop(pid) when is_pid(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
  catch
    :exit, _ -> :ok
  end

  defp safe_stop(_), do: :ok
end

defmodule Autoangel.Ingest.IngestStatusAndTlsTest do
  @moduledoc """
  Task 11.9 — example-based unit tests for the ingest status codes and the
  listener's all-or-nothing TLS policy.

  Validates Requirements 1.5, 1.8, 7.2, 7.6, 48.6, 74.5.

  These are DB-free, socket-free examples driving the REAL plugs. One example
  covers each row of the ingest error table, the body-size boundary (exactly the
  limit vs limit + 1), and each TLS misconfiguration branch:

    * 401 unauthorized (req 7.2) — no `authorization` header.
    * 400 invalid exposition (req 1.5) — the first invalid line is reported.
    * 413 request body too large (req 1.8) — N + 1 bytes rejected, exactly N
      bytes NOT rejected (the "body size at limit and limit + 1" requirement).
    * 503 engine overloaded (req 48.6) — a Worker queue reservation fails.
    * 429 rate limited (req 74.5) — the per-credential token bucket is exhausted.
    * 202 accepted (sanity) — a valid authed body within limits.

  TLS (req 7.6) exercises `Autoangel.HTTP.Listener.tls_options/1`,
  `listener_options/3`, and `init/1` directly — no sockets are bound.

  `async: false` because the ingest rows start the named `Autoangel.SeriesRegistry`
  and touch the shared `Autoangel.Workers.Worker` persistent_term namespace and
  the process-global `Autoangel.Ingest.Metrics` load-shed counter.
  """
  use ExUnit.Case, async: false

  import Plug.Test
  import Plug.Conn
  import ExUnit.CaptureLog

  alias Autoangel.HTTP.Ingest, as: HTTPIngest
  alias Autoangel.HTTP.Listener
  alias Autoangel.Ingest
  alias Autoangel.Ingest.Metrics
  alias Autoangel.Secret
  alias Autoangel.SeriesRegistry
  alias Autoangel.Workers.Worker

  @token "ingest-token-11-9"
  @config %{ingest: %{token: Secret.new(@token)}}

  # A single-sample valid exposition body. Parsing yields one accepted sample.
  @valid_body "ingest_status_metric 1.0\n"

  setup_all do
    Application.put_env(:plug, :validate_header_keys_during_test, false)
    :ok
  end

  # The ingestion rows need the named SeriesRegistry. Start it with small limits
  # and a fixed pool so worker indices are deterministic; stop it on exit.
  setup do
    registry = start_registry(per_metric_limit: 100, total_limit: 100, pool_size: 1)

    on_exit(fn -> safe_stop(registry) end)

    :ok
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

  # --- Ingest error table ---------------------------------------------------

  test "401 unauthorized: no auth header is rejected and nothing is ingested (req 7.2)" do
    before = SeriesRegistry.count()

    conn =
      :post
      |> conn("/api/v1/push", @valid_body)
      |> put_req_header("content-type", "text/plain")

    result = HTTPIngest.call(conn, opts())

    assert result.status == 401
    assert result.resp_body == "unauthorized"
    assert result.halted
    # No worker enqueue and no storage cast happened for this request. Both the
    # worker (`Worker.enqueue/2`) and the storage writer receive samples via
    # `GenServer.cast/2`, so a delivered sample lands in the mailbox as
    # `{:"$gen_cast", {:samples, _}}` — the pattern below must match that shape
    # or the refute is vacuous.
    refute_receive {:"$gen_cast", {:samples, _}}
    assert SeriesRegistry.count() == before
  end

  test "400 invalid exposition: reports the first invalid line number (req 1.5)" do
    before = SeriesRegistry.count()
    # Line 1 is valid; line 2 has too many whitespace-separated tokens and fails
    # `split_sample/1`, so the first invalid line is line 2.
    body = "ingest_status_metric 1.0\nthis is not valid @@@\n"

    conn = authed_conn(body)
    result = HTTPIngest.call(conn, opts())

    assert result.status == 400
    assert result.resp_body =~ "invalid exposition at line 2"
    # Samples are delivered as GenServer casts, so assert against the
    # `{:"$gen_cast", {:samples, _}}` shape; nothing should be delivered here.
    refute_receive {:"$gen_cast", {:samples, _}}
    assert SeriesRegistry.count() == before
  end

  test "413 body too large: N + 1 is rejected but exactly N is not (req 1.8)" do
    before = SeriesRegistry.count()
    max = byte_size(@valid_body)

    # Exactly N bytes: the valid single-sample body is N bytes, so it proceeds
    # (202 accepted), never 413.
    at_limit = authed_conn(@valid_body)

    at_limit_result =
      Ingest.call(at_limit, opts(max_body_size: max, worker_fun: fn _ -> self() end))

    assert at_limit_result.status != 413
    assert at_limit_result.status == 202

    # N + 1 bytes: one byte over the limit is rejected before parsing.
    over_body = @valid_body <> "x"
    assert byte_size(over_body) == max + 1
    over = authed_conn(over_body)
    over_result = Ingest.call(over, opts(max_body_size: max, worker_fun: fn _ -> self() end))

    assert over_result.status == 413
    assert over_result.resp_body == "request body too large"
    # The oversize request is rejected without creating any new series beyond the
    # one accepted by the at-limit request above.
    assert SeriesRegistry.count() == before + 1
  end

  test "503 overloaded: a failed queue reservation sheds the load (req 48.6)" do
    before_count = SeriesRegistry.count()
    before_shed = Metrics.load_shed()

    # The pipeline hashes the single series to worker index 0 (pool_size: 1).
    # Seed a full per-index counter with zero capacity so `try_reserve/3` fails.
    index = 0
    counter = :atomics.new(1, signed: true)
    :atomics.put(counter, 1, 1)
    :persistent_term.put({Worker, index}, counter)
    :persistent_term.put({Worker, :capacity, index}, 0)

    on_exit(fn ->
      :persistent_term.erase({Worker, index})
      :persistent_term.erase({Worker, :capacity, index})
    end)

    conn = authed_conn(@valid_body)
    result = Ingest.call(conn, opts(worker_fun: fn _ -> self() end, pool_size: 1))

    assert result.status == 503
    assert result.resp_body == "engine overloaded"
    # The request carries exactly one sample (the single-line @valid_body), and
    # the whole batch is shed when the queue reservation fails, so the load-shed
    # counter advances by exactly that one sample.
    assert Metrics.load_shed() == before_shed + 1
    # Reservation is checked before series admission, so no series is created.
    assert SeriesRegistry.count() == before_count
  end

  test "429 rate limited: an exhausted bucket rejects with retry-after (req 74.5)" do
    before = SeriesRegistry.count()
    table = String.to_atom("ingest_status_rl_#{System.unique_integer([:positive])}")

    on_exit(fn ->
      case :ets.whereis(table) do
        :undefined -> :ok
        _ -> :ets.delete(table)
      end
    end)

    # A burst of 1 with a fixed clock: the first request consumes the only token,
    # the second is rate limited.
    rl_opts =
      opts(
        table: table,
        rate: 1,
        burst: 1,
        clock: fn -> 0 end,
        worker_fun: fn _ -> self() end
      )

    first = HTTPIngest.call(authed_conn(@valid_body), rl_opts)
    assert first.status == 202

    # The allowed request delivered its one sample via two GenServer casts — one
    # from the worker (`worker_fun` returns self/0) and one from the storage
    # writer (`storage_writer: self/0`). Drain BOTH before refuting the limited
    # request, otherwise the refute below would match the allowed request's
    # leftover cast and pass vacuously.
    assert_receive {:"$gen_cast", {:samples, [_]}}
    assert_receive {:"$gen_cast", {:samples, [_]}}

    limited = HTTPIngest.call(authed_conn(@valid_body), rl_opts)

    assert limited.status == 429
    assert limited.halted
    assert get_resp_header(limited, "retry-after") != []
    # The limited request never reached the pipeline: no storage/worker cast, and
    # no new series beyond the one the first (accepted) request created. With the
    # allowed request's casts drained above, a short-timeout refute now meaningfully
    # asserts the rejected request delivered nothing.
    refute_receive {:"$gen_cast", {:samples, _}}, 100
    assert SeriesRegistry.count() == before + 1
  end

  test "202 accepted: a valid authed body returns the accepted/rejected counts" do
    conn = authed_conn(@valid_body)
    result = HTTPIngest.call(conn, opts(worker_fun: fn _ -> self() end))

    assert result.status == 202
    assert result.resp_body == ~s({"accepted":1,"rejected_cardinality":0})
    assert get_resp_header(result, "content-type") == ["application/json; charset=utf-8"]
  end

  # --- TLS misconfiguration (req 7.6) ---------------------------------------

  test "cert present but key missing is rejected" do
    config = %{tls: %{cert_file: "/tmp/cert.pem", key_file: nil}}
    assert {:error, {:tls, :key_file, :missing_key}} = Listener.tls_options(config)
  end

  test "key present but cert missing is rejected" do
    config = %{tls: %{cert_file: nil, key_file: "/tmp/key.pem"}}
    assert {:error, {:tls, :cert_file, :missing_certificate}} = Listener.tls_options(config)
  end

  test "both present but the cert file is unreadable or invalid is rejected and logged" do
    junk = Path.join(System.tmp_dir!(), "ingest_status_junk_#{System.unique_integer([:positive])}")
    File.write!(junk, "not a pem certificate")
    on_exit(fn -> File.rm(junk) end)

    config = %{tls: %{cert_file: junk, key_file: junk}}

    log =
      capture_log(fn ->
        assert {:error, {:tls, :cert_file, :invalid_file}} = Listener.tls_options(config)
      end)

    assert log =~ junk
  end

  test "neither TLS file configured uses plain HTTP" do
    assert {:ok, %{cert_file: nil, key_file: nil}} = Listener.tls_options(%{})

    assert {:ok, options} =
             Listener.listener_options(:ingest, %{}, %{cert_file: nil, key_file: nil})

    assert options[:scheme] == :http
    refute Keyword.has_key?(options, :certfile)
  end

  test "TLS configured listener serves https with cert/key — never plain http (req 7.6)" do
    # Positive guarantee: when both TLS files are configured, the listener is
    # built for https and carries the cert/key through. `listener_options/3`
    # does NOT read the files, so opaque placeholders exercise the policy branch
    # without touching the filesystem.
    assert {:ok, tls_options} =
             Listener.listener_options(:ingest, %{}, %{cert_file: "c", key_file: "k"})

    assert tls_options[:scheme] == :https
    assert tls_options[:certfile] == "c"
    assert tls_options[:keyfile] == "k"

    # Contrast: with neither file configured the listener is plain http and
    # carries no TLS transport options. Taken together these two cases prove a
    # TLS-configured listener serves https and never silently falls back to
    # plain http.
    assert {:ok, plain_options} =
             Listener.listener_options(:ingest, %{}, %{cert_file: nil, key_file: nil})

    assert plain_options[:scheme] == :http
    refute Keyword.has_key?(plain_options, :certfile)
    refute Keyword.has_key?(plain_options, :keyfile)
  end

  test "init stops on a TLS misconfig instead of falling back to plain HTTP" do
    config = %{tls: %{cert_file: "/does/not/exist/cert.pem", key_file: nil}}

    log =
      capture_log(fn ->
        assert {:stop, {:tls, :key_file, :missing_key}} = Listener.init(config: config)
      end)

    assert log =~ "TLS"
  end

  # --- Helpers ---------------------------------------------------------------

  defp authed_conn(body) do
    :post
    |> conn("/api/v1/push", body)
    |> put_req_header("content-type", "text/plain")
    |> put_req_header("authorization", "Bearer #{@token}")
  end

  # Base plug options: config for auth/rate-limit plus a storage writer pointed at
  # the test process so `{:samples, _}` casts are observable (and their absence
  # assertable via `refute_receive/1`).
  defp opts(extra \\ []) do
    [config: @config, storage_writer: self()]
    |> Keyword.merge(extra)
  end
end

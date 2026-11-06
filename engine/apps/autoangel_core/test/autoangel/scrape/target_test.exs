defmodule Autoangel.Scrape.TargetTest do
  # async: false — these tests use the default-named Autoangel.SeriesRegistry for the
  # success/recovery path, so they must not run concurrently with other suites that
  # start the same named registry. Each test still carves out a UNIQUE ETS status
  # table so there is no shared scrape state between tests.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Autoangel.Scrape.Target

  # A validated target map shaped exactly like Target.validate/2 returns. A large
  # interval means the auto-scheduled :tick from init/handle_info never fires during a
  # test; every tick in these tests is driven manually via send(pid, :tick).
  defp target_map(id) do
    %{id: id, url: "http://127.0.0.1:0/metrics", interval: 3600, timeout: 5}
  end

  # Start one Target with a unique status table and the given fetcher. Returns
  # {pid, table, id}. The Target is stopped in on_exit with a crash-tolerant stop so a
  # lingering timer/process never leaks into another test.
  defp start_target!(fetcher, opts \\ []) do
    id = "target-#{System.unique_integer([:positive])}"
    table = :"scrape_status_#{System.unique_integer([:positive])}"
    :ets.new(table, [:named_table, :public, :set])

    start_opts =
      Keyword.merge(
        [target: target_map(id), status_table: table, fetcher: fetcher],
        opts
      )

    {:ok, pid} = Target.start_link(start_opts)
    Process.unlink(pid)

    on_exit(fn ->
      safe_stop(pid)
      if :ets.whereis(table) != :undefined, do: :ets.delete(table)
    end)

    {pid, table, id}
  end

  # Ensure the default-named SeriesRegistry is available for the success/recovery path.
  # The autoangel_core application supervises one under its default name during the
  # test run, so the registry-backed success path works against the already-running
  # instance. If it is somehow absent (e.g. a racing suite stopped it), start a fresh
  # one; this test neither stops nor replaces an existing supervised registry, which
  # avoids the restart race that killing the supervised process would trigger.
  defp ensure_registry! do
    case Process.whereis(Autoangel.SeriesRegistry) do
      nil ->
        case Autoangel.SeriesRegistry.start_link(
               per_metric_limit: 2_000,
               total_limit: 50_000,
               pool_size: 1
             ) do
          {:ok, pid} ->
            Process.unlink(pid)
            {:started, pid}

          {:error, {:already_started, pid}} ->
            {:existing, pid}
        end

      _pid ->
        :existing
    end
  end

  # Stop a process during teardown without failing the test if it is already exiting.
  defp safe_stop(pid) when is_pid(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
  catch
    :exit, _ -> :ok
  end

  defp safe_stop(_), do: :ok

  # Block until the Target's in_flight is set (a fetch Task has been spawned) or the
  # deadline passes. Reads the GenServer struct via :sys.get_state/1 — a test-only
  # introspection, not an implementation change.
  defp wait_until(_pid, _fun, 0), do: :timed_out

  defp wait_until(pid, fun, retries) do
    if fun.(:sys.get_state(pid)) do
      :ok
    else
      Process.sleep(10)
      wait_until(pid, fun, retries - 1)
    end
  end

  # A healthy exposition body with a UNIQUE metric name per call, so the globally
  # created series in the (shared, application-supervised) SeriesRegistry never collide
  # across test runs or seeds. Returns {metric_name, body}.
  defp valid_body do
    metric = "http_requests_total_#{System.unique_integer([:positive])}"
    {metric, "# TYPE #{metric} counter\n#{metric}{method=\"get\"} 5\n"}
  end

  describe "single-flight scheduling (Requirement 2.2)" do
    test "a tick while a fetch is in flight is skipped and starts no second fetch" do
      test_pid = self()

      # A fetcher that signals it started, then blocks until explicitly released, so the
      # first fetch is guaranteed to still be in flight when the second :tick arrives.
      fetcher = fn _target, _opts ->
        send(test_pid, {:fetch_started, self()})

        receive do
          {:release, reply} -> reply
        end
      end

      {pid, _table, _id} = start_target!(fetcher)

      # capture_log keeps the "scrape tick skipped" debug line out of the console while
      # still exercising the single-flight skip path. The skipped/in_flight assertions
      # below are unchanged.
      capture_log(fn ->
        # First tick: the fetch Task spawns and blocks. in_flight becomes the Task ref.
        send(pid, :tick)
        assert_receive {:fetch_started, fetch_pid}, 1_000
        assert :ok = wait_until(pid, &(&1.in_flight != nil), 100)
        in_flight_ref = :sys.get_state(pid).in_flight

        # Second tick WHILE in flight: it must be skipped. No new fetch is started, the
        # skipped counter increments to 1, and in_flight is unchanged.
        send(pid, :tick)
        assert :ok = wait_until(pid, &(&1.skipped == 1), 100)
        refute_received {:fetch_started, _}
        state_after_skip = :sys.get_state(pid)
        assert state_after_skip.skipped == 1
        assert state_after_skip.in_flight == in_flight_ref

        # Release the in-flight fetch with a well-formed 2xx empty-body result. Once the
        # result is handled, in_flight returns to nil.
        send(fetch_pid, {:release, {:ok, 200, ""}})
        assert :ok = wait_until(pid, &(&1.in_flight == nil), 100)
        assert :sys.get_state(pid).in_flight == nil
      end)
    end
  end

  describe "failure handling (Requirements 2.4, 2.5)" do
    test "a request failure / timeout marks the target failing and logs a warning (2.4)" do
      test_pid = self()

      # Capture the opts the Target passes to the fetcher so we can prove the configured
      # receive_timeout (the target's timeout, 5) is actually threaded through to the
      # fetch call, then return a timeout error to drive the failing path.
      fetcher = fn _target, opts ->
        send(test_pid, {:fetch_opts, opts})
        {:error, :timeout}
      end

      {pid, table, id} = start_target!(fetcher)

      log =
        capture_log(fn ->
          send(pid, :tick)
          assert :ok = wait_until(pid, &(&1.in_flight == nil), 100)
          assert wait_failing(table, id, true, 100)
        end)

      # The fetcher was invoked with the target's configured timeout as receive_timeout.
      assert_receive {:fetch_opts, fetch_opts}, 1_000
      assert fetch_opts[:receive_timeout] == target_map(id).timeout

      assert Target.failing?(table, id) == true
      assert log =~ "scrape failed"
      assert log =~ ":timeout"
    end

    test "a non-2xx response marks the target failing and logs the status (2.5)" do
      fetcher = fn _target, _opts -> {:ok, 503, "service unavailable"} end
      {pid, table, id} = start_target!(fetcher)

      log =
        capture_log(fn ->
          send(pid, :tick)
          assert :ok = wait_until(pid, &(&1.in_flight == nil), 100)
          assert wait_failing(table, id, true, 100)
        end)

      assert Target.failing?(table, id) == true
      assert log =~ "HTTP 503"
    end

    test "an invalid exposition body marks the target failing and logs the bad line (2.5)" do
      fetcher = fn _target, _opts -> {:ok, 200, "not valid exposition\n@@@"} end
      {pid, table, id} = start_target!(fetcher)

      log =
        capture_log(fn ->
          send(pid, :tick)
          assert :ok = wait_until(pid, &(&1.in_flight == nil), 100)
          assert wait_failing(table, id, true, 100)
        end)

      assert Target.failing?(table, id) == true
      assert log =~ "invalid exposition"
      assert log =~ "first invalid line"
      # The warning names the target (Requirement 2.5).
      assert log =~ id
    end
  end

  describe "recovery after a healthy scrape (Requirement 2.6)" do
    test "a successful scrape clears a previously failing target" do
      # The success path routes parsed exposition through Autoangel.Ingest, which needs
      # a running SeriesRegistry. worker_fun returns the test pid so enqueue is a no-op
      # cast to us, and storage_writer is the test pid for the same reason; neither
      # requires a started Workers pool. We reuse the application-supervised registry
      # rather than replacing it, which avoids a supervisor restart race.
      registry = ensure_registry!()

      on_exit(fn ->
        case registry do
          {:started, pid} -> safe_stop(pid)
          _ -> :ok
        end
      end)

      # An Agent hands out the next canned fetch result per tick: first a non-2xx
      # failure, then a healthy 2xx body. This lets a single Target transition
      # failing -> healthy across two deterministic ticks. The healthy body uses a
      # UNIQUE metric name so the shared SeriesRegistry never collides across seeds.
      {_metric, body} = valid_body()

      {:ok, responses} =
        Agent.start_link(fn -> [{:ok, 503, "down"}, {:ok, 200, body}] end)

      on_exit(fn -> safe_stop(responses) end)

      fetcher = fn _target, _opts ->
        Agent.get_and_update(responses, fn
          [next | rest] -> {next, rest}
          [] -> {{:ok, 200, ""}, []}
        end)
      end

      # enqueue routes accepted samples to worker_fun.(index) and storage_writer via a
      # GenServer cast. Both must be the TEST process (not the Target) so the casts are
      # harmless no-ops we simply ignore — casting {:samples, _} at the Target GenServer
      # would crash it, since Target handles no such message. self() is captured here, in
      # the test process, rather than inside the worker_fun closure (which the Target
      # would evaluate as its own pid).
      test_pid = self()

      {pid, table, id} =
        start_target!(fetcher,
          processor_opts: [
            worker_fun: fn _ -> test_pid end,
            storage_writer: test_pid,
            pool_size: 1
          ]
        )

      capture_log(fn ->
        # Tick 1: the 503 marks the target failing. A failed scrape ingests nothing, so
        # no samples cast should arrive at the test pid. This refute runs BEFORE the
        # success tick, so it cannot consume the later success message.
        send(pid, :tick)
        assert :ok = wait_until(pid, &(&1.in_flight == nil), 100)
        assert wait_failing(table, id, true, 100)
        assert Target.failing?(table, id) == true
        refute_receive {:"$gen_cast", {:samples, _}}, 100

        # Tick 2: a healthy scrape clears the failing status (true -> false) AND actually
        # routes parsed samples through Processor.process -> Ingest.process_samples, which
        # casts {:samples, _} to the storage_writer / worker_fun test pid. Asserting the
        # cast arrives proves ingestion really happened, not merely that the failing flag
        # flipped (the flag is cleared even on {:error, :overloaded}).
        send(pid, :tick)
        assert :ok = wait_until(pid, &(&1.in_flight == nil), 100)
        assert wait_failing(table, id, false, 100)
        assert_receive {:"$gen_cast", {:samples, [_ | _]}}, 1_000
      end)

      assert Target.failing?(table, id) == false
    end
  end

  # Poll the ETS status table until failing? matches the expected boolean or the
  # deadline passes. Status is written asynchronously after the Task result is handled,
  # so a short poll avoids racing the GenServer.
  defp wait_failing(_table, _id, _expected, 0), do: false

  defp wait_failing(table, id, expected, retries) do
    if Target.failing?(table, id) == expected do
      true
    else
      Process.sleep(10)
      wait_failing(table, id, expected, retries - 1)
    end
  end
end

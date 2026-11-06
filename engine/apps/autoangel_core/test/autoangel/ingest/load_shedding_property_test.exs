defmodule Autoangel.Ingest.LoadSheddingPropertyTest do
  @moduledoc """
  Property 49 (Requirement 48.6) — all-or-nothing load shedding.

  "For any Worker queue fill levels and any request, either every Sample in the
  request is admitted or none is; on rejection the queued items are unchanged,
  the load-shedding counter grows by the request's Sample count, the reply is
  503, and no Series is created."

  The real admission pipeline lives in `Autoangel.Ingest.process_samples/2`. It
  groups the request's Samples by Worker index and then performs an
  all-or-nothing capacity reservation through `reserve_all/2`, which calls
  `Autoangel.Workers.Worker.try_reserve(index, count, capacity)` for each index
  in turn. On the FIRST index whose reservation fails (queue would overflow) or
  whose `worker_fun.(index)` is not a live pid, it releases every reservation it
  already took and returns `{:error, :overloaded}`. On overload it increments the
  load-shed counters (`Autoangel.Ingest.Metrics.increment_load_shed/1` and
  `Autoangel.Metrics.increment(:samples_load_shed, ...)`) and NEVER reaches
  `resolve_samples/2`, so no Series is created. The HTTP layer maps
  `{:error, :overloaded}` to `send_resp(conn, 503, "engine overloaded")`.

  This test drives that REAL reservation path. Per-Worker queue capacity lives in
  per-index atomics counters stored in `:persistent_term` under
  `{Autoangel.Workers.Worker, index}` (the counter) and
  `{Autoangel.Workers.Worker, :capacity, index}` (the limit); `try_reserve/3`
  treats an absent counter as unlimited. By seeding those two keys for the
  indices a request maps to and pre-filling the counter to a chosen fill level,
  we make a request overflow (reject) or fit (admit) deterministically, without
  starting any real Worker GenServer.

  Routing is pinned with explicit options: `pool_size` fixes how
  `SeriesRegistry.worker_index/2` maps each Series to an index, and
  `worker_fun: fn _ -> self() end` makes every index resolve to a live pid (this
  test process) so the ONLY reason a reservation fails is capacity. With the test
  process as the worker, the `{:samples, ...}` cast emitted on the admit path is
  delivered to our own mailbox and is never consumed, so the reserved count stays
  on the counter and we can observe it (a real Worker would decrement it while
  draining). A `SeriesRegistry` with ample limits is started so that resolution
  on the admit path actually creates Series — and so the reject path's "no Series
  created" claim is meaningful rather than vacuous.

  Counters are process-wide (atomics in persistent_term): `load_shed/0` and
  `SeriesRegistry.count/0` accumulate across the whole suite, so every assertion
  is on the DELTA captured immediately before and after the request under test,
  never on an absolute value. `async: false` because of the shared persistent_term
  namespace and the single named SeriesRegistry.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  alias Autoangel.Exposition.Sample
  alias Autoangel.Ingest
  alias Autoangel.Ingest.Metrics
  alias Autoangel.SeriesRegistry
  alias Autoangel.Workers.Worker

  # Ample cardinality room so that, on the admit path, every generated Series is
  # created (never rejected for cardinality) and the "Series created" delta
  # equals the number of admitted Samples.
  @per_metric_limit 100_000
  @total_limit 1_000_000

  setup do
    pid =
      start_registry(per_metric_limit: @per_metric_limit, total_limit: @total_limit, pool_size: 4)

    Process.unlink(pid)

    on_exit(fn -> safe_stop(Process.whereis(SeriesRegistry)) end)

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

  # Seed a per-index queue counter + capacity in persistent_term and pre-fill the
  # counter to `fill`. Returns the atomics ref so the test can read it back. The
  # caller must erase both keys (see `release_index/1`).
  defp seed_index(index, capacity, fill) do
    counter = :atomics.new(1, signed: true)
    if fill > 0, do: :atomics.put(counter, 1, fill)
    :persistent_term.put({Worker, index}, counter)
    :persistent_term.put({Worker, :capacity, index}, capacity)
    counter
  end

  defp release_index(index) do
    :persistent_term.erase({Worker, index})
    :persistent_term.erase({Worker, :capacity, index})
  end

  # A request Sample for a given series name. Distinct names keep Series identity
  # distinct; a fixed label shape keeps canonical keys simple.
  defp sample(name), do: %Sample{name: name, labels: %{"job" => "svc"}, value: 1.0}

  # Options that pin routing: every index resolves to THIS process (a live pid),
  # so a reservation can only fail on capacity, and the generated pool size fixes
  # the Series -> index mapping.
  defp opts(pool_size), do: [pool_size: pool_size, worker_fun: fn _ -> self() end]

  # ---- Single-Worker property: all samples map to index 0 (pool_size: 1). ----

  property "a request is admitted in full or rejected in full, with reject invariants" do
    check all(
            request_size <- integer(1..30),
            capacity <- integer(1..40),
            # Pre-fill from empty up to full; combined with request_size this
            # straddles both the fits (admit) and overflows (reject) cases.
            fill <- integer(0..40),
            max_runs: 120
          ) do
      index = 0
      counter = seed_index(index, capacity, fill)

      names = for i <- 1..request_size, do: "load_shed_single_#{unique()}_#{i}"
      samples = Enum.map(names, &sample/1)

      load_shed_before = Metrics.load_shed()
      series_before = SeriesRegistry.count()

      result = Ingest.process_samples(samples, opts(1))

      load_shed_after = Metrics.load_shed()
      series_after = SeriesRegistry.count()
      counter_now = :atomics.get(counter, 1)

      if fill + request_size > capacity do
        # REJECT: the whole request is shed.
        assert result == {:error, :overloaded}

        # Queued items unchanged: every reservation was released back, so the
        # counter equals the pre-request fill. (A mutation that forgot to release
        # on halt would leave `fill + request_size` here and fail this.)
        assert counter_now == fill,
               "rejected request left counter at #{counter_now}, expected #{fill}"

        # Load-shed counter grew by exactly the request's Sample count.
        assert load_shed_after - load_shed_before == request_size

        # No Series created on the reject path (resolve_samples never runs).
        assert series_after == series_before
      else
        # ADMIT: the whole request is accepted.
        assert {:ok, %{accepted: accepted}} = result
        assert accepted == request_size

        # Reserved capacity is held (the {:samples,...} cast went to our own
        # mailbox and was not drained), so the counter advanced by the request.
        assert counter_now == fill + request_size,
               "admitted request left counter at #{counter_now}, expected #{fill + request_size}"

        # Load-shed counter is untouched on the admit path.
        assert load_shed_after == load_shed_before

        # A Series was created for each distinct admitted Sample.
        assert series_after - series_before == request_size
      end

      release_index(index)
    end
  end

  # ---- All-or-nothing across TWO Workers: one bucket full, one has room. ----

  property "all-or-nothing across workers: a full bucket rejects the whole request" do
    check all(scenario <- two_worker_scenario(), max_runs: 80) do
      %{by_index: by_index, samples: samples, roomy: roomy, full: full} = scenario

      # The full bucket is seeded at capacity so any reservation overflows; the
      # roomy bucket has slack for its whole share. Record each counter's fill so
      # we can assert both are unchanged after the rejected request.
      full_fill = full.capacity
      roomy_fill = 0

      full_counter = seed_index(full.index, full.capacity, full_fill)
      roomy_counter = seed_index(roomy.index, roomy.capacity, roomy_fill)

      load_shed_before = Metrics.load_shed()
      series_before = SeriesRegistry.count()

      result = Ingest.process_samples(samples, opts(2))

      load_shed_after = Metrics.load_shed()
      series_after = SeriesRegistry.count()

      # None admitted: the whole request is rejected because one bucket is full.
      assert result == {:error, :overloaded}

      # Prior fills unchanged on BOTH buckets: the full bucket was never advanced
      # (its reservation failed) and the roomy bucket's reservation, if taken
      # first, was released back on halt.
      assert :atomics.get(full_counter, 1) == full_fill,
             "full bucket (index #{full.index}) changed after reject"

      assert :atomics.get(roomy_counter, 1) == roomy_fill,
             "roomy bucket (index #{roomy.index}) not released after reject"

      # Load-shed counter grew by the full request's Sample count.
      assert load_shed_after - load_shed_before == length(samples)

      # No Series created anywhere.
      assert series_after == series_before

      # Sanity: the request genuinely spanned both worker indices.
      assert map_size(by_index) == 2

      release_index(full.index)
      release_index(roomy.index)
    end
  end

  # Generates a request whose Samples span BOTH worker indices under pool_size 2,
  # plus a roomy/full index assignment and capacities. We generate candidate
  # names, map each to its real worker index via SeriesRegistry.worker_index/2,
  # and only keep scenarios where both index 0 and index 1 are populated so the
  # all-or-nothing behaviour across two Workers is actually exercised.
  defp two_worker_scenario do
    gen all(
          names <- uniq_list_of(name_gen(), min_length: 2, max_length: 24),
          full_is_zero <- boolean(),
          roomy_capacity <- integer(1..50),
          full_capacity <- integer(1..50)
        ) do
      samples = Enum.map(names, &sample/1)

      by_index =
        Enum.group_by(samples, fn %Sample{name: n, labels: l} ->
          SeriesRegistry.worker_index(SeriesRegistry.canonical_key(n, l), 2)
        end)

      {full_index, roomy_index} = if full_is_zero, do: {0, 1}, else: {1, 0}

      %{
        by_index: by_index,
        samples: samples,
        full: %{index: full_index, capacity: full_capacity},
        # Ensure the roomy bucket has room for every Sample in the whole request
        # so, if it were reserved first, it could succeed before the full bucket
        # forces the halt. This isolates the failure to the full bucket.
        roomy: %{index: roomy_index, capacity: roomy_capacity + length(samples)}
      }
    end
    |> StreamData.filter(fn %{by_index: by_index} ->
      Map.has_key?(by_index, 0) and Map.has_key?(by_index, 1)
    end)
  end

  defp name_gen do
    gen all(suffix <- string([?a..?z, ?0..?9], min_length: 1, max_length: 12)) do
      "load_shed_pair_#{suffix}"
    end
  end

  # ---- HTTP-level example: {:error, :overloaded} surfaces as 503. ----

  test "an overloaded request replies 503 \"engine overloaded\" at the HTTP boundary" do
    index = 0
    # Capacity 1, pre-filled to 1 (full): any non-empty request overflows.
    seed_index(index, 1, 1)

    body = "load_shed_http_metric 1\n"

    conn =
      Plug.Test.conn(:post, "/api/v1/push", body)
      |> Ingest.call(pool_size: 1, worker_fun: fn _ -> self() end)

    assert conn.status == 503
    assert conn.resp_body == "engine overloaded"

    release_index(index)
  end

  defp unique, do: System.unique_integer([:positive])
end

defmodule Autoangel.Ingest.AcceptedFlowPropertyTest do
  # async: false because every case starts the named Autoangel.SeriesRegistry
  # GenServer (singleton) and the dispatch path keys process-global
  # :persistent_term by worker index through Autoangel.Workers.Worker. Running
  # concurrently would race on the shared registry name and those keys.
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Plug.Conn

  alias Autoangel.Exposition.Sample
  alias Autoangel.Ingest
  alias Autoangel.Secret
  alias Autoangel.SeriesRegistry

  # **Validates: Requirements 1.1, 1.2, 1.3, 1.4, 5.6, 8.2, 36.1**
  # Feature: v1, Property 3: the accepted Sample flow.
  #
  # "For any sequence of authenticated push requests with valid bodies and a
  #  fixed receive clock, the Samples handed to Workers and to the Storage_Queue
  #  are exactly the parsed Samples minus those rejected by a cardinality limit,
  #  one per pushed line and per push (no merging across pushes), with the pushed
  #  timestamp or the receive time when absent, and with raw values (never
  #  rates)."
  #
  # The property core drives Autoangel.Ingest.process_samples/2 directly with
  # %Sample{} structs (Property 3 is about the admission/dispatch flow, not
  # exposition parsing). Workers and the Storage_Queue are replaced by collector
  # pids injected via :worker_fun and :storage_writer, so every batch a Worker or
  # the Storage_Queue would receive (a GenServer.cast {:samples, batch}) is
  # captured and asserted against an independent oracle. A single trailing
  # example drives the full Autoangel.HTTP.Ingest.call/2 path (bearer auth +
  # exposition body + config) to prove the HTTP wiring reaches the same
  # collectors and returns the 202 accounting body.

  @pool_size 4
  @assert_timeout 500

  # Casts for a push are all placed in the mailbox synchronously before
  # process_samples/2 returns (GenServer.cast is a send, and enqueue happens
  # inside the call), so by the time we drain they are already queued. A short
  # drain timeout therefore collects every delivered batch for a push without
  # paying a long per-push wait across many property runs.
  @drain_timeout 50

  # A fixed receive clock, as the property requires. Distinct from any pushed
  # timestamp the generator may produce so the stamping rule is observable.
  @receive_time 1_700_000_000_000

  # ---- Generators --------------------------------------------------------

  # A small metric-name alphabet so distinct label sets collide onto the same
  # metric name often enough to exercise the per-metric cardinality limit.
  defp name_gen, do: member_of(~w(cpu mem disk net reqs))

  # Build label maps by choosing a subset of a small fixed key set and pairing
  # each chosen key with a generated value. Using `map_of` over a 4-element key
  # domain trips StreamData's consecutive-duplicate guard (TooManyDuplicatesError)
  # because it keeps regenerating the same few keys; selecting a subset avoids
  # that while still covering 0..4 labels with varied values.
  @label_keys ~w(host zone region shard)

  defp labels_gen do
    gen all(
          keys <- subset_gen(@label_keys),
          values <-
            list_of(string([?a..?z, ?0..?9], min_length: 1, max_length: 4), length: length(keys))
        ) do
      keys |> Enum.zip(values) |> Map.new()
    end
  end

  # A random subset (any size, order-independent) of the given elements.
  defp subset_gen(elements) do
    elements
    |> Enum.map(fn element -> {element, boolean()} end)
    |> then(fn pairs ->
      gen all(flags <- fixed_list(Enum.map(pairs, fn {_e, g} -> g end))) do
        elements
        |> Enum.zip(flags)
        |> Enum.filter(fn {_e, keep?} -> keep? end)
        |> Enum.map(fn {e, _} -> e end)
      end
    end)
  end

  # Finite floats only: :nan/:inf are valid Samples but awkward to compare for
  # exact value equality, and the raw-value clause is about pass-through, which
  # finite floats demonstrate cleanly.
  defp value_gen, do: float(min: -1.0e6, max: 1.0e6)

  defp timestamp_gen, do: one_of([constant(nil), integer(1..1_000_000)])

  defp sample_gen do
    gen all(
          name <- name_gen(),
          labels <- labels_gen(),
          value <- value_gen(),
          timestamp <- timestamp_gen(),
          type_hint <- member_of([:gauge, :counter])
        ) do
      %Sample{
        name: name,
        labels: labels,
        value: value,
        timestamp: timestamp,
        type_hint: type_hint
      }
    end
  end

  # ---- Oracle helpers ----------------------------------------------------

  # The identity of a Sample as observed after stamping: the fields Property 3
  # constrains (name/labels/value/timestamp/received_at). Used to compare input
  # Samples against delivered Samples as multisets, independent of struct
  # extras the pipeline may set (e.g. :series_kind).
  defp identity(%Sample{} = sample) do
    {sample.name, SeriesRegistry.canonical_key(sample.name, sample.labels), sample.value,
     sample.timestamp, sample.received_at}
  end

  # The stamped form of an input Sample under the fixed receive clock, derived
  # straight from the rule text rather than by calling the pipeline: a nil
  # timestamp becomes the receive time, a set timestamp is kept, and received_at
  # is always the receive time.
  defp expected_stamped(%Sample{timestamp: nil} = sample) do
    %{sample | timestamp: @receive_time, received_at: @receive_time}
  end

  defp expected_stamped(%Sample{} = sample) do
    %{sample | received_at: @receive_time}
  end

  # ---- Independent cardinality oracle ------------------------------------
  #
  # This oracle decides, for EVERY input Sample across all pushes, whether it is
  # admitted or cardinality-rejected, WITHOUT ever calling the code under test
  # (it never touches Ingest.resolve_samples/2, Ingest.process_samples/2, or
  # SeriesRegistry.resolve_or_create/2 / admit/3). It re-implements the admission
  # rule from Autoangel.SeriesRegistry.admit/3 directly:
  #
  #   * A canonical key already created earlier (seen before) is ALWAYS admitted
  #     (the registry replies {:existing, ...} and never re-checks limits).
  #   * A NEW canonical key is admitted iff, at the moment it is processed,
  #       total_count  < total_limit  AND
  #       per_metric_count(metric_name) < per_metric_limit.
  #     Admitting a new key increments both the global total and the per-metric
  #     bucket keyed by the metric NAME (admit matches {metric, _labels} = key and
  #     counts by `metric`). A rejected key increments neither.
  #
  # Order matters: Ingest.resolve_samples/2 folds over the stamped samples in
  # input order, and the registry (ETS + atomics) persists across pushes within
  # one property run. So the oracle folds over the stamped samples in the SAME
  # input order, across ALL pushes in sequence, threading one shared state
  # (created keys, total_count, per-metric counts).
  #
  # SeriesRegistry.canonical_key/2 is reused ONLY for key identity: it is a pure
  # normalization helper (sort labels, stringify), not the admission logic, so
  # using it keeps the oracle's notion of "same series" identical to the
  # registry's without borrowing any admission decision.
  #
  # Returns a map:
  #   %{
  #     per_push: [%{admitted: [stamped_sample], rejected_count: n}],  # per push, input order
  #     admitted_total: n,
  #     rejected_total: n
  #   }
  # where each admitted entry is the expected_stamped/1 form of the input Sample.
  defp cardinality_oracle(pushes, per_metric_limit, total_limit) do
    initial_state = %{created: MapSet.new(), total: 0, per_metric: %{}}
    limits = %{per_metric: per_metric_limit, total: total_limit}

    {per_push_rev, _final_state} =
      Enum.reduce(pushes, {[], initial_state}, fn samples, {acc, state} ->
        {push_result, next_state} = oracle_push(samples, limits, state)
        {[push_result | acc], next_state}
      end)

    per_push = Enum.reverse(per_push_rev)

    %{
      per_push: per_push,
      admitted_total: per_push |> Enum.flat_map(& &1.admitted) |> length(),
      rejected_total: Enum.reduce(per_push, 0, &(&1.rejected_count + &2))
    }
  end

  # Fold one push's samples (in input order) through the shared oracle state,
  # returning the push result and the carried-forward state.
  defp oracle_push(samples, limits, state) do
    {admitted_rev, rejected, next_state} =
      Enum.reduce(samples, {[], 0, state}, fn sample, {admitted, rejected, st} ->
        oracle_step(sample, limits, admitted, rejected, st)
      end)

    {%{admitted: Enum.reverse(admitted_rev), rejected_count: rejected}, next_state}
  end

  # The per-sample admission decision, re-implementing SeriesRegistry.admit/3.
  defp oracle_step(sample, limits, admitted, rejected, st) do
    key = SeriesRegistry.canonical_key(sample.name, sample.labels)
    metric = sample.name

    cond do
      MapSet.member?(st.created, key) ->
        # Existing series: always admitted, no counter change.
        {[expected_stamped(sample) | admitted], rejected, st}

      st.total >= limits.total ->
        {admitted, rejected + 1, st}

      Map.get(st.per_metric, metric, 0) >= limits.per_metric ->
        {admitted, rejected + 1, st}

      true ->
        st = %{
          st
          | created: MapSet.put(st.created, key),
            total: st.total + 1,
            per_metric: Map.update(st.per_metric, metric, 1, &(&1 + 1))
        }

        {[expected_stamped(sample) | admitted], rejected, st}
    end
  end

  # Multiset equality by identity frequencies.
  defp multiset_equal?(a, b) do
    Enum.frequencies(a) == Enum.frequencies(b)
  end

  # Collect every batch cast delivered to a collector pid (self()), one list of
  # batches. Autoangel.Workers.Worker.enqueue/2 uses GenServer.cast, so the raw
  # message landing in the mailbox is {:"$gen_cast", {:samples, batch}} rather
  # than a bare {:samples, batch}. Batch boundaries are preserved so "no merging
  # across pushes" and "one sample per pushed line" can be checked per push.
  defp drain_batches, do: drain_batches([])

  defp drain_batches(acc) do
    receive do
      {:"$gen_cast", {:samples, batch}} -> drain_batches([batch | acc])
    after
      @drain_timeout -> Enum.reverse(acc)
    end
  end

  defp start_registry(opts) do
    opts = Keyword.put(opts, :pool_size, @pool_size)
    if pid = Process.whereis(SeriesRegistry), do: GenServer.stop(pid)

    case SeriesRegistry.start_link(opts) do
      {:ok, pid} ->
        Process.unlink(pid)
        pid

      {:error, {:already_started, running}} ->
        GenServer.stop(running)
        {:ok, pid} = SeriesRegistry.start_link(opts)
        Process.unlink(pid)
        pid
    end
  end

  # Stop a process tolerantly: a registry/collector may already be gone (its own
  # on_exit ran, or a later start_registry stopped it), so swallow the :exit.
  defp safe_stop(pid) when is_pid(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
  catch
    :exit, _ -> :ok
  end

  defp safe_stop(_), do: :ok

  setup do
    on_exit(fn -> safe_stop(Process.whereis(SeriesRegistry)) end)

    :ok
  end

  # ---- Property ----------------------------------------------------------

  property "Feature: v1, Property 3: accepted Samples flow unchanged to Workers and storage" do
    check all(
            pushes <- list_of(list_of(sample_gen(), max_length: 8), min_length: 1, max_length: 4),
            # Sometimes generous (everything is admitted), sometimes tight (some
            # Samples are rejected by a cardinality limit), so both sides of the
            # "minus those rejected by a cardinality limit" clause are exercised.
            per_metric_limit <- integer(1..40),
            total_limit <- integer(1..80),
            max_runs: 60
          ) do
      start_registry(per_metric_limit: per_metric_limit, total_limit: total_limit)

      # One collector pid for all Workers (so the union across Worker casts is
      # simply every batch this pid received) and a separate one for storage.
      worker_collector = self()
      storage_collector = spawn_storage_collector()

      opts = [
        receive_time: @receive_time,
        pool_size: @pool_size,
        queue_capacity: 1_000_000,
        worker_fun: fn _index -> worker_collector end,
        storage_writer: storage_collector
      ]

      # Drive each push through the SAME opts/collectors. Per push we record the
      # reported accounting and the Worker batches delivered for that push, so
      # cross-push merging is detectable (a merge would move Samples across the
      # per-push boundary).
      per_push =
        Enum.map(pushes, fn samples ->
          assert {:ok, %{accepted: accepted, rejected_cardinality: rejected}} =
                   Ingest.process_samples(samples, opts)

          # All Worker casts for THIS push, drained before the next push runs.
          worker_batches = drain_batches()

          %{
            input: samples,
            accepted: accepted,
            rejected: rejected,
            worker_batches: worker_batches
          }
        end)

      # Storage casts are drained once at the end; their union must equal the
      # union of all accepted Worker samples (same accepted set flows to storage).
      storage_samples = collect_storage(storage_collector)

      # Independent oracle: decides admitted/rejected per sample across all pushes
      # in input order against the SAME limits the registry was started with,
      # without ever calling the pipeline's admission code.
      oracle = cardinality_oracle(pushes, per_metric_limit, total_limit)

      assert_accounting(per_push, oracle)
      assert_worker_delivery(per_push, oracle)
      assert_storage_matches_oracle(per_push, storage_samples, oracle)
      assert_no_cross_push_merge(per_push)

      stop_collector(storage_collector)
      safe_stop(Process.whereis(SeriesRegistry))
    end
  end

  # Clause: accepted + rejected == pushed lines, and both the accepted and
  # rejected counts match the INDEPENDENT oracle exactly, not just each other
  # (requirements 8.2, 1.4). Checked per push and in aggregate so a pipeline
  # that mis-split the admit/reject boundary (e.g. rejected everything) is caught
  # rather than passing vacuously.
  defp assert_accounting(per_push, oracle) do
    pairs = Enum.zip(per_push, oracle.per_push)

    for {push, oracle_push} <- pairs do
      assert push.accepted + push.rejected == length(push.input)
      assert push.rejected >= 0
      assert push.accepted >= 0

      # Per-push counts equal the oracle's own admitted/rejected counts.
      assert push.accepted == length(oracle_push.admitted)
      assert push.rejected == oracle_push.rejected_count
    end

    # Summed across pushes: accepted == oracle admitted, rejected == oracle
    # rejected.
    total_accepted = Enum.reduce(per_push, 0, &(&1.accepted + &2))
    total_rejected = Enum.reduce(per_push, 0, &(&1.rejected + &2))
    assert total_accepted == oracle.admitted_total
    assert total_rejected == oracle.rejected_total
  end

  # Clauses: one Sample per pushed line (no merge/dedup), the delivered Samples
  # are EXACTLY the oracle's admitted stamped inputs for this push (multiset
  # equality, not merely a sub-multiset), the pushed-timestamp / receive-time
  # stamping rule holds, and raw values pass through unchanged
  # (requirements 1.1, 1.2, 1.3, 5.6).
  defp assert_worker_delivery(per_push, oracle) do
    for {push, oracle_push} <- Enum.zip(per_push, oracle.per_push) do
      delivered = List.flatten(push.worker_batches)

      # One Sample per admitted pushed line: the count of delivered Samples
      # equals the oracle's admitted count. No merging, no dedup, no fan-out.
      assert length(delivered) == length(oracle_push.admitted)

      # EXACT multiset equality: the delivered Samples (by stamped identity) are
      # precisely the oracle's admitted stamped inputs — every admitted input is
      # delivered exactly once, and nothing else is delivered. This is strictly
      # stronger than a sub-multiset check: a pipeline that wrongly rejected
      # every sample (delivering nothing) would fail here whenever the oracle
      # admits anything.
      expected_admitted = Enum.map(oracle_push.admitted, &identity/1)
      delivered_identities = Enum.map(delivered, &identity/1)

      assert multiset_equal?(delivered_identities, expected_admitted)

      # Stamping and raw-value rule, asserted directly on each delivered Sample
      # by locating the matching input (raw value is carried through, never a
      # rate; process_samples computes no rates).
      for sample <- delivered do
        input = find_input(push.input, sample)
        assert input != nil

        assert sample.received_at == @receive_time

        expected_ts = input.timestamp || @receive_time
        assert sample.timestamp == expected_ts

        # Raw value: the delivered value is exactly the pushed value.
        assert sample.value == input.value
      end
    end
  end

  # Clause (req 36.1): the Samples handed to the Storage_Queue are exactly the
  # oracle's admitted set — the same accepted set handed to the Workers — as a
  # multiset. Asserting against the oracle (rather than only against the Worker
  # deliveries) means both sinks are pinned to the independent admitted set, so
  # they cannot agree on a wrong set and pass.
  defp assert_storage_matches_oracle(per_push, storage_samples, oracle) do
    expected_admitted =
      oracle.per_push
      |> Enum.flat_map(& &1.admitted)
      |> Enum.map(&identity/1)

    worker_identities =
      per_push
      |> Enum.flat_map(& &1.worker_batches)
      |> List.flatten()
      |> Enum.map(&identity/1)

    storage_identities = Enum.map(storage_samples, &identity/1)

    # Storage == oracle admitted == workers, all as multisets.
    assert multiset_equal?(storage_identities, expected_admitted)
    assert multiset_equal?(worker_identities, expected_admitted)
    assert multiset_equal?(storage_identities, worker_identities)
  end

  # Clause: no merging across pushes. Each push delivers its own batches; a
  # Sample delivered for one push is accounted to that push only. The total
  # delivered equals the sum of per-push accepted counts, and each push's own
  # delivered count equals its own accepted count (already asserted), so Samples
  # never migrate across push boundaries.
  defp assert_no_cross_push_merge(per_push) do
    total_delivered =
      per_push
      |> Enum.flat_map(& &1.worker_batches)
      |> List.flatten()
      |> length()

    total_accepted = Enum.reduce(per_push, 0, &(&1.accepted + &2))
    assert total_delivered == total_accepted

    # A push that accepted nothing delivers no Worker batches; a push that
    # accepted something delivers at least one batch. Either way, batch
    # boundaries belong to the push that produced them.
    for push <- per_push do
      if push.accepted == 0 do
        assert List.flatten(push.worker_batches) == []
      else
        assert List.flatten(push.worker_batches) != []
      end
    end
  end

  # ---- Full-path example (HTTP wiring) -----------------------------------

  test "Property 3 example: the HTTP push path wires accepted Samples through to collectors" do
    # A single concrete example proving Autoangel.HTTP.Ingest.call/2 threads an
    # authenticated, rate-allowed, parsed body through process_samples to the
    # same Worker and Storage_Queue collectors and returns the 202 accounting
    # body. The property above owns the exhaustive admission/dispatch coverage;
    # this guards the HTTP seam.
    Application.put_env(:plug, :validate_header_keys_during_test, false)

    start_registry(per_metric_limit: 1, total_limit: 50)

    worker_collector = self()
    storage_collector = spawn_storage_collector()

    config = %{
      ingest: %{token: Secret.new("ingest-token")},
      # Generous rate so the single request is never shed by the limiter.
      ingest_rate_limit: %{rate: 1000, burst: 1000}
    }

    body = """
    # TYPE http_requests_total counter
    http_requests_total{method="get"} 5 123
    http_requests_total{method="post"} 9
    cpu_seconds 1.5
    """

    opts = [
      config: config,
      receive_time: @receive_time,
      pool_size: @pool_size,
      queue_capacity: 1_000_000,
      worker_fun: fn _index -> worker_collector end,
      storage_writer: storage_collector,
      table: String.to_atom("accepted_flow_http_#{System.unique_integer([:positive])}"),
      clock: fn -> 0 end
    ]

    conn =
      Plug.Test.conn(:post, "/api/v1/push", body)
      |> put_req_header("content-type", "text/plain")
      |> put_req_header("authorization", "Bearer ingest-token")

    result = Autoangel.HTTP.Ingest.call(conn, opts)

    assert result.status == 202
    assert result.state == :sent

    %{"accepted" => accepted, "rejected_cardinality" => rejected} = Jason.decode!(result.resp_body)
    assert is_integer(accepted)
    assert is_integer(rejected)

    # The three pushed lines map to two metric names; per_metric_limit: 1 admits
    # the first http_requests_total series and the single cpu_seconds series, so
    # the second http_requests_total line is cardinality-rejected. That is
    # exactly two admitted and one rejected.
    assert accepted == 2
    assert rejected == 1
    assert accepted + rejected == 3

    delivered = drain_batches() |> List.flatten()
    assert length(delivered) == accepted

    storage_samples = collect_storage(storage_collector)
    assert length(storage_samples) == accepted

    # The accepted Samples reaching the Workers are exactly those reaching
    # storage, and each carries the fixed receive time as received_at.
    assert Enum.map(delivered, &identity/1) |> Enum.sort() ==
             Enum.map(storage_samples, &identity/1) |> Enum.sort()

    for sample <- delivered do
      assert sample.received_at == @receive_time
    end

    # The method="get" sample carried an explicit pushed timestamp (123) and
    # keeps it; the timestamp-less cpu_seconds sample takes the receive time.
    get_sample =
      Enum.find(delivered, fn sample ->
        sample.name == "http_requests_total" and Map.get(sample.labels, "method") == "get"
      end)

    assert get_sample != nil
    assert get_sample.timestamp == 123

    cpu_sample = Enum.find(delivered, fn sample -> sample.name == "cpu_seconds" end)
    assert cpu_sample != nil
    assert cpu_sample.timestamp == @receive_time

    timestamps = delivered |> Enum.map(& &1.timestamp) |> Enum.sort()
    assert 123 in timestamps
    assert @receive_time in timestamps

    stop_collector(storage_collector)
    safe_stop(Process.whereis(SeriesRegistry))
  end

  # ---- Small utilities ---------------------------------------------------

  defp find_input(inputs, delivered) do
    target = identity(delivered)
    Enum.find(inputs, fn input -> identity(expected_stamped(input)) == target end)
  end

  # A storage collector is a plain GenServer-like receiver: process_samples casts
  # {:samples, accepted} to it via GenServer.cast, which arrives as a plain
  # message. We use a dedicated pid (not self()) so storage casts stay separate
  # from Worker casts, then query it for everything it received.
  defp spawn_storage_collector do
    test = self()

    spawn_link(fn -> storage_loop(test, []) end)
  end

  defp storage_loop(test, acc) do
    receive do
      {:"$gen_cast", {:samples, batch}} ->
        storage_loop(test, [batch | acc])

      {:collect, from} ->
        send(from, {:storage, List.flatten(Enum.reverse(acc))})
        storage_loop(test, acc)

      :stop ->
        :ok
    end
  end

  defp collect_storage(pid) do
    send(pid, {:collect, self()})

    receive do
      {:storage, samples} -> samples
    after
      @assert_timeout -> flunk("storage collector did not respond")
    end
  end

  defp stop_collector(pid), do: send(pid, :stop)
end

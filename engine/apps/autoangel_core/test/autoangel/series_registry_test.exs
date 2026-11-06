defmodule Autoangel.SeriesRegistryTest do
  use ExUnit.Case, async: false
  use ExUnitProperties

  setup do
    if Process.whereis(Autoangel.SeriesRegistry), do: GenServer.stop(Autoangel.SeriesRegistry)

    {:ok, pid} =
      Autoangel.SeriesRegistry.start_link(per_metric_limit: 2, total_limit: 3, pool_size: 4)

    Process.unlink(pid)

    on_exit(fn ->
      if registry = Process.whereis(Autoangel.SeriesRegistry), do: GenServer.stop(registry)
    end)

    :ok
  end

  test "canonical identity ignores label order and is deterministic" do
    first =
      Autoangel.SeriesRegistry.canonical_key("http_requests_total", %{
        "zone" => "a",
        "method" => "GET"
      })

    second =
      Autoangel.SeriesRegistry.canonical_key("http_requests_total", [
        {"method", "GET"},
        {"zone", "a"}
      ])

    assert first == second
    assert Autoangel.SeriesRegistry.series_id(first) == Autoangel.SeriesRegistry.series_id(second)

    assert Autoangel.SeriesRegistry.worker_index(first, 4) ==
             Autoangel.SeriesRegistry.worker_index(second, 4)
  end

  test "existing series remains admissible at cardinality limits" do
    first_key = Autoangel.SeriesRegistry.canonical_key("requests", %{"id" => "one"})
    second_key = Autoangel.SeriesRegistry.canonical_key("requests", %{"id" => "two"})
    assert {:created, first} = Autoangel.SeriesRegistry.resolve_or_create(first_key)
    assert {:created, _second} = Autoangel.SeriesRegistry.resolve_or_create(second_key)
    assert {:existing, ^first} = Autoangel.SeriesRegistry.resolve_or_create(first_key)
    assert Autoangel.SeriesRegistry.count() == 2
    assert Autoangel.SeriesRegistry.rejection_counts() == %{per_metric: 0, total: 0}
  end

  test "limits reject new series without exceeding counters" do
    assert {:created, _} =
             Autoangel.SeriesRegistry.resolve_or_create({"requests", %{"id" => "one"}})

    assert {:created, _} =
             Autoangel.SeriesRegistry.resolve_or_create({"requests", %{"id" => "two"}})

    assert {:rejected, :per_metric} =
             Autoangel.SeriesRegistry.resolve_or_create({"requests", %{"id" => "three"}})

    assert Autoangel.SeriesRegistry.count() == 2
    assert %{per_metric: 1, total: 0} = Autoangel.SeriesRegistry.rejection_counts()
  end

  test "concurrent admissions respect both limits" do
    keys = for id <- 1..20, do: {"metric", %{"id" => Integer.to_string(id)}}

    results =
      Task.async_stream(keys, &Autoangel.SeriesRegistry.resolve_or_create/1, max_concurrency: 20)
      |> Enum.map(&elem(&1, 1))

    assert Enum.count(results, &match?({:created, _}, &1)) == 2
    assert Enum.count(results, &match?({:rejected, :per_metric}, &1)) == 18
    assert Autoangel.SeriesRegistry.count() == 2
  end

  test "updates an existing kind when TYPE metadata changes" do
    key = Autoangel.SeriesRegistry.canonical_key("jobs", %{})
    assert {:created, %{kind: :gauge}} = Autoangel.SeriesRegistry.resolve_or_create(key, :gauge)

    assert {:existing, %{kind: :counter}} =
             Autoangel.SeriesRegistry.resolve_or_create(key, :counter)
  end

  test "total limit rejects new metric names without allocating metric counters" do
    assert {:created, _} = Autoangel.SeriesRegistry.resolve_or_create({"first", %{}})
    assert {:created, _} = Autoangel.SeriesRegistry.resolve_or_create({"second", %{}})
    assert {:created, _} = Autoangel.SeriesRegistry.resolve_or_create({"third", %{}})
    assert {:rejected, :total} = Autoangel.SeriesRegistry.resolve_or_create({"fourth", %{}})
    assert {:rejected, :total} = Autoangel.SeriesRegistry.resolve_or_create({"fifth", %{}})
    assert Autoangel.SeriesRegistry.count() == 3
    assert %{per_metric: 0, total: 2} = Autoangel.SeriesRegistry.rejection_counts()
  end

  # Independent oracle for the override property below. This re-derives the expected
  # Series_Kind straight from the rule text (Property 7, requirements 5.1/5.2/5.3) and
  # never calls Autoangel.SeriesKind, so the registry override assertions are a genuine
  # cross-check rather than a tautology. It is intentionally duplicated here (rather
  # than shared with Autoangel.SeriesKindTest) so each suite carries its own oracle.
  defp override_reference_kind(name, type_hint, labels) do
    case type_hint do
      :counter -> :counter
      :gauge -> :gauge
      :histogram -> override_histogram_kind(name)
      :summary -> override_summary_kind(name, labels)
      _ -> override_suffix_kind(name)
    end
  end

  defp override_histogram_kind(name) do
    if override_ends_with_any?(name, ["_bucket", "_count", "_sum"]),
      do: :counter,
      else: :gauge
  end

  defp override_summary_kind(name, labels) do
    cond do
      Map.has_key?(labels, "quantile") -> :gauge
      override_ends_with_any?(name, ["_count", "_sum"]) -> :counter
      true -> :gauge
    end
  end

  defp override_suffix_kind(name) do
    if override_ends_with_any?(name, ["_total", "_count", "_sum", "_bucket"]),
      do: :counter,
      else: :gauge
  end

  # Case-sensitive end-suffix test, implemented independently via trailing bytes.
  defp override_ends_with_any?(name, suffixes) do
    Enum.any?(suffixes, fn suffix ->
      suffix_size = byte_size(suffix)

      byte_size(name) >= suffix_size and
        binary_part(name, byte_size(name) - suffix_size, suffix_size) == suffix
    end)
  end

  test "Property 7 override: a counter TYPE line changes an existing gauge Series" do
    # **Validates: Requirements 5.1, 5.7** — a later TYPE line genuinely changes the
    # kind of an EXISTING Series. This exercises the stateful registry path
    # (resolve_or_create -> update_kind), not the pure infer/3 function.
    key = Autoangel.SeriesRegistry.canonical_key("jobs", %{})

    assert {:created, %{kind: :gauge}} = Autoangel.SeriesRegistry.resolve_or_create(key, :gauge)

    assert {:existing, %{kind: :counter}} =
             Autoangel.SeriesRegistry.resolve_or_create(key, :counter)

    # The reverse direction also overrides: counter -> gauge on the same Series.
    assert {:existing, %{kind: :gauge}} =
             Autoangel.SeriesRegistry.resolve_or_create(key, :gauge)
  end

  test "Property 7 override: an untyped or nil later TYPE line is a no-op for an existing Series" do
    # **Validates: Requirements 5.1, 5.7** — update_kind only re-infers for an explicit
    # counter/gauge/histogram/summary hint; :untyped and nil keep the existing kind.
    key = Autoangel.SeriesRegistry.canonical_key("jobs", %{})

    assert {:created, %{kind: :counter}} =
             Autoangel.SeriesRegistry.resolve_or_create(key, :counter)

    assert {:existing, %{kind: :counter}} =
             Autoangel.SeriesRegistry.resolve_or_create(key, :untyped)

    assert {:existing, %{kind: :counter}} =
             Autoangel.SeriesRegistry.resolve_or_create(key, nil)
  end

  property "Feature: v1, Property 7 override: a later TYPE line changes an existing Series kind" do
    # **Validates: Requirements 5.1, 5.7**
    #
    # A Series is created with one TYPE hint, then re-resolved under the SAME canonical
    # key with a second TYPE hint. The registry must:
    #   * re-infer the kind from the second hint when it is one of
    #     [:counter, :gauge, :histogram, :summary] (a real override), and
    #   * keep the originally created kind when the second hint is :untyped or nil
    #     (the override is a deliberate no-op).
    #
    # A single metric name with a fixed-shape label set keeps every run under the
    # per-metric and total limits of the generated registry, so the created series is
    # always admitted and the second call always lands on the existing-series path.
    override_hint = member_of([:counter, :gauge, :histogram, :summary, :untyped, nil])

    # Bias generated names toward the significant end-suffixes so the histogram,
    # summary and suffix counter branches of the override path are actually exercised,
    # not just the counter/gauge hints. A plain random string almost never ends in
    # "_total"/"_count"/"_sum"/"_bucket".
    name_gen =
      gen all(
            base <- string([?a..?z, ?A..?Z, ?0..?9, ?_], min_length: 1, max_length: 24),
            suffix <- member_of(["", "_total", "_count", "_sum", "_bucket"])
          ) do
        base <> suffix
      end

    check all(
            name <- name_gen,
            labels <-
              map_of(
                string([?a..?z, ?A..?Z], min_length: 1, max_length: 6),
                string([?a..?z, ?A..?Z, ?0..?9], min_length: 0, max_length: 6),
                max_length: 3
              ),
            include_quantile <- boolean(),
            quantile_value <- member_of(["0.5", "0.9", "0.99", "+Inf"]),
            first_hint <- override_hint,
            second_hint <- override_hint,
            max_runs: 150
          ) do
      labels = if include_quantile, do: Map.put(labels, "quantile", quantile_value), else: labels

      # A fresh registry per run with ample room so a single key is always admitted and
      # the second resolve always exercises the existing-series override path.
      if Process.whereis(Autoangel.SeriesRegistry),
        do: GenServer.stop(Autoangel.SeriesRegistry)

      {:ok, pid} =
        Autoangel.SeriesRegistry.start_link(
          per_metric_limit: 4,
          total_limit: 4,
          pool_size: 4
        )

      Process.unlink(pid)

      key = Autoangel.SeriesRegistry.canonical_key(name, labels)

      assert {:created, created} = Autoangel.SeriesRegistry.resolve_or_create(key, first_hint)
      assert created.kind == override_reference_kind(name, first_hint, labels)

      assert {:existing, overridden} =
               Autoangel.SeriesRegistry.resolve_or_create(key, second_hint)

      # Identity is preserved across the override: only the kind may change.
      assert overridden.id == created.id
      assert overridden.key == created.key
      assert overridden.worker == created.worker

      expected_kind =
        if second_hint in [:counter, :gauge, :histogram, :summary] do
          # A real override: the kind now reflects the second TYPE line.
          override_reference_kind(name, second_hint, labels)
        else
          # :untyped / nil keep the originally created kind.
          created.kind
        end

      assert overridden.kind == expected_kind

      GenServer.stop(Autoangel.SeriesRegistry)
    end
  end

  property "Feature: v1, Property 5: canonical Series identity and Worker mapping" do
    # **Validates: Requirements 4.3, 48.1, 48.2**
    check all(
            name <- string([?a..?z, ?A..?Z, ?0..?9, ?_], min_length: 1, max_length: 40),
            labels <-
              map_of(
                string([?a..?z, ?A..?Z, ?0..?9, ?_], min_length: 1, max_length: 12),
                string(:utf8, min_length: 0, max_length: 20),
                max_length: 8
              ),
            permutation_keys <- list_of(integer(), min_length: 8, max_length: 8),
            pool_size <- integer(1..32)
          ) do
      labels_list = Map.to_list(labels)

      permuted_labels =
        labels_list
        |> Enum.zip(Enum.take(permutation_keys, length(labels_list)))
        |> Enum.sort_by(&elem(&1, 1))
        |> Enum.map(&elem(&1, 0))

      first = Autoangel.SeriesRegistry.canonical_key(name, labels)
      second = Autoangel.SeriesRegistry.canonical_key(name, permuted_labels)
      different = Autoangel.SeriesRegistry.canonical_key(name <> "_different", labels)

      assert first == second
      assert first != different

      assert Autoangel.SeriesRegistry.canonical_binary(first) ==
               Autoangel.SeriesRegistry.canonical_binary(second)

      assert Autoangel.SeriesRegistry.canonical_binary(first) !=
               Autoangel.SeriesRegistry.canonical_binary(different)

      assert Autoangel.SeriesRegistry.series_id(first) ==
               Autoangel.SeriesRegistry.series_id(second)

      assert Autoangel.SeriesRegistry.series_id(first) !=
               Autoangel.SeriesRegistry.series_id(different)

      assert Autoangel.SeriesRegistry.worker_index(first, pool_size) ==
               Autoangel.SeriesRegistry.worker_index(second, pool_size)

      assert Autoangel.SeriesRegistry.worker_index(first, pool_size) in 0..(pool_size - 1)

      assert Autoangel.SeriesRegistry.canonical_key(%{name: name, labels: permuted_labels}) == first

      assert {:created, reference} = Autoangel.SeriesRegistry.resolve_or_create({name, labels})
      assert reference.id == Autoangel.SeriesRegistry.series_id(first)
      assert reference.worker == Autoangel.SeriesRegistry.worker_index(first, 4)

      GenServer.stop(Autoangel.SeriesRegistry)

      {:ok, pid} =
        Autoangel.SeriesRegistry.start_link(per_metric_limit: 2, total_limit: 3, pool_size: 4)

      Process.unlink(pid)
      assert {:created, restarted} = Autoangel.SeriesRegistry.resolve_or_create(first)
      assert restarted.id == reference.id
      assert restarted.worker == reference.worker
    end
  end

  property "Feature: v1, Property 6: cardinality limits hold under concurrency" do
    # **Validates: Requirements 4.5, 8.2, 8.4, 8.5**
    check all(
            per_metric_limit <- integer(1..6),
            total_limit <- integer(1..12),
            metric_count <- integer(1..4),
            labels_per_metric <- integer(1..6),
            # A second wave of the same keys exercises the existing-series path once
            # limits are already full.
            replays <- integer(0..2),
            max_runs: 40
          ) do
      # Each property run uses a freshly started registry with generated limits so the
      # limits under test are the generated ones, not the shared setup fixture.
      if Process.whereis(Autoangel.SeriesRegistry),
        do: GenServer.stop(Autoangel.SeriesRegistry)

      {:ok, pid} =
        Autoangel.SeriesRegistry.start_link(
          per_metric_limit: per_metric_limit,
          total_limit: total_limit,
          pool_size: 4
        )

      Process.unlink(pid)

      # Build a pool of distinct keys across several metric names. The candidate key
      # count often exceeds both limits so that rejections actually occur.
      keys =
        for metric <- 1..metric_count,
            id <- 1..labels_per_metric do
          {"metric_#{metric}", %{"id" => Integer.to_string(id)}}
        end

      # First wave plus optional replays of the same keys, shuffled together into one
      # concurrent stream of new and existing Series.
      stream =
        keys
        |> List.duplicate(replays + 1)
        |> List.flatten()
        |> Enum.shuffle()

      results =
        stream
        |> Task.async_stream(
          fn key -> {key, Autoangel.SeriesRegistry.resolve_or_create(key)} end,
          max_concurrency: 16,
          ordered: false
        )
        |> Enum.map(fn {:ok, {key, result}} -> {key, result} end)

      created = for {_key, {:created, ref}} <- results, do: ref
      existing = for {_key, {:existing, ref}} <- results, do: ref
      rejected = for {_key, {:rejected, limit}} <- results, do: limit

      # The registry serializes admissions through a GenServer, so the final admitted
      # count is deterministic for a given key pool: each metric admits at most
      # min(per_metric_limit, candidate labels) keys, and the total is capped at the
      # total limit. Asserting the exact value rejects both over-admission (exceeding a
      # limit) and over-rejection (rejecting a Sample that should have been accepted).
      per_metric_room = min(per_metric_limit, labels_per_metric)
      expected_count = min(total_limit, metric_count * per_metric_room)

      count = Autoangel.SeriesRegistry.count()
      assert count == expected_count
      assert count <= total_limit

      # No metric name ever holds more than the per-metric limit, and when the total
      # limit is not the binding constraint every metric fills its available room.
      created_per_metric =
        created
        |> Enum.map(fn ref -> elem(ref.key, 0) end)
        |> Enum.frequencies()

      for {_metric, metric_total} <- created_per_metric do
        assert metric_total <= per_metric_limit
      end

      if count < total_limit do
        for {_metric, metric_total} <- created_per_metric do
          assert metric_total == per_metric_room
        end
      end

      # Distinct created keys equal the registry count: a rejected Sample creates no
      # Series, and a created key is created exactly once under concurrency.
      distinct_created = created |> Enum.map(& &1.key) |> Enum.uniq()
      assert length(distinct_created) == count
      assert length(created) == count

      # Every rejection is attributed to one of the two limits.
      assert Enum.all?(rejected, &(&1 in [:per_metric, :total]))

      # Rejection counters reflect exactly the observed rejections per limit.
      observed = Enum.frequencies(rejected)
      counts = Autoangel.SeriesRegistry.rejection_counts()
      assert counts.per_metric == Map.get(observed, :per_metric, 0)
      assert counts.total == Map.get(observed, :total, 0)

      # Existing-series admissions only ever point at keys that were actually created
      # and never invent or mutate identity.
      created_by_key = Map.new(created, &{&1.key, &1})

      for ref <- existing do
        assert Map.has_key?(created_by_key, ref.key)
        assert created_by_key[ref.key].id == ref.id
        assert created_by_key[ref.key].worker == ref.worker
      end

      # Within the concurrent wave, a replay of an already-created key is never
      # rejected: existing Series stay admissible even once limits are full.
      for {{name, labels}, result} <- results do
        if Map.has_key?(created_by_key, Autoangel.SeriesRegistry.canonical_key(name, labels)) do
          refute match?({:rejected, _}, result)
        end
      end

      # Every Sample of an already-existing Series is accepted even once the limits are
      # full: re-resolving every created key returns :existing, never :rejected, and
      # creates no new Series.
      for %{key: key} = ref <- created do
        assert {:existing, same} = Autoangel.SeriesRegistry.resolve_or_create(key)
        assert same.id == ref.id
        assert same.worker == ref.worker
      end

      assert Autoangel.SeriesRegistry.count() == count

      # Mixed request: a single concurrent batch that interleaves known (existing) keys
      # with brand-new keys. Known keys stay accepted; new keys are admitted only while
      # a limit allows it and otherwise rejected, and a rejection creates no Series.
      rejections_before = Autoangel.SeriesRegistry.rejection_counts()

      new_over_limit =
        for metric <- 1..metric_count do
          {"metric_#{metric}", %{"id" => "overflow_#{System.unique_integer([:positive])}"}}
        end

      mixed = Enum.shuffle(distinct_created ++ new_over_limit)

      mixed_results =
        mixed
        |> Task.async_stream(&Autoangel.SeriesRegistry.resolve_or_create/1,
          max_concurrency: 16,
          ordered: false
        )
        |> Enum.map(fn {:ok, result} -> result end)

      mixed_created = for {:created, ref} <- mixed_results, do: ref
      mixed_existing = for {:existing, ref} <- mixed_results, do: ref.key
      mixed_rejected = for {:rejected, limit} <- mixed_results, do: limit

      # Every known key in the mixed batch is accepted as existing, none rejected.
      assert Enum.sort(Enum.uniq(mixed_existing)) == Enum.sort(distinct_created)
      assert length(mixed_existing) == length(distinct_created)

      # Accounting: the registry grows by exactly the number of newly created keys, and
      # that growth plus the rejections equals the number of brand-new keys offered.
      final_count = Autoangel.SeriesRegistry.count()
      assert final_count == count + length(mixed_created)
      assert final_count <= total_limit
      assert length(mixed_created) + length(mixed_rejected) == length(new_over_limit)

      # Rejection counters advanced by exactly the mixed-batch rejections, per limit.
      rejections_after = Autoangel.SeriesRegistry.rejection_counts()
      mixed_rejected_by_limit = Enum.frequencies(mixed_rejected)

      assert rejections_after.per_metric - rejections_before.per_metric ==
               Map.get(mixed_rejected_by_limit, :per_metric, 0)

      assert rejections_after.total - rejections_before.total ==
               Map.get(mixed_rejected_by_limit, :total, 0)

      # A rejected brand-new key created no Series: re-resolving it is rejected again
      # (counters only grow, so a full limit stays full) and creates nothing.
      for {name, labels} = key <- new_over_limit do
        canonical = Autoangel.SeriesRegistry.canonical_key(name, labels)

        case Enum.find(mixed_results, fn
               {:rejected, _} -> false
               {_tag, ref} -> ref.key == canonical
             end) do
          nil ->
            # This key was rejected in the mixed batch; it must not exist now.
            assert {:rejected, _} = Autoangel.SeriesRegistry.resolve_or_create(key)

          _created_ref ->
            :ok
        end
      end

      assert Autoangel.SeriesRegistry.count() == final_count

      GenServer.stop(Autoangel.SeriesRegistry)
    end
  end
end

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

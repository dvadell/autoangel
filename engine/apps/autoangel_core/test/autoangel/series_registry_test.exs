defmodule Autoangel.SeriesRegistryTest do
  use ExUnit.Case, async: false

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
end

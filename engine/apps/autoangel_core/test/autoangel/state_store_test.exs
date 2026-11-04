defmodule Autoangel.StateStoreTest do
  use ExUnit.Case, async: false

  setup do
    path =
      Path.join(System.tmp_dir!(), "autoangel-state-test-#{System.unique_integer([:positive])}")

    {:ok, store} = Autoangel.StateStore.start_link(path: path)
    on_exit(fn -> File.rm_rf!(path) end)
    %{store: store, path: path}
  end

  test "restores the current checkpoint and removes the old generation", %{store: store} do
    assert :ok =
             Autoangel.StateStore.checkpoint(store, [{:series_a, %{state: 1}}, {:series_b, :ok}])

    assert {:ok, %{entries: first}} = Autoangel.StateStore.restore(store)
    assert Enum.sort(first) == [series_a: %{state: 1}, series_b: :ok]

    assert :ok = Autoangel.StateStore.checkpoint(store, [{:series_a, %{state: 2}}])
    assert {:ok, %{entries: second}} = Autoangel.StateStore.restore(store)
    assert second == [series_a: %{state: 2}]
  end

  test "feedback records are synchronously durable", %{store: store, path: path} do
    assert :ok = Autoangel.StateStore.put_incident(store, "i-1", %{open: true})
    assert :ok = Autoangel.StateStore.put_mute(store, "s-1", %{until: 42})
    assert :ok = Autoangel.StateStore.put_sensitivity(store, "s-1", 0.5)
    assert {:ok, %{open: true}} = Autoangel.StateStore.get_record(store, {:incident, "i-1"})
    assert {:ok, %{until: 42}} = Autoangel.StateStore.get_record(store, {:mute, "s-1"})
    assert {:ok, 0.5} = Autoangel.StateStore.get_record(store, {:sensitivity, "s-1"})

    GenServer.stop(store)
    {:ok, reopened} = Autoangel.StateStore.start_link(path: path)
    assert {:ok, %{open: true}} = Autoangel.StateStore.get_record(reopened, {:incident, "i-1"})
    GenServer.stop(reopened)
  end

  test "empty store has no checkpoint", %{store: store} do
    assert :none = Autoangel.StateStore.restore(store)
  end
end

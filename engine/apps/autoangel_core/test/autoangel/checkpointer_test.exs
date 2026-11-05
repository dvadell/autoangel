defmodule Autoangel.CheckpointerTest do
  use ExUnit.Case, async: false

  alias Autoangel.{Checkpointer, StateStore}
  alias Autoangel.Kernel.State
  alias Autoangel.Workers.Worker

  setup do
    path =
      Path.join(
        System.tmp_dir!(),
        "autoangel-checkpoint-test-#{System.unique_integer([:positive])}"
      )

    {:ok, store} = StateStore.start_link(path: path)

    {:ok, worker} =
      Worker.start_link(index: 2, state_store: store, sweep_interval: 60_000)

    on_exit(fn ->
      if Process.alive?(worker), do: GenServer.stop(worker)
      if Process.alive?(store), do: GenServer.stop(store)
      File.rm_rf!(path)
    end)

    %{store: store, worker: worker}
  end

  test "checkpoint restores only the worker's series before scoring", %{
    store: store,
    worker: worker
  } do
    state = State.encode(State.new())
    meta = %{series_key: {"requests", []}, status: :healthy, first_ts: 10}
    assert :ok = Worker.put_series(worker, {"requests", []}, state, meta)

    {:ok, checkpointer} =
      Checkpointer.start_link(
        state_store: store,
        workers: [worker],
        interval_ms: 60_000,
        name: nil
      )

    assert :ok = Checkpointer.checkpoint_now(checkpointer)
    GenServer.stop(checkpointer)

    {:ok, restored} =
      Worker.start_link(index: 2, state_store: store, sweep_interval: 60_000)

    assert %{meta: %{status: :healthy}} =
             Worker.series(restored, {"requests", []})

    GenServer.stop(restored)
  end

  test "feedback records are available independently of checkpoints", %{store: store} do
    assert :ok = StateStore.put_incident(store, "incident", %{open: true})
    assert :ok = StateStore.put_mute(store, {:series, "requests"}, true)
    assert :ok = StateStore.put_sensitivity(store, "requests", 0.5)

    assert {:ok,
            %{
              incidents: [{"incident", %{open: true}}],
              mutes: [{{:series, "requests"}, true}],
              sensitivities: [{"requests", 0.5}]
            }} =
             StateStore.feedback(store)
  end
end

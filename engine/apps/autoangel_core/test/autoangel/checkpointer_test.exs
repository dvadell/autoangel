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

    # Start the StateStore unnamed so parallel async:false suites that also use a
    # StateStore cannot collide on the default registered name during interleaved
    # setup/teardown. The worker index is unique per run for the same reason
    # (the Worker keeps per-index persistent_term keys).
    {:ok, store} = StateStore.start_link(path: path, name: nil)

    worker_index = System.unique_integer([:positive])

    {:ok, worker} =
      Worker.start_link(index: worker_index, state_store: store, sweep_interval: 60_000)

    on_exit(fn ->
      safe_stop(worker)
      safe_stop(store)
      :persistent_term.erase({Worker, worker_index})
      :persistent_term.erase({Worker, :capacity, worker_index})
      File.rm_rf!(path)
    end)

    %{store: store, worker: worker, worker_index: worker_index}
  end

  # Stop a GenServer during teardown without failing the test if it is already
  # exiting (linked store/worker may receive :shutdown before on_exit runs).
  defp safe_stop(pid) when is_pid(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
  catch
    :exit, _ -> :ok
  end

  defp safe_stop(_), do: :ok

  test "checkpoint restores only the worker's series before scoring", %{
    store: store,
    worker: worker,
    worker_index: worker_index
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
      Worker.start_link(index: worker_index, state_store: store, sweep_interval: 60_000)

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

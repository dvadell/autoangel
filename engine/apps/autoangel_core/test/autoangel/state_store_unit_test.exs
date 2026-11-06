defmodule Autoangel.StateStoreUnitTest do
  @moduledoc """
  Unit tests for durable state persistence (Task 10.4).

  Complements `Autoangel.StateStoreTest` and `Autoangel.CheckpointerTest` by
  exercising, one requirement per test:

    * 43.2 - periodic checkpoints driven by the Checkpointer timer, including the
      re-arm of the timer so subsequent mutations keep getting persisted,
    * 43.8 - the final shutdown checkpoint is taken by `Application.prep_stop/1`
      and survives a StateStore restart,
    * 43.9 - a failure partway through a checkpoint write leaves the previously
      persisted checkpoint readable and unchanged, the failure is logged, the
      in-memory worker state is preserved, and the Checkpointer keeps retrying on
      its interval,
    * 43.3 - mute and Incident records persist immediately (without waiting for a
      checkpoint) and are returned in a deterministic order independent of the
      insertion order.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Autoangel.{Checkpointer, StateStore}
  alias Autoangel.Detect.Decision.Meta
  alias Autoangel.Kernel.State
  alias Autoangel.Workers.Worker

  # Bounded polling helper: avoids sleep-and-hope by retrying a predicate on a
  # short cadence until it succeeds or the attempt budget is exhausted. The
  # default budget (~2s) is generous enough to stay reliable across seeds while
  # still failing fast on genuine regressions.
  defp assert_eventually(fun, attempts \\ 400)

  defp assert_eventually(_fun, 0) do
    flunk("condition was not met within the allotted polling budget")
  end

  defp assert_eventually(fun, attempts) do
    case fun.() do
      {:ok, value} ->
        value

      :retry ->
        Process.sleep(5)
        assert_eventually(fun, attempts - 1)
    end
  end

  # Bounded poll that proves a re-armed Checkpointer keeps logging its failure.
  # Each iteration captures a single *continuous* window of background logging
  # long enough to span several timer intervals, so fires are not lost in gaps
  # between captures. It returns as soon as more than one failure is observed, or
  # after the attempt budget is exhausted.
  @failure_marker "checkpoint write failure"
  # Window spans several 25ms intervals so a single capture reliably sees >1 fire
  # even under scheduler contention.
  @failure_window_ms 150

  defp poll_until_repeated_failures(acc, 0), do: acc

  defp poll_until_repeated_failures(acc, _attempts) when acc > 1, do: acc

  defp poll_until_repeated_failures(_acc, attempts) do
    window = capture_log(fn -> Process.sleep(@failure_window_ms) end)

    found =
      window
      |> String.split(@failure_marker)
      |> length()
      |> Kernel.-(1)

    # Each continuous window is independent; use the richest window observed so a
    # transient scheduler stall in one window cannot mask a healthy re-arm.
    poll_until_repeated_failures(found, attempts - 1)
  end

  # Race-safe teardown stop. A linked process can begin shutting down between
  # the `Process.alive?/1` check and the `GenServer.stop/1` call (or a
  # dependency stopped earlier may take it down), which would make
  # `GenServer.stop/1` exit and fail the test during teardown. Catching the exit
  # keeps cleanup deterministic: if the process is already gone, that is fine.
  defp safe_stop(pid) when is_pid(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
  catch
    :exit, _ -> :ok
  end

  defp safe_stop(_), do: :ok

  defp tmp_path(tag) do
    Path.join(
      System.tmp_dir!(),
      "autoangel-state-unit-#{tag}-#{System.unique_integer([:positive])}"
    )
  end

  defp healthy_series(first_ts \\ 10) do
    state_bin = State.encode(State.new())
    meta = %Meta{status: :healthy, first_ts: first_ts}
    {state_bin, meta}
  end

  # Fixed worker indexes are used so persistent_term can be cleaned up
  # deterministically in on_exit, keeping the test isolated from the pool.
  defp erase_worker_persistent_term(index) do
    :persistent_term.erase({Worker, index})
    :persistent_term.erase({Worker, :capacity, index})
  rescue
    ArgumentError -> :ok
  end

  describe "43.2 periodic checkpoints" do
    test "the periodic timer re-arms and keeps persisting the worker's latest series" do
      path = tmp_path("periodic")
      {:ok, store} = StateStore.start_link(path: path, name: nil)

      {:ok, worker} =
        Worker.start_link(index: 30, state_store: store, sweep_interval: 60_000)

      {state_bin, meta} = healthy_series(10)
      assert :ok = Worker.put_series(worker, {"requests", []}, state_bin, meta)

      # Small interval so the timer fires quickly; we still poll for the result
      # rather than sleeping for a fixed duration.
      {:ok, checkpointer} =
        Checkpointer.start_link(
          state_store: store,
          workers: [worker],
          interval_ms: 30,
          name: nil
        )

      on_exit(fn ->
        safe_stop(checkpointer)
        safe_stop(worker)
        safe_stop(store)
        erase_worker_persistent_term(30)
        File.rm_rf!(path)
      end)

      # First timer fire: the initial series is persisted.
      {gen1_id, gen1_entries} =
        assert_eventually(fn ->
          case StateStore.restore(store) do
            {:ok, %{id: id, entries: [_ | _] = entries}} -> {:ok, {id, entries}}
            _ -> :retry
          end
        end)

      assert [{{"requests", []}, {1, ^state_bin, meta1_bin}}] = gen1_entries

      assert %{status: :healthy, first_ts: 10, worker_index: 30} =
               :erlang.binary_to_term(meta1_bin, [:safe])

      # Mutate the worker's series. A later checkpoint must capture this change,
      # which only happens if the periodic timer re-armed after its first fire.
      {state_bin2, meta2} = healthy_series(999)
      assert :ok = Worker.put_series(worker, {"requests", []}, state_bin2, meta2)

      {gen2_id, gen2_entries} =
        assert_eventually(fn ->
          # Accept only a checkpoint that both has a NEW id AND reflects the
          # mutated series (first_ts: 999). Matching solely on a new id could
          # accept a periodic re-fire that snapshotted the pre-mutation series
          # if the timer happened to fire between the put_series above and this
          # poll, which would then fail the first_ts assertion below.
          case StateStore.restore(store) do
            {:ok, %{id: id, entries: [{{"requests", []}, {1, _state, meta_bin}}] = entries}}
            when id != gen1_id ->
              case :erlang.binary_to_term(meta_bin, [:safe]) do
                %{first_ts: 999} -> {:ok, {id, entries}}
                _ -> :retry
              end

            _ ->
              :retry
          end
        end)

      # A genuinely newer checkpoint reflecting the mutated series.
      refute gen2_id == gen1_id
      assert [{{"requests", []}, {1, ^state_bin2, meta2_bin}}] = gen2_entries

      assert %{status: :healthy, first_ts: 999, worker_index: 30} =
               :erlang.binary_to_term(meta2_bin, [:safe])
    end
  end

  describe "43.8 shutdown checkpoint via Application.prep_stop/1" do
    test "prep_stop writes a final checkpoint that survives a StateStore stop/reopen" do
      path = tmp_path("shutdown")
      {:ok, store} = StateStore.start_link(path: path, name: nil)

      {:ok, worker} =
        Worker.start_link(index: 31, state_store: store, sweep_interval: 60_000)

      {state_bin, meta} = healthy_series()
      assert :ok = Worker.put_series(worker, {"orders", []}, state_bin, meta)

      # prep_stop/1 locates the Checkpointer by its registered name
      # (Autoangel.Checkpointer) via Process.whereis/1. In :test env no
      # application children run, so that name is free to claim here.
      {:ok, checkpointer} =
        Checkpointer.start_link(
          state_store: store,
          workers: [worker],
          interval_ms: 60_000,
          name: Autoangel.Checkpointer
        )

      on_exit(fn ->
        # Always release the named process so it cannot leak into later tests.
        case Process.whereis(Autoangel.Checkpointer) do
          nil -> :ok
          pid -> safe_stop(pid)
        end

        safe_stop(worker)
        safe_stop(store)
        erase_worker_persistent_term(31)
        File.rm_rf!(path)
      end)

      # Exercise the real shutdown path. prep_stop/1 must find the named
      # Checkpointer and drive checkpoint_now/1, returning the state it was given.
      assert :sentinel_state = Autoangel.Application.prep_stop(:sentinel_state)

      safe_stop(checkpointer)
      safe_stop(worker)
      safe_stop(store)

      {:ok, reopened} = StateStore.start_link(path: path, name: nil)

      try do
        assert {:ok, %{entries: [{{"orders", []}, {1, ^state_bin, meta_bin}}]}} =
                 StateStore.restore(reopened)

        assert %{status: :healthy, worker_index: 31} =
                 :erlang.binary_to_term(meta_bin, [:safe])
      after
        safe_stop(reopened)
      end
    end
  end

  describe "43.9 failure halfway through a write" do
    test "a partial second write leaves generation 1 intact and readable" do
      path = tmp_path("failure-partial")
      {:ok, store} = StateStore.start_link(path: path, name: nil)

      on_exit(fn ->
        safe_stop(store)
        File.rm_rf!(path)
      end)

      # Generation 1: a real, committed checkpoint written through the store.
      {state_bin, meta} = healthy_series()
      meta_bin = :erlang.term_to_binary(Map.put(Map.from_struct(meta), :worker_index, 32))
      entries = [{{"writes", []}, {1, state_bin, meta_bin}}]
      assert :ok = StateStore.checkpoint(store, entries)

      assert {:ok, %{id: gen1_id, entries: gen1_entries}} = StateStore.restore(store)
      assert [{{"writes", []}, {1, ^state_bin, ^meta_bin}}] = gen1_entries

      # Stop the store so we can manipulate the raw CubDB directory directly.
      safe_stop(store)

      # Simulate a crash BETWEEN phase 1 (writing records under a new id) and
      # phase 2 (switching {:checkpoint, :current}): write orphan records under a
      # fabricated generation-2 id but never advance :current.
      {:ok, db} = CubDB.start_link(data_dir: path, auto_file_sync: true)
      gen2_id = {:checkpoint, System.system_time(:microsecond), :crypto.strong_rand_bytes(16)}

      orphan_meta_bin =
        :erlang.term_to_binary(Map.put(Map.from_struct(meta), :worker_index, 32))

      :ok = CubDB.put(db, {:checkpoint, gen2_id, {"writes", []}}, {1, state_bin, orphan_meta_bin})
      :ok = CubDB.put(db, {:checkpoint, gen2_id, {"phantom", []}}, {1, state_bin, orphan_meta_bin})
      # {:checkpoint, :current} is deliberately left pointing at gen1.
      CubDB.stop(db)

      # Reopen through the StateStore and confirm the prior checkpoint survived.
      {:ok, reopened} = StateStore.start_link(path: path, name: nil)

      try do
        assert {:ok, %{id: restored_id, entries: restored_entries}} =
                 StateStore.restore(reopened)

        # The orphaned gen2 records are ignored: :current still points at gen1.
        assert restored_id == gen1_id
        assert restored_entries == gen1_entries
        refute restored_id == gen2_id

        # restore_worker for the relevant index likewise yields only gen1 content.
        assert {:ok, [{{"writes", []}, {1, ^state_bin, ^meta_bin}}]} =
                 StateStore.restore_worker(reopened, 32)
      after
        safe_stop(reopened)
      end
    end

    test "a failed checkpoint is logged, returns error, preserves in-memory state, and retries" do
      path = tmp_path("failure-logged")
      {:ok, store} = StateStore.start_link(path: path, name: nil)

      {:ok, worker} =
        Worker.start_link(index: 33, state_store: store, sweep_interval: 60_000)

      {state_bin, meta} = healthy_series()
      assert :ok = Worker.put_series(worker, {"writes", []}, state_bin, meta)

      # A dead pid cannot answer Worker.snapshot/1; the Checkpointer catches the
      # exit, logs the failure, and returns an error without touching the store.
      dead_pid = spawn(fn -> :ok end)

      assert_eventually(fn ->
        if Process.alive?(dead_pid), do: :retry, else: {:ok, dead_pid}
      end)

      # Small interval so the periodic timer re-arms quickly after each failure.
      # The live worker is listed BEFORE the dead pid so a real snapshot is taken
      # of it (read-only) before snapshots/1 halts on the dead pid; this makes the
      # "in-memory state preserved" assertion below meaningful rather than vacuous.
      {:ok, failing_checkpointer} =
        Checkpointer.start_link(
          state_store: store,
          workers: [worker, dead_pid],
          interval_ms: 25,
          name: nil
        )

      on_exit(fn ->
        safe_stop(failing_checkpointer)
        safe_stop(worker)
        safe_stop(store)
        erase_worker_persistent_term(33)
        File.rm_rf!(path)
      end)

      # The Checkpointer logs the failure and returns an error on demand.
      log =
        capture_log(fn ->
          assert {:error, _reason} = Checkpointer.checkpoint_now(failing_checkpointer)
        end)

      assert log =~ "checkpoint write failure"

      # The store was never touched, so there is no checkpoint at all.
      assert :none = StateStore.restore(store)

      # In-memory state is preserved: the live worker still holds its series,
      # unchanged, after the failed checkpoint attempt.
      assert %{key: {"writes", []}, kernel_state: ^state_bin, meta: ^meta} =
               Worker.series(worker, {"writes", []})

      # Next attempt at the next interval: the periodic timer re-arms, so the
      # Checkpointer keeps attempting and logs the failure repeatedly. We poll a
      # bounded number of short capture windows, accumulating the count of
      # failure log lines, and succeed as soon as we have seen more than one. A
      # single occurrence would mean the timer never re-armed.
      occurrences = poll_until_repeated_failures(0, 8)

      assert occurrences > 1,
             "expected repeated failure logging from re-armed timer, saw #{occurrences}"
    end
  end

  describe "43.3 mute and incident persistence ordering" do
    test "records are durable immediately and returned in a stable, insertion-order-independent order" do
      path_a = tmp_path("order-a")
      path_b = tmp_path("order-b")
      {:ok, store_a} = StateStore.start_link(path: path_a, name: nil)
      {:ok, store_b} = StateStore.start_link(path: path_b, name: nil)

      on_exit(fn ->
        safe_stop(store_a)
        safe_stop(store_b)
        File.rm_rf!(path_a)
        File.rm_rf!(path_b)
      end)

      incident_ids = ["inc-b", "inc-a", "inc-c"]
      mute_ids = ["mute-3", "mute-1", "mute-2"]

      # Insert interleaved, in one order, with no checkpoint in between.
      assert :ok = StateStore.put_incident(store_a, "inc-b", %{open: true})
      assert :ok = StateStore.put_mute(store_a, "mute-3", %{until: 3})

      # Immediately durable: readable before any checkpoint is taken.
      assert {:ok, %{open: true}} = StateStore.get_record(store_a, {:incident, "inc-b"})
      assert {:ok, %{until: 3}} = StateStore.get_record(store_a, {:mute, "mute-3"})
      assert :none = StateStore.restore(store_a)

      assert :ok = StateStore.put_incident(store_a, "inc-a", %{open: false})
      assert :ok = StateStore.put_mute(store_a, "mute-1", %{until: 1})
      assert :ok = StateStore.put_incident(store_a, "inc-c", %{open: true})
      assert :ok = StateStore.put_mute(store_a, "mute-2", %{until: 2})

      # Insert into a second store in the reverse order.
      for id <- Enum.reverse(incident_ids) do
        assert :ok = StateStore.put_incident(store_b, id, %{id: id})
      end

      for id <- Enum.reverse(mute_ids) do
        assert :ok = StateStore.put_mute(store_b, id, %{id: id})
      end

      # These ids sort identically under plain ordering and under the store's
      # internal term ordering, so a plain sort is a faithful expectation.
      expected_incident_order = Enum.sort(incident_ids)
      expected_mute_order = Enum.sort(mute_ids)

      incidents_a = Enum.map(StateStore.records(store_a, :incident), fn {id, _} -> id end)
      incidents_b = Enum.map(StateStore.records(store_b, :incident), fn {id, _} -> id end)
      mutes_a = Enum.map(StateStore.records(store_a, :mute), fn {id, _} -> id end)
      mutes_b = Enum.map(StateStore.records(store_b, :mute), fn {id, _} -> id end)

      # Deterministic, stable sort regardless of insertion order.
      assert incidents_a == expected_incident_order
      assert incidents_b == expected_incident_order
      assert mutes_a == expected_mute_order
      assert mutes_b == expected_mute_order

      # Records survive a stop/reopen of the StateStore, still ordered.
      safe_stop(store_a)
      {:ok, reopened} = StateStore.start_link(path: path_a, name: nil)

      try do
        reopened_incidents =
          Enum.map(StateStore.records(reopened, :incident), fn {id, _} -> id end)

        reopened_mutes = Enum.map(StateStore.records(reopened, :mute), fn {id, _} -> id end)
        assert reopened_incidents == expected_incident_order
        assert reopened_mutes == expected_mute_order

        # Deletes remove records.
        assert :ok = StateStore.delete_incident(reopened, "inc-a")
        assert :ok = StateStore.delete_mute(reopened, "mute-1")
        assert :not_found = StateStore.get_record(reopened, {:incident, "inc-a"})
        assert :not_found = StateStore.get_record(reopened, {:mute, "mute-1"})

        remaining_incidents =
          Enum.map(StateStore.records(reopened, :incident), fn {id, _} -> id end)

        remaining_mutes = Enum.map(StateStore.records(reopened, :mute), fn {id, _} -> id end)
        assert remaining_incidents == expected_incident_order -- ["inc-a"]
        assert remaining_mutes == expected_mute_order -- ["mute-1"]
      after
        safe_stop(reopened)
      end
    end
  end
end

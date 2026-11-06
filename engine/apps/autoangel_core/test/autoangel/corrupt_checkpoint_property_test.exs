defmodule Autoangel.CorruptCheckpointPropertyTest do
  @moduledoc """
  Property 43 (Requirement 43.10).

  "For any Checkpoint and any subset of entries whose bytes are corrupted,
  restore succeeds, every intact entry is restored with its recorded status, and
  every corrupted entry's Series starts fresh in learning."

  This test drives the REAL durable restore path end to end: a live
  `Autoangel.StateStore` (CubDB on a tmp directory) holds a checkpoint, and a
  live `Autoangel.Workers.Worker` restores from it on `init/1`. The isolation of
  corrupt entries happens in `Autoangel.Workers.Worker.restore_entry/2`: a
  checkpoint entry is `{series_key, {1, kernel_state_bin, meta_bin}}`; on restore
  the Worker runs `Autoangel.Kernel.State.decode(kernel_state_bin)` and
  `:erlang.binary_to_term(meta_bin, [:safe])`. If EITHER fails — corrupt state
  bytes or non-term metadata bytes — the Worker logs "unreadable checkpoint data
  for series ...; starting fresh" and omits that series from its restored map, so
  it restarts fresh (status `:learning`) when it next sees a sample. Intact
  entries are restored as `%{key: key, kind: decoded.kind, kernel_state:
  state_bin, meta: meta}`, keeping their recorded metadata (and its status).

  Faithfulness notes grounded in the real code:

    * `Autoangel.StateStore.restore_worker/2` filters checkpoint entries by the
      `worker_index` embedded in each entry's metadata (see
      `checkpoint_worker?/2`). The Worker's snapshot path stamps that field via
      `Map.put(meta, :worker_index, index)`, which keeps the `Decision.Meta`
      struct and survives `:erlang.term_to_binary/2`. So every metadata we write
      carries `worker_index: 0` and the Worker under test runs at `index: 0`,
      exactly the pairing that makes entries in scope for restore.

    * For a corrupt-STATE / valid-META entry, `StateStore.checkpoint_worker?/2`
      reads the (valid) metadata, sees `worker_index: 0`, and includes it; the
      Worker then drops it at `State.decode/1`.

    * For a corrupt-META entry, `StateStore.checkpoint_worker?/2` cannot parse the
      metadata, so it returns `true` (includes the entry); the Worker then drops
      it at the `:erlang.binary_to_term/2` step.

  Both corruption modes therefore reach `restore_entry/2` and are dropped there,
  which is the faithful test of 43.10. The generator verifies that each corrupt
  binary is ACTUALLY invalid (`State.decode/1` returns `{:error, :bad_state}` for
  state corruption, `:erlang.binary_to_term/2` raises for metadata corruption)
  before relying on it.

  Isolation: the Worker keys a `:persistent_term` on its index and arms a
  missing-data sweep timer. Each run uses a fresh tmp CubDB directory and a very
  large `sweep_interval` so no sweep mutates state mid-check, with a fixed `now`
  for determinism. The StateStore and Worker are started with `name: nil` so they
  never collide with the default-named processes, and the Worker's
  persistent_term keys are erased on teardown. In the `:test` environment the
  application boots no children (see `Autoangel.Application`), so `index: 0` and
  `{Worker, 0}` are free. `async: false` because it starts GenServers and shares
  the per-index persistent_term namespace.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import ExUnit.CaptureLog

  alias Autoangel.Detect.Decision.Meta
  alias Autoangel.Kernel.State
  alias Autoangel.SeriesRegistry
  alias Autoangel.StateStore
  alias Autoangel.Workers.Worker

  # Fixed wall clock so restore and any later sample are deterministic.
  @fixed_now 1_700_000_000_000
  # Large enough that the missing-data sweep never fires during a check.
  @huge_sweep 100_000_000
  # The single worker index under test; every metadata carries this index so the
  # entries are in scope for `StateStore.restore_worker/2`.
  @worker_index 0

  # Recorded statuses an intact entry may carry; all must survive restore.
  @statuses [:learning, :healthy, :anomalous, :missing]

  defp start_store do
    path =
      Path.join(
        System.tmp_dir!(),
        "autoangel-corrupt-#{System.unique_integer([:positive])}"
      )

    {:ok, store} = StateStore.start_link(path: path, name: nil)

    on_exit(fn ->
      safe_stop(store)
      File.rm_rf!(path)
    end)

    store
  end

  # Start the Worker under test at the fixed index, restoring from `store`.
  # Returns the pid; the Worker's persistent_term keys are erased on teardown.
  defp start_worker(store) do
    {:ok, pid} =
      Worker.start_link(
        index: @worker_index,
        name: nil,
        sweep_interval: @huge_sweep,
        now: fn -> @fixed_now end,
        queue_counter: :atomics.new(1, signed: true),
        queue_capacity: 1_000_000,
        state_store: store
      )

    on_exit(fn ->
      safe_stop(pid)
      :persistent_term.erase({Worker, @worker_index})
      :persistent_term.erase({Worker, :capacity, @worker_index})
    end)

    pid
  end

  # Stops a GenServer during teardown without failing the test if the process
  # has already exited (e.g. because a dependency it was linked to was stopped
  # first, or the alive? check raced its exit).
  defp safe_stop(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
  catch
    :exit, _ -> :ok
  end

  # Build a valid metadata binary exactly as the Worker's snapshot path would:
  # a `Decision.Meta` struct stamped with `worker_index` and serialized
  # deterministically.
  defp meta_bin(key, kind, status) do
    %Meta{key: key, kind: kind, status: status}
    |> Map.put(:worker_index, @worker_index)
    |> :erlang.term_to_binary([:deterministic])
  end

  # A valid kernel-state binary for the given kind.
  defp state_bin(kind), do: State.encode(State.new(kind: kind))

  # Corrupt the kernel-state binary so `State.decode/1` fails, keeping the entry
  # tuple shape intact so `restore_entry/2`'s `{1, state_bin, meta_bin}` clause
  # still matches (the faithful decode-failure path). Truncation to a few bytes
  # is reliably rejected by `State.decode/1`.
  defp corrupt_state(valid) do
    truncated = binary_part(valid, 0, min(8, byte_size(valid)))
    {:error, :bad_state} = State.decode(truncated)
    truncated
  end

  # Corrupt the metadata binary so `:erlang.binary_to_term(_, [:safe])` raises.
  # A fixed invalid external-term encoding is used rather than random bytes:
  # random bytes occasionally decode (the external-term format ignores trailing
  # bytes), which would make a corruption generator flaky. `<<0, 1, 2, 3>>` has a
  # bad version byte (valid terms start with 131), so it always raises.
  defp corrupt_meta do
    bytes = <<0, 1, 2, 3>>
    true = raises_on_term?(bytes)
    bytes
  end

  defp raises_on_term?(binary) do
    _ = :erlang.binary_to_term(binary, [:safe])
    false
  rescue
    _ -> true
  end

  # A distinct canonical series key per index; labels differ so keys never clash.
  defp series_key(i),
    do:
      SeriesRegistry.canonical_key("corrupt_checkpoint_metric", %{"series" => Integer.to_string(i)})

  # Generates N series, each described by its kind, recorded status, and a
  # corruption choice: `:intact`, `:corrupt_state`, or `:corrupt_meta`. The whole
  # list, its corrupted subset, and the intact subset are all derivable from this.
  defp scenario do
    gen all(
          descriptors <-
            list_of(
              fixed_map(%{
                kind: member_of([:gauge, :counter]),
                status: member_of(@statuses),
                corruption: member_of([:intact, :corrupt_state, :corrupt_meta])
              }),
              min_length: 1,
              max_length: 12
            )
        ) do
      descriptors
      |> Enum.with_index()
      |> Enum.map(fn {descriptor, i} -> Map.put(descriptor, :key, series_key(i)) end)
    end
  end

  # Build the checkpoint entry `{key, {1, state_bin, meta_bin}}` for a descriptor,
  # applying the chosen corruption. Intact entries carry a valid state and a valid
  # metadata whose recorded status is `descriptor.status`.
  defp entry(%{key: key, kind: kind, status: status, corruption: corruption}) do
    valid_state = state_bin(kind)
    valid_meta = meta_bin(key, kind, status)

    value =
      case corruption do
        :intact -> {1, valid_state, valid_meta}
        :corrupt_state -> {1, corrupt_state(valid_state), valid_meta}
        :corrupt_meta -> {1, valid_state, corrupt_meta()}
      end

    {key, value}
  end

  property "corrupting any subset of checkpoint entries drops only those, restoring the rest with their recorded status" do
    check all(descriptors <- scenario(), max_runs: 40) do
      store = start_store()

      entries = Enum.map(descriptors, &entry/1)
      assert :ok = StateStore.checkpoint(store, entries)

      intact = Enum.filter(descriptors, &(&1.corruption == :intact))
      corrupted = Enum.filter(descriptors, &(&1.corruption != :intact))

      {pid, log} = with_log(fn -> start_worker(store) end)

      # Restore "succeeds": the Worker started and is alive.
      assert Process.alive?(pid)

      # Every intact entry is restored with its recorded status, kind, and the
      # exact kernel-state bytes it was checkpointed with.
      Enum.each(intact, fn %{key: key, kind: kind, status: status} ->
        series = Worker.series(pid, key)

        assert series != nil,
               "intact series #{inspect(key)} was dropped during restore"

        assert series.kind == kind
        assert Map.get(series.meta, :status) == status
        assert series.kernel_state == state_bin(kind)
      end)

      # Every corrupted entry is absent from the restored set.
      Enum.each(corrupted, fn %{key: key} ->
        assert Worker.series(pid, key) == nil,
               "corrupted series #{inspect(key)} should have been dropped"
      end)

      # A dropped series starts fresh in :learning the next time it is scored.
      Enum.each(corrupted, fn %{key: key} ->
        Worker.enqueue(pid, [fresh_sample(key)])
        series = Worker.series(pid, key)

        assert series != nil,
               "dropped series #{inspect(key)} did not start fresh on first sample"

        assert Map.get(series.meta, :status) == :learning,
               "dropped series #{inspect(key)} did not restart in :learning"
      end)

      # The "unreadable checkpoint data" error is logged once per corrupted key.
      Enum.each(corrupted, fn %{key: key} ->
        assert log =~ "unreadable checkpoint data for series #{inspect(key)}"
      end)
    end
  end

  # A minimal 4-tuple gauge sample for `key`, stamped at the fixed clock so the
  # freshly-created series is deterministic.
  defp fresh_sample(key), do: {key, 1.0, @fixed_now, :gauge}

  # Deterministic example pinning the three outcomes to concrete input: one
  # intact entry (restored with its recorded :healthy status), one corrupt-state
  # entry, and one corrupt-meta entry (both dropped and logged).
  test "intact entry survives while a corrupt-state and a corrupt-meta entry are dropped" do
    store = start_store()

    intact_key = series_key(0)
    corrupt_state_key = series_key(1)
    corrupt_meta_key = series_key(2)

    valid_state = state_bin(:gauge)

    entries = [
      {intact_key, {1, valid_state, meta_bin(intact_key, :gauge, :healthy)}},
      {corrupt_state_key,
       {1, corrupt_state(valid_state), meta_bin(corrupt_state_key, :gauge, :healthy)}},
      {corrupt_meta_key, {1, valid_state, corrupt_meta()}}
    ]

    assert :ok = StateStore.checkpoint(store, entries)

    {pid, log} = with_log(fn -> start_worker(store) end)

    assert Process.alive?(pid)

    intact = Worker.series(pid, intact_key)
    assert intact != nil
    assert Map.get(intact.meta, :status) == :healthy
    assert intact.kernel_state == valid_state

    assert Worker.series(pid, corrupt_state_key) == nil
    assert Worker.series(pid, corrupt_meta_key) == nil

    assert log =~ "unreadable checkpoint data for series #{inspect(corrupt_state_key)}"
    assert log =~ "unreadable checkpoint data for series #{inspect(corrupt_meta_key)}"

    # A dropped series starts fresh in :learning on its first sample.
    Worker.enqueue(pid, [fresh_sample(corrupt_state_key)])
    fresh = Worker.series(pid, corrupt_state_key)
    assert fresh != nil
    assert Map.get(fresh.meta, :status) == :learning
  end
end

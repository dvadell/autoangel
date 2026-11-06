defmodule Autoangel.CheckpointRoundtripPropertyTest do
  # async: false because each case starts named-nil GenServers (a StateStore and
  # two Workers) and mutates process-global :persistent_term keyed by worker
  # index. Running concurrently would race on those keys.
  use ExUnit.Case, async: false
  use ExUnitProperties

  alias Autoangel.Detect.Decision
  alias Autoangel.Detect.Decision.Meta
  alias Autoangel.Kernel.Scoring.Elixir, as: ScoringElixir
  alias Autoangel.Kernel.State
  alias Autoangel.StateStore
  alias Autoangel.Workers.Worker

  # **Validates: Requirements 43.5, 43.7, 49.2**
  # Feature: v1, Property 42: checkpoint round trip
  #
  # "For any Baseline state and Series metadata produced by feeding a random
  #  Sample sequence, encoding to a Checkpoint entry and decoding gives a state
  #  that, fed any further Sample sequence, yields the same scores and the same
  #  Series_Status sequence as the original, including a learning Series
  #  continuing its Learning_Period."
  #
  # This test drives the REAL persistence path rather than hand-building and
  # hand-decoding a checkpoint entry. The full path exercised per case is:
  #
  #   Worker.put_series/4 -> Worker.snapshot/1 -> StateStore.checkpoint/2
  #     -> (new Worker on the same index) Worker.init/1 -> restore_entries/1
  #        -> StateStore.restore_worker/2 -> Worker.restore_entry/2 -> Worker.series/2
  #
  # so a regression in snapshot (dropping fields, wrong meta stamping, wrong
  # kind) or restore (resetting meta, using the wrong kind, corrupting the
  # kernel bytes) is caught by the continued-scoring assertions below.

  @learning_cfg_duration 3_600

  defp fresh_kernel_state(kind), do: State.encode(State.new(kind: kind))

  defp fresh_meta(kind), do: %Meta{kind: kind, status: :learning}

  # Decision.step reads the kind from the result map (falling back to meta.kind).
  # We tag every result with the series kind so counter/gauge threading is explicit.
  defp tag_kind(result, kind), do: Map.put(result, :kind, kind)

  # Feed a sample sequence through the kernel (threading state) and the detector
  # (threading meta), using each sample's timestamp as the receive time so the
  # learning clock advances monotonically. Returns the final kernel state binary,
  # the final meta, the per-sample scores, and the per-sample status sequence.
  defp run(state_bin, meta, kind, samples, cfg) do
    Enum.reduce(samples, {state_bin, meta, [], []}, fn sample,
                                                       {acc_state, acc_meta, scores, statuses} ->
      {ts, _value, _kind} = sample
      {:ok, next_state, [result]} = ScoringElixir.score(acc_state, [sample])
      {next_meta, _events} = Decision.step(acc_meta, tag_kind(result, kind), ts, cfg)

      {next_state, next_meta, [result.score | scores], [next_meta.status | statuses]}
    end)
    |> then(fn {state_bin, meta, scores, statuses} ->
      {state_bin, meta, Enum.reverse(scores), Enum.reverse(statuses)}
    end)
  end

  # Drive the baseline state + metadata through the REAL checkpoint/restore path
  # and return the restored {kernel_state_bin, meta, kind} exactly as a freshly
  # started Worker would see them after recovering from disk.
  #
  # Steps:
  #   1. Start a StateStore over a unique tmp dir and a Worker on a unique index.
  #   2. Hand the baseline state + meta to the Worker via put_series/4.
  #   3. Snapshot the Worker (stamps worker_index, serializes meta) and persist
  #      the entries via StateStore.checkpoint/2.
  #   4. Start a SECOND Worker on the same index and store; its init restores
  #      the series from the checkpoint filtered by worker_index.
  #   5. Read the restored series back with Worker.series/2.
  #
  # Cleanup (GenServers, persistent_term, tmp dir) runs inline afterwards. A
  # failing assertion aborts the surrounding test, which may leak the resources
  # for that single failing case only; the OS tmp dir is small and the leak is
  # bounded, so we favour inline teardown over per-iteration on_exit (which is
  # not available inside `check all`).
  defp real_round_trip(state_after_prefix_bin, meta_after_prefix, key) do
    index = System.unique_integer([:positive])
    tmp = Path.join(System.tmp_dir!(), "aa_ckpt_#{index}_#{System.unique_integer([:positive])}")
    fixed = 1_700_000_000_000

    {:ok, store} = StateStore.start_link(path: tmp, name: nil)

    {:ok, a} =
      Worker.start_link(
        index: index,
        state_store: store,
        sweep_interval: 100_000_000,
        now: fn -> fixed end,
        name: nil
      )

    try do
      :ok = Worker.put_series(a, key, state_after_prefix_bin, meta_after_prefix)
      {:ok, entries} = Worker.snapshot(a)
      :ok = StateStore.checkpoint(store, entries)

      {:ok, b} =
        Worker.start_link(
          index: index,
          state_store: store,
          sweep_interval: 100_000_000,
          now: fn -> fixed end,
          name: nil
        )

      try do
        restored = Worker.series(b, key)
        refute is_nil(restored), "restored series must not be nil after real checkpoint restore"
        {restored.kernel_state, restored.meta, restored.kind}
      after
        safe_stop(b)
      end
    after
      safe_stop(a)
      safe_stop(store)
      :persistent_term.erase({Worker, index})
      :persistent_term.erase({Worker, :capacity, index})
      File.rm_rf!(tmp)
    end
  end

  defp safe_stop(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
    :ok
  catch
    :exit, _ -> :ok
  end

  # Build a monotonically increasing sample sequence starting at `start_ts`.
  # Each step advances the timestamp by a strictly positive amount so counter
  # rates are always defined.
  defp build_samples(start_ts, steps, kind) do
    {samples, _ts} =
      Enum.map_reduce(steps, start_ts, fn {step, value}, ts ->
        ts = ts + step
        {{ts, value * 1.0, kind}, ts}
      end)

    samples
  end

  property "Feature: v1, Property 42: checkpoint round trip preserves scores and status" do
    check all(
            kind <- member_of([:gauge, :counter]),
            # Learning duration in seconds; controls when a learning series flips
            # to healthy once latest_ts - first_ts >= duration * 1000.
            learning_duration <- integer(1..86_400),
            prefix_steps <-
              list_of(tuple({integer(1..5_000), integer(-1_000..1_000)}),
                min_length: 1,
                max_length: 40
              ),
            suffix_steps <-
              list_of(tuple({integer(1..5_000), integer(-1_000..1_000)}),
                min_length: 1,
                max_length: 40
              ),
            # Each case starts two Workers + a StateStore (disk), so keep the run
            # count modest.
            max_runs: 50
          ) do
      cfg = %{learning: %{duration: learning_duration}}
      key = {"metric_#{kind}", %{"i" => Integer.to_string(learning_duration)}}

      prefix = build_samples(0, prefix_steps, kind)
      {last_prefix_ts, _value, _kind} = List.last(prefix)
      suffix = build_samples(last_prefix_ts, suffix_steps, kind)

      # 1. Build state + meta by feeding the prefix.
      {state_after_prefix_bin, meta_after_prefix, _prefix_scores, _prefix_statuses} =
        run(fresh_kernel_state(kind), fresh_meta(kind), kind, prefix, cfg)

      # 2. Round trip through the REAL persistence path (snapshot -> checkpoint
      #    -> new Worker init -> restore).
      {restored_state_bin, restored_meta, restored_kind} =
        real_round_trip(state_after_prefix_bin, meta_after_prefix, key)

      # The kernel bytes and the kind must survive untouched.
      assert restored_state_bin == state_after_prefix_bin
      assert restored_kind == kind

      # 3. Feed the suffix from both the original and the restored checkpoint.
      {original_final_state, _original_meta, original_scores, original_statuses} =
        run(state_after_prefix_bin, meta_after_prefix, kind, suffix, cfg)

      {restored_final_state, _restored_final_meta, restored_scores, restored_statuses} =
        run(restored_state_bin, restored_meta, kind, suffix, cfg)

      # Identical per-sample scores, identical status sequence, identical final state.
      assert restored_scores == original_scores
      assert restored_statuses == original_statuses
      assert restored_final_state == original_final_state
    end
  end

  test "a learning series continues its learning period across a checkpoint (req 43.5)" do
    # Short prefix well inside a large learning window: the series is still
    # learning at the checkpoint point. The suffix then crosses the learning
    # boundary. Driving through the REAL restore path, the restored series must
    # transition to :healthy at the SAME absolute suffix index as the original,
    # proving the learning clock (first_ts / latest_ts) was preserved on disk.
    kind = :gauge
    cfg = %{learning: %{duration: @learning_cfg_duration}}
    boundary_ms = @learning_cfg_duration * 1_000
    key = {"learning_continuation", %{}}

    # Prefix: three samples spaced 60s apart -> timestamps 60_000, 120_000,
    # 180_000, so first_ts = 60_000 and latest_ts = 180_000. latest - first =
    # 120_000, far below the 3_600_000 ms boundary, so still learning.
    prefix = build_samples(0, [{60_000, 10}, {60_000, 11}, {60_000, 9}], kind)

    {state_after_prefix_bin, meta_after_prefix, _scores, prefix_statuses} =
      run(fresh_kernel_state(kind), fresh_meta(kind), kind, prefix, cfg)

    assert meta_after_prefix.status == :learning
    assert List.last(prefix_statuses) == :learning

    {restored_state_bin, restored_meta, _restored_kind} =
      real_round_trip(state_after_prefix_bin, meta_after_prefix, key)

    assert restored_meta.status == :learning

    # Suffix timestamps (first_ts stays 60_000): learning ends once
    # latest_ts - first_ts >= boundary_ms, i.e. latest_ts >= 3_660_000.
    #   idx 0: ts = 240_000       (< 3_660_000 -> learning)
    #   idx 1: ts = 3_600_000     (< 3_660_000 -> learning)
    #   idx 2: ts = 3_720_000     (>= 3_660_000 -> healthy)  <-- flip
    #   idx 3: ts = 3_780_000     (healthy)
    {last_prefix_ts, _v, _k} = List.last(prefix)

    suffix =
      build_samples(
        last_prefix_ts,
        [
          {60_000, 10},
          # jump close to but before the boundary
          {boundary_ms - 60_000 - last_prefix_ts, 10},
          # this sample crosses first_ts + boundary_ms
          {120_000, 10},
          {60_000, 10}
        ],
        kind
      )

    expected_flip_index = 2

    {_orig_state, _orig_meta, _orig_scores, original_statuses} =
      run(state_after_prefix_bin, meta_after_prefix, kind, suffix, cfg)

    {_rest_state, _rest_meta, _rest_scores, restored_statuses} =
      run(restored_state_bin, restored_meta, kind, suffix, cfg)

    # The learning progress is preserved through the real checkpoint: both
    # sequences are identical and both flip at the SAME absolute suffix index.
    assert restored_statuses == original_statuses
    assert :learning in original_statuses
    assert :healthy in original_statuses

    original_flip_index = Enum.find_index(original_statuses, &(&1 == :healthy))
    restored_flip_index = Enum.find_index(restored_statuses, &(&1 == :healthy))
    assert original_flip_index == expected_flip_index
    assert restored_flip_index == expected_flip_index

    # CONTRAST: run the SAME suffix from a FRESH meta (no carried learning
    # progress). Its first_ts is set by the first suffix sample (240_000), so
    # learning only ends once latest_ts >= 240_000 + 3_600_000 = 3_840_000 — a
    # timestamp the suffix never reaches. It must therefore NOT flip to healthy.
    # If the checkpoint had dropped the learning progress (first_ts), the
    # restored run above would behave like this fresh run instead.
    {_fresh_state, _fresh_meta, _fresh_scores, fresh_statuses} =
      run(fresh_kernel_state(kind), fresh_meta(kind), kind, suffix, cfg)

    refute :healthy in fresh_statuses
    assert is_nil(Enum.find_index(fresh_statuses, &(&1 == :healthy)))
    assert restored_statuses != fresh_statuses
  end

  test "checkpoint of an already-healthy series keeps continued scores identical (req 43.7)" do
    kind = :gauge
    # Tiny learning duration so the series is healthy almost immediately.
    cfg = %{learning: %{duration: 1}}
    key = {"already_healthy", %{}}

    prefix =
      build_samples(0, [{1_000, 10}, {1_000, 10}, {1_000, 11}, {1_000, 9}, {1_000, 10}], kind)

    {state_after_prefix_bin, meta_after_prefix, _scores, _statuses} =
      run(fresh_kernel_state(kind), fresh_meta(kind), kind, prefix, cfg)

    assert meta_after_prefix.status == :healthy

    {restored_state_bin, restored_meta, restored_kind} =
      real_round_trip(state_after_prefix_bin, meta_after_prefix, key)

    assert restored_state_bin == state_after_prefix_bin
    assert restored_kind == kind
    assert restored_meta.status == :healthy

    {last_prefix_ts, _v, _k} = List.last(prefix)
    # Include an outlier so scores are non-trivial on the continued stream.
    suffix = build_samples(last_prefix_ts, [{1_000, 10}, {1_000, 500}, {1_000, 10}], kind)

    {original_final_state, _om, original_scores, original_statuses} =
      run(state_after_prefix_bin, meta_after_prefix, kind, suffix, cfg)

    {restored_final_state, _rm, restored_scores, restored_statuses} =
      run(restored_state_bin, restored_meta, kind, suffix, cfg)

    assert restored_scores == original_scores
    assert restored_statuses == original_statuses
    assert restored_final_state == original_final_state
    assert Enum.any?(original_scores, &(is_float(&1) and &1 > 0.0))
  end
end

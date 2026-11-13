defmodule Autoangel.Workers.WarmStartTest do
  @moduledoc """
  Task 34.1 and 34.2: a new Series without a checkpoint is warmed from stored
  history, each Sample is applied once, and stored Samples raise no alerts.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import ExUnit.CaptureLog

  alias Autoangel.Detect.Decision.Meta
  alias Autoangel.{History, SeriesRegistry}
  alias Autoangel.Kernel.{Params, State}
  alias Autoangel.Test.HistoryBackend
  alias Autoangel.Workers.{HistoryOps, Worker}

  @key {"warm_metric", []}
  @id SeriesRegistry.series_id(@key)
  @step 60_000
  @now 10_000_000_000

  # Feature: v1, Property 44: Warm start applies each Sample once
  property "every distinct timestamp is applied exactly once, stored-only first and then the buffer" do
    ts = integer(0..60)

    check all(
            stored_ts <- list_of(ts, max_length: 40),
            buffer_ts <- list_of(ts, max_length: 40)
          ) do
      stored = Enum.map(stored_ts, &{&1, {:stored, &1}})
      buffer = Enum.map(Enum.uniq(buffer_ts), &%{timestamp: &1, from: :live})

      {stored_only, ordered_buffer} = HistoryOps.plan(stored, buffer, & &1.timestamp)

      applied = Enum.map(stored_only, &elem(&1, 0)) ++ Enum.map(ordered_buffer, & &1.timestamp)
      expected = (stored_ts ++ buffer_ts) |> Enum.uniq() |> Enum.sort()

      # Exactly once each.
      assert Enum.sort(applied) == expected
      assert length(applied) == length(Enum.uniq(applied))

      # Stored Samples come first and in order; the buffer is kept whole and in order.
      assert Enum.map(stored_only, &elem(&1, 0)) == Enum.sort(Enum.map(stored_only, &elem(&1, 0)))
      assert Enum.map(ordered_buffer, & &1.timestamp) == Enum.sort(Enum.uniq(buffer_ts))
      assert Enum.all?(ordered_buffer, &(&1.from == :live))

      # A timestamp present in both is applied from the live buffer, not twice.
      live = MapSet.new(buffer_ts)
      assert Enum.all?(stored_only, fn {t, _} -> not MapSet.member?(live, t) end)
    end
  end

  describe "in a Worker" do
    setup do
      HistoryBackend.clear()
      on_exit(&HistoryBackend.clear/0)
      start_supervised!({Task.Supervisor, name: Autoangel.Background.Tasks})

      start_supervised!(
        {History.Server, backend: HistoryBackend, backend_config: %{report_to: self()}}
      )

      :ok
    end

    defp start_worker(opts \\ []) do
      index = System.unique_integer([:positive])

      {:ok, pid} =
        Worker.start_link(
          Keyword.merge(
            [
              index: index,
              name: nil,
              max_batch: 2,
              kernel_params: %Params{},
              sweep_interval: 3_600_000,
              sweep_timer: false,
              now: fn -> @now end,
              queue_counter: :atomics.new(1, signed: true),
              queue_capacity: 1_000_000,
              scrape_status_table: nil,
              state_store: nil,
              dispatcher: nil,
              correlator: self(),
              storage_writer: nil,
              history: true,
              decision_cfg: %{learning: %{duration: 1_000, cycles: 2}, persistence: %{n: 3, m: 5}}
            ],
            opts
          )
        )

      on_exit(fn ->
        :persistent_term.erase({Worker, index})
        :persistent_term.erase({Worker, :capacity, index})
      end)

      pid
    end

    defp sample(ts, value),
      do: %{name: "warm_metric", labels: %{}, value: value, timestamp: ts, received_at: ts}

    defp stored_rows(range, value_fun) do
      for i <- range, do: {@id, @now - 3_000_000 + i * @step, value_fun.(i)}
    end

    defp quiet_value(i), do: 50.0 + rem(i, 7) * 0.1

    defp settle(pid), do: :sys.get_state(pid)

    defp started_alerts(acc \\ []) do
      receive do
        {:"$gen_cast", {:anomaly_started, a}} -> started_alerts([a | acc])
      after
        100 -> Enum.reverse(acc)
      end
    end

    test "stored history becomes the Baseline and raises no alert, even where it holds a spike" do
      spike = fn i -> if i in 30..36, do: 900.0, else: quiet_value(i) end
      rows = stored_rows(0..49, spike)
      HistoryBackend.put_rows(rows)
      first_stored = rows |> hd() |> elem(1)

      pid = start_worker()
      live_ts = @now - 100_000
      Worker.enqueue(pid, [sample(live_ts, 50.0)])
      assert_receive {:read, :raw, [@id], _from, @now}, 1_000
      Process.sleep(100)
      settle(pid)

      entry = Worker.series(pid, @key)
      refute entry.meta.warming?
      refute Map.has_key?(entry, :warm_buffer)
      # Learning is evaluated from the earliest stored Sample.
      assert entry.meta.first_ts == first_stored
      assert entry.meta.open_value_anomaly == nil
      assert started_alerts() == []

      # The Baseline was learned from history: a live spike now alerts at once.
      live = for i <- 1..4, do: sample(live_ts + i * @step, 900.0)
      Worker.enqueue(pid, live)
      settle(pid)
      assert [%{kind: :value}] = started_alerts()
    end

    test "a live Sample that is also stored is applied once" do
      rows = stored_rows(0..49, &quiet_value/1)
      HistoryBackend.put_rows(rows)
      {_, dup_ts, _} = List.last(rows)

      pid = start_worker()
      Worker.enqueue(pid, [sample(dup_ts, 50.0)])
      Process.sleep(150)
      settle(pid)

      state_bin = Worker.series(pid, @key).kernel_state
      {:ok, state} = State.decode(state_bin)
      assert length(state.window) == 50
    end

    test "Samples arriving while warming are buffered and applied after history" do
      rows = stored_rows(0..49, &quiet_value/1)
      HistoryBackend.put_rows({:sleep, 100, rows})

      pid = start_worker(warm_timeout: 2_000)
      Worker.enqueue(pid, [sample(@now - 50_000, 50.0)])
      settle(pid)
      assert Worker.series(pid, @key).meta.warming?
      Worker.enqueue(pid, [sample(@now - 40_000, 50.0), sample(@now - 30_000, 50.0)])
      settle(pid)
      assert Worker.series(pid, @key).meta.warming?

      Process.sleep(300)
      settle(pid)
      entry = Worker.series(pid, @key)
      refute entry.meta.warming?
      {:ok, state} = State.decode(entry.kernel_state)
      assert length(state.window) == 53
    end

    test "a read that is too slow starts the Series fresh with its buffered Samples" do
      HistoryBackend.put_rows({:sleep, 400, stored_rows(0..49, &quiet_value/1)})
      pid = start_worker(warm_timeout: 50)
      Worker.enqueue(pid, [sample(@now - 50_000, 50.0), sample(@now - 40_000, 50.0)])
      Process.sleep(150)
      settle(pid)

      entry = Worker.series(pid, @key)
      refute entry.meta.warming?
      {:ok, state} = State.decode(entry.kernel_state)
      assert length(state.window) == 2

      # The late history result is ignored.
      Process.sleep(500)
      settle(pid)
      {:ok, later} = State.decode(Worker.series(pid, @key).kernel_state)
      assert length(later.window) == 2
    end

    test "a failed read starts the Series fresh" do
      HistoryBackend.put_rows({:error, :down})
      pid = start_worker()

      capture_log(fn ->
        Worker.enqueue(pid, [sample(@now - 50_000, 50.0)])
        Process.sleep(150)
        settle(pid)
      end)

      entry = Worker.series(pid, @key)
      refute entry.meta.warming?
      {:ok, state} = State.decode(entry.kernel_state)
      assert length(state.window) == 1
    end

    test "without storage, a new Series is not warmed" do
      pid = start_worker(history: false)
      Worker.enqueue(pid, [sample(@now - 50_000, 50.0)])
      settle(pid)
      refute Worker.series(pid, @key).meta.warming?
      refute_received {:read, _, _, _, _}
    end

    test "a Series restored from a checkpoint is not warmed" do
      pid = start_worker()

      Worker.put_series(
        pid,
        @key,
        State.encode(State.new()),
        %Meta{key: @key}
      )

      Worker.enqueue(pid, [sample(@now - 50_000, 50.0)])
      settle(pid)
      refute Worker.series(pid, @key).meta.warming?
      refute_received {:read, _, _, _, _}
    end
  end
end

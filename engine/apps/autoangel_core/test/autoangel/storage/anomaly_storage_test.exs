defmodule Autoangel.Storage.AnomalyStorageTest do
  @moduledoc """
  Task 21.1 producer side: Series rows at creation and on status change, ahead
  of dependent items; Anomaly starts with all Candidates and updates without
  them; baseline bands flushed once per checkpoint; Writer routes :anomaly
  items to the backend.
  """
  use ExUnit.Case, async: true

  alias Autoangel.{Checkpointer, Correlate, SeriesRegistry}
  alias Autoangel.Detect.Decision.Meta
  alias Autoangel.Kernel.{Params, State}
  alias Autoangel.Storage.{Rows, Writer}
  alias Autoangel.Workers.Worker

  @key {"latency", [{"host", "h1"}]}

  defp start_worker(opts) do
    index = System.unique_integer([:positive])

    {:ok, pid} =
      Worker.start_link(
        opts ++
          [
            index: index,
            name: nil,
            max_batch: 2,
            kernel_params: %Params{},
            sweep_interval: 3_600_000,
            now: fn -> 1_000 end,
            queue_counter: :atomics.new(1, signed: true),
            queue_capacity: 1_000_000,
            scrape_status_table: nil,
            state_store: nil,
            dispatcher: nil,
            correlator: nil
          ]
      )

    on_exit(fn ->
      :persistent_term.erase({Worker, index})
      :persistent_term.erase({Worker, :capacity, index})
    end)

    pid
  end

  defp casts(acc \\ []) do
    receive do
      {:"$gen_cast", {:enqueue, kind, [item]}} -> casts([{kind, item} | acc])
    after
      50 -> Enum.reverse(acc)
    end
  end

  describe "Rows" do
    test "anomaly ids map to 16 bytes deterministically" do
      hex = "0123456789abcdef0123456789abcdef"
      assert Rows.anomaly_uuid(hex) == Base.decode16!(hex, case: :lower)
      assert byte_size(Rows.anomaly_uuid("not-a-uuid")) == 16
      assert Rows.anomaly_uuid("x") == Rows.anomaly_uuid("x")
      raw = :crypto.strong_rand_bytes(16)
      assert Rows.anomaly_uuid(raw) == raw
    end

    test "a start carries every candidate with rank, evidence and weak; an update has none" do
      anomaly = %{id: "a1", series_key: @key, start_ms: 5, score: 80.0, learning?: true}
      cands = [%{series_key: {"cpu", []}, rank: 1, weak?: true, evidence: %{support: 0}}]

      item = Rows.anomaly_item(anomaly, cands)
      assert item.anomaly.status == :open and item.anomaly.learning == true
      assert item.anomaly.series_id == SeriesRegistry.series_id(@key)
      assert [%{rank: 1, weak: true, evidence: %{weak?: true, support: 0}}] = item.candidates

      update = Rows.anomaly_update(Map.put(anomaly, :end_ms, 9))
      refute Map.has_key?(update, :candidates)
      assert update.anomaly.status == :resolved and update.anomaly.end_time == 9
    end
  end

  describe "Worker" do
    test "enqueues the Series row at creation, ahead of later items, and on status change" do
      pid = start_worker(storage_writer: self(), decision_cfg: %{learning: %{duration: 0}})

      Worker.enqueue(pid, for(t <- 1..4, do: {@key, 10.0 + t, t * 1_000, :gauge}))
      _ = Worker.series(pid, @key)

      items = casts()
      assert [{:series, first} | _] = items
      assert first == Rows.series(@key, :gauge, :learning)

      statuses = for {:series, row} <- items, do: row.status
      assert :learning in statuses
      assert Enum.any?(statuses, &(&1 != :learning)), "status change must update the row"
      GenServer.stop(pid)
    end

    test "a restored Series has its row enqueued before any dependent item" do
      pid = start_worker(storage_writer: self())
      state = State.encode(State.new(kind: :gauge))
      meta = %Meta{key: @key, kind: :gauge, status: :healthy}
      :ok = Worker.put_series(pid, @key, state, meta)

      Worker.enqueue(pid, [{@key, 10.0, 1_000, :gauge}])
      _ = Worker.series(pid, @key)

      assert [{:series, row} | _] = casts()
      assert row == Rows.series(@key, :gauge, :healthy)
      GenServer.stop(pid)
    end

    test "a missing-data sweep that silences a Series updates its stored status" do
      pid = start_worker(storage_writer: self(), now: fn -> 10_000_000 end)
      state = State.encode(State.new(kind: :gauge))

      meta = %Meta{
        key: @key,
        kind: :gauge,
        status: :healthy,
        last_recv_ms: 0,
        typical_interval_ms: 1_000
      }

      :ok = Worker.put_series(pid, @key, state, meta)
      send(pid, :sweep)
      _ = Worker.series(pid, @key)

      statuses = for {:series, row} <- casts(), do: row.status
      assert :silent in statuses
      GenServer.stop(pid)
    end

    test "records the latest band and take_bands returns then clears it" do
      pid = start_worker(storage_writer: nil)
      Worker.enqueue(pid, for(t <- 1..6, do: {@key, 10.0 + rem(t, 3), t * 1_000, :gauge}))

      assert [band] = Worker.take_bands(pid)
      assert band.series_id == SeriesRegistry.series_id(@key)
      assert band.ts == 6_000
      assert is_number(band.lower) and is_number(band.upper) and band.lower <= band.upper
      assert Worker.take_bands(pid) == []
      GenServer.stop(pid)
    end
  end

  describe "Checkpointer" do
    test "flushes one band per Series each interval to the Storage_Writer" do
      worker = start_worker(storage_writer: nil)
      Worker.enqueue(worker, for(t <- 1..3, do: {@key, 10.0, t * 1_000, :gauge}))
      _ = Worker.series(worker, @key)

      {:ok, store} = Autoangel.StateStore.start_link(name: nil, path: tmp_dir())

      {:ok, cp} =
        Checkpointer.start_link(
          name: nil,
          workers: [worker],
          state_store: store,
          interval_ms: 3_600_000,
          storage_writer: self()
        )

      # A synchronous call after the tick is a barrier: the tick (and its band
      # casts) has been handled by the time it returns.
      send(cp, :checkpoint)
      _ = Checkpointer.workers(cp)
      assert [{:band, band}] = casts()
      assert band.series_id == SeriesRegistry.series_id(@key)

      send(cp, :checkpoint)
      _ = Checkpointer.workers(cp)
      assert casts() == [], "no new samples means no new band"
      Enum.each([cp, store, worker], &GenServer.stop/1)
    end
  end

  describe "Correlator" do
    test "stores starts with candidates and resolutions as updates" do
      {:ok, pid} =
        Correlate.start_link(name: nil, dispatcher: nil, storage_writer: self(), now: fn -> 0 end)

      a = %{id: "a1", series_key: @key, start_ms: 1_000, score: 80.0}
      b = %{id: "b1", series_key: {"cpu", []}, start_ms: 1_010, score: 70.0}
      Correlate.correlate(pid, a)
      Correlate.correlate(pid, b)
      Correlate.anomaly_resolved(pid, Map.put(b, :end_ms, 2_000))
      _ = Correlate.correlate(pid, %{id: "z", series_key: {"z", []}, start_ms: 99_999_999})

      assert [{:anomaly, s1}, {:anomaly, s2}, {:anomaly, upd} | _] = casts()
      assert s1.candidates == []
      assert [%{rank: 1}] = s2.candidates
      refute Map.has_key?(upd, :candidates)
      assert upd.anomaly.status == :resolved
      GenServer.stop(pid)
    end
  end

  defmodule Backend do
    @moduledoc false
    @behaviour Autoangel.Storage
    def init(%{test: pid}), do: {:ok, pid}
    def append_samples(_h, _r), do: :ok
    def write_series(h, r), do: send(h, {:written, :series, r}) && :ok
    def write_anomalies(h, r), do: send(h, {:written, :anomaly, r}) && :ok
    def write_bands(h, r), do: send(h, {:written, :band, r}) && :ok
    def read_range(_h, _ids, _f, _t), do: {:ok, []}
    def read_aggregates(_h, _ids, _f, _t, _r), do: {:ok, []}
  end

  test "the Writer routes :anomaly items to write_anomalies, FIFO after the Series row" do
    {:ok, writer} =
      Writer.start_link(
        name: nil,
        backend: Backend,
        backend_config: %{test: self()},
        migration_ready: true
      )

    Writer.enqueue_async(writer, :series, %{id: 1})
    Writer.enqueue_async(writer, :anomaly, %{anomaly: %{id: 2}, candidates: []})

    assert_receive {:written, :series, [%{id: 1}]}
    assert_receive {:written, :anomaly, [%{anomaly: %{id: 2}}]}
    GenServer.stop(writer)
  end

  defp tmp_dir do
    dir = Path.join(System.tmp_dir!(), "aa-anom-#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf!(dir) end)
    dir
  end
end

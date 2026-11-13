defmodule Autoangel.HistoryTest do
  @moduledoc """
  Task 33.1 and 33.2: History reads are clamped, run as background Tasks, log
  failures by name, and are never issued without storage.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import ExUnit.CaptureLog

  alias Autoangel.History
  alias Autoangel.Test.HistoryBackend

  @day 86_400_000

  setup do
    on_exit(&HistoryBackend.clear/0)
    :ok
  end

  # Feature: v1, Property 41: History reads stay within bounds
  property "every read asks for at most 200 Series and 15 days, ending where requested" do
    check all(
            count <- integer(0..500),
            to <- integer(0..(1_000 * @day)),
            span <- integer(0..(100 * @day)),
            max_series <- one_of([constant(nil), integer(1..300)]),
            max_range_days <- one_of([constant(nil), integer(1..30)])
          ) do
      ids = for i <- 1..count//1, do: <<i::128>>
      from = to - span
      opts = [backend: HistoryBackend, handle: self()]
      opts = if max_series, do: [{:max_series, max_series} | opts], else: opts
      opts = if max_range_days, do: [{:max_range_ms, max_range_days * @day} | opts], else: opts

      assert {:ok, _rows} = History.read(ids, from, to, opts)

      limit_series = max_series || 200
      limit_range = (max_range_days || 15) * @day

      if count == 0 do
        refute_received {:read, _, _, _, _}
      else
        assert_received {:read, :raw, read_ids, read_from, read_to}
        assert length(read_ids) == min(count, limit_series)
        assert read_ids == Enum.take(ids, limit_series)
        assert read_to == to
        assert read_to - read_from <= limit_range
        assert read_from >= from
        assert read_from == max(from, to - limit_range)
      end
    end
  end

  test "an hourly read uses the aggregate reader" do
    assert {:ok, []} =
             History.read([<<1::128>>], 0, @day,
               backend: HistoryBackend,
               handle: self(),
               resolution: :hour
             )

    assert_received {:read, :hour, [<<1::128>>], 0, @day}
  end

  test "with no storage backend no read is issued" do
    {:ok, server} = History.Server.start_link(backend: nil, name: :history_none)
    assert {:error, :no_storage} = History.read([<<1::128>>], 0, 10, server: server)
    refute_received {:read, _, _, _, _}

    {:ok, none} = History.Server.start_link(backend: :none, name: :history_none_atom)
    assert {:error, :no_storage} = History.read([<<1::128>>], 0, 10, server: none)
  end

  test "the server connects once and reuses the handle" do
    {:ok, server} =
      History.Server.start_link(
        backend: HistoryBackend,
        backend_config: %{report_to: self()},
        name: :history_conn
      )

    assert {:ok, []} = History.read([<<1::128>>], 0, 10, server: server)
    assert_received {:read, :raw, _, _, _}
    assert {:ok, _, handle} = History.Server.connection(server)
    assert handle == self()
  end

  test "a read failure is an error, not a crash" do
    HistoryBackend.put_rows({:error, :timeout})

    assert {:error, :timeout} =
             History.read([<<1::128>>], 0, 10, backend: HistoryBackend, handle: self())
  end

  describe "background analyses" do
    setup do
      start_supervised!({Task.Supervisor, name: Autoangel.Background.Tasks})
      :ok
    end

    test "run in another process and reply to the caller" do
      parent = self()

      {:ok, _} =
        History.async("seasonality", [:a], fn -> send(parent, {:ran_in, self()}) && {:ok, 1} end,
          reply_to: parent
        )

      assert_receive {:ran_in, pid}
      assert pid != parent
      assert_receive {:history_result, "seasonality", [:a], {:ok, 1}}
    end

    test "a failure logs a warning naming the analysis and the Series" do
      log =
        capture_log(fn ->
          {:ok, _} =
            History.async("warm start", [{"cpu_load", [{"host", "a"}]}], fn -> {:error, :boom} end,
              reply_to: self()
            )

          assert_receive {:history_result, "warm start", _, {:error, :boom}}
        end)

      assert log =~ "warm start"
      assert log =~ "cpu_load"
    end

    test "a crash logs a warning and still replies" do
      log =
        capture_log(fn ->
          {:ok, _} =
            History.async("seasonality", [:x], fn -> raise "bad read" end, reply_to: self())

          assert_receive {:history_result, "seasonality", [:x], {:error, %RuntimeError{}}}
        end)

      assert log =~ "bad read"
    end
  end

  defp eventually(fun, tries \\ 50) do
    cond do
      fun.() -> true
      tries == 0 -> false
      true -> Process.sleep(20) && eventually(fun, tries - 1)
    end
  end

  describe "pair seeding" do
    test "pairs of Anomaly starts inside the window are counted once per occurrence" do
      events = [{:a, 0}, {:b, 100}, {:c, 5_000}, {:a, 10_000}, {:b, 10_050}]
      pairs = History.cooccurring_pairs(events, 600) |> Map.new()
      assert pairs == %{{:a, :b} => 2}
    end

    test "the same Series twice is not a pair" do
      assert History.cooccurring_pairs([{:a, 0}, {:a, 10}], 600) == []
    end

    test "seeding adds the counts to the Correlator's pair table" do
      start_supervised!({Task.Supervisor, name: Autoangel.Background.Tasks})
      HistoryBackend.put_rows([{:a, 0}, {:b, 100}])

      start_supervised!(
        {History.Server, backend: HistoryBackend, backend_config: %{report_to: self()}}
      )

      {:ok, correlate} = Autoangel.Correlate.start_link(name: :seeded_correlate)

      {:ok, _} =
        History.seed_pairs(
          correlator: correlate,
          window_ms: 600,
          now: 10_000,
          server: Autoangel.History.Server
        )

      assert_receive {:read, :anomaly_events, [], from, 10_000}, 1_000
      assert from == 10_000 - 15 * @day
      assert eventually(fn -> Autoangel.Correlate.pairs(correlate) == [{{:a, :b}, 1}] end)
    end
  end
end

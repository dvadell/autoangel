defmodule Autoangel.CorrelateTest do
  @moduledoc """
  Behavioural unit tests for `Autoangel.Correlate` (task 19.1) that complement
  the Property 23/24/25 suites (tasks 19.2-19.4): init validation, the learning
  gate, forwarding robustness, support excluding the current co-occurrence, and
  end-to-end delivery of enriched events through the Correlator.
  """
  use ExUnit.Case, async: true

  alias Autoangel.Correlate

  defp key(name, labels), do: {name, Enum.sort(labels)}

  defp anomaly(name, labels, start_ms, opts \\ []) do
    %{
      id: Keyword.get(opts, :id, "#{name}-#{start_ms}"),
      series_key: key(name, labels),
      start_ms: start_ms,
      score: Keyword.get(opts, :score, 80.0),
      learning?: Keyword.get(opts, :learning?, false)
    }
  end

  defp start_correlate!(opts) do
    opts = Keyword.put_new(opts, :name, nil)
    {:ok, pid} = Correlate.start_link(opts)
    pid
  end

  describe "init validation (Req 19.8)" do
    test "refuses to start when the pool limit is not a positive integer" do
      Process.flag(:trap_exit, true)

      assert {:error, {:invalid_setting, :candidate_pool_size_limit, 0}} =
               Correlate.start_link(name: nil, pool_limit: 0)

      assert {:error, {:invalid_setting, :candidate_pool_size_limit, -3}} =
               Correlate.start_link(name: nil, pool_limit: -3)
    end

    test "refuses to start when the support threshold is negative" do
      Process.flag(:trap_exit, true)

      assert {:error, {:invalid_setting, :support_threshold, -1}} =
               Correlate.start_link(name: nil, support_threshold: -1)
    end

    test "accepts a zero support threshold and positive pool limit" do
      pid = start_correlate!(pool_limit: 1, support_threshold: 0, dispatcher: nil)
      assert Process.alive?(pid)
    end
  end

  describe "candidate production" do
    test "selects other series inside the inclusive window and excludes the target" do
      pid = start_correlate!(window_ms: 600_000, dispatcher: nil, now: fn -> 0 end)

      # Index two neighbours (each correlate call both indexes and returns
      # candidates; we only care about the final target's candidate list).
      _ = Correlate.correlate(pid, anomaly("mem", [{"host", "h1"}], 1_200))
      _ = Correlate.correlate(pid, anomaly("disk", [{"host", "h1"}], 900))

      candidates = Correlate.correlate(pid, anomaly("cpu", [{"host", "h1"}], 1_000))
      series = candidates |> Enum.map(&elem(&1.series_key, 0)) |> Enum.sort()

      assert series == ["disk", "mem"]
      # Ranks are exactly 1..n.
      assert candidates |> Enum.map(& &1.rank) |> Enum.sort() == [1, 2]
    end

    test "an event exactly on the window boundary is included; past it is not" do
      # Inject a fixed clock so pruning (now - 2*window) is deterministic and
      # does not depend on wall time. Events sit within the 2*window retention.
      pid = start_correlate!(window_ms: 1_000, dispatcher: nil, now: fn -> 1_000 end)

      _ = Correlate.correlate(pid, anomaly("edge", [{"host", "h"}], 0))
      _ = Correlate.correlate(pid, anomaly("outside", [{"host", "h"}], 2_001))

      candidates = Correlate.correlate(pid, anomaly("target", [{"host", "h"}], 1_000))
      names = Enum.map(candidates, &elem(&1.series_key, 0))

      # edge is exactly w before the target (inclusive); outside is w+1 after.
      assert "edge" in names
      refute "outside" in names
    end
  end

  describe "support (historical co-occurrence)" do
    test "support excludes the current co-occurrence and grows with earlier ones" do
      pid = start_correlate!(window_ms: 600_000, dispatcher: nil, now: fn -> 0 end)
      labels = [{"host", "h"}]

      # First time cpu & mem co-occur: the support seen for the pair is 0,
      # because the current co-occurrence is only counted after evidence.
      _ = Correlate.correlate(pid, anomaly("mem", labels, 1_000, id: "m1"))

      [%{evidence: %{support: first_support}}] =
        Correlate.correlate(pid, anomaly("cpu", labels, 1_010, id: "c1"))

      assert first_support == 0

      # A later co-occurrence sees strictly more support than the first did,
      # since earlier co-occurrences are now counted.
      _ = Correlate.correlate(pid, anomaly("mem", labels, 2_000, id: "m2"))

      [%{evidence: %{support: second_support}}] =
        Correlate.correlate(pid, anomaly("cpu", labels, 2_010, id: "c2"))

      assert second_support > first_support
    end
  end

  describe "learning gate (Req 35.2) and storage forwarding" do
    test "a non-learning anomaly is forwarded to the Dispatcher" do
      pid = start_correlate!(window_ms: 600_000, dispatcher: self(), storage_writer: nil)
      Correlate.anomaly_started(pid, anomaly("cpu", [{"host", "h"}], 1_000, learning?: false))

      assert_receive {:"$gen_cast", {:event, {:anomaly_started, _anomaly, _candidates}}}
    end

    test "a learning anomaly is NOT forwarded to the Dispatcher but IS sent to storage" do
      pid =
        start_correlate!(
          window_ms: 600_000,
          dispatcher: self(),
          storage_writer: self()
        )

      Correlate.anomaly_started(pid, anomaly("cpu", [{"host", "h"}], 1_000, learning?: true))

      # Storage receives the item in every case (cast {:enqueue, :anomaly, [item]}).
      assert_receive {:"$gen_cast", {:enqueue, :anomaly, [_item]}}
      # The Dispatcher does not receive an anomaly_started event.
      refute_receive {:"$gen_cast", {:event, {:anomaly_started, _, _}}}, 50
    end

    test "resolutions are relayed to the Dispatcher" do
      pid = start_correlate!(dispatcher: self(), storage_writer: nil)
      Correlate.anomaly_resolved(pid, anomaly("cpu", [{"host", "h"}], 1_000))

      assert_receive {:"$gen_cast", {:event, {:anomaly_resolved, _anomaly}}}
    end
  end

  describe "robustness" do
    test "a nil or dead Dispatcher never crashes the Correlator" do
      pid = start_correlate!(dispatcher: nil, storage_writer: nil)
      Correlate.anomaly_started(pid, anomaly("cpu", [{"host", "h"}], 1_000))
      # Still responsive after forwarding to a nil dispatcher.
      assert [] == Correlate.correlate(pid, anomaly("cpu2", [{"host", "h2"}], 5_000_000))
      assert Process.alive?(pid)
    end
  end
end

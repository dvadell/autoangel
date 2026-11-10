defmodule Autoangel.Storage.HeartbeatTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Autoangel.Storage.Heartbeat

  defmodule Backend do
    @moduledoc false
    # init/1 succeeds once `ready` holds true; writes report to the test and
    # fail while `fail` holds true.
    def init(%{agent: agent, test: test}) do
      if Agent.get(agent, & &1.ready),
        do: {:ok, %{agent: agent, test: test}},
        else: {:error, :migrations_failed}
    end

    def write_heartbeat(%{agent: agent, test: test}, ts, version) do
      if Agent.get(agent, & &1.fail) do
        {:retryable, :db_down}
      else
        send(test, {:heartbeat, ts, version})
        :ok
      end
    end

    def prune_heartbeats(%{test: test}, before), do: send(test, {:pruned, before}) && :ok
  end

  defmodule ExitingInit do
    @moduledoc false
    def init(_), do: exit(:connection_gone)
    def write_heartbeat(_, _, _), do: :ok
  end

  defmodule NoHeartbeat do
    @moduledoc false
    def init(_), do: {:ok, nil}
  end

  defp start(agent, opts \\ []) do
    clock = Keyword.get(opts, :now, fn -> 100_000_000 end)

    {:ok, pid} =
      Heartbeat.start_link(
        name: nil,
        backend: Backend,
        backend_config: %{agent: agent, test: self()},
        interval_ms: Keyword.get(opts, :interval_ms, 20),
        version: "9.9.9",
        now: clock
      )

    pid
  end

  defp agent(ready, fail \\ false) do
    {:ok, agent} = Agent.start_link(fn -> %{ready: ready, fail: fail} end)
    agent
  end

  test "writes the first Heartbeat immediately with the time and Engine version" do
    pid = start(agent(true), interval_ms: 60_000)
    assert_receive {:heartbeat, 100_000_000, "9.9.9"}, 200
    assert_receive {:pruned, before}
    assert before == 100_000_000 - 86_400_000
    GenServer.stop(pid)
  end

  test "later Heartbeats follow at the configured interval" do
    pid = start(agent(true), interval_ms: 20)

    for _ <- 1..4, do: assert_receive({:heartbeat, _, _}, 200)
    GenServer.stop(pid)
  end

  test "no Heartbeat is written until migrations have completed" do
    a = agent(false)

    log =
      capture_log(fn ->
        pid = start(a, interval_ms: 20)
        refute_receive {:heartbeat, _, _}, 80
        Agent.update(a, &%{&1 | ready: true})
        assert_receive {:heartbeat, _, _}, 200
        GenServer.stop(pid)
      end)

    assert log =~ "heartbeat not written"
  end

  test "a failed write is logged and retried at the next interval" do
    a = agent(true, true)

    log =
      capture_log(fn ->
        pid = start(a, interval_ms: 20)
        Process.sleep(60)
        refute_received {:heartbeat, _, _}
        Agent.update(a, &%{&1 | fail: false})
        assert_receive {:heartbeat, _, _}, 200
        assert Heartbeat.state(pid).failed >= 1
        GenServer.stop(pid)
      end)

    assert log =~ "heartbeat write failure"
  end

  test "a backend without Heartbeat support starts no Heartbeat process" do
    assert :ignore == Heartbeat.start_link(name: nil, backend: NoHeartbeat)
  end

  test "an exiting backend init is logged and does not crash the Heartbeat" do
    log =
      capture_log(fn ->
        {:ok, pid} = Heartbeat.start_link(name: nil, backend: ExitingInit, interval_ms: 10)
        Process.sleep(40)
        assert Process.alive?(pid)
        assert Heartbeat.state(pid).failed >= 2
        GenServer.stop(pid)
      end)

    assert log =~ "heartbeat not written"
  end
end

defmodule Autoangel.Workers.SweepTimerTest do
  @moduledoc "A Worker keeps at most one sweep timer, and none under simulated time."
  use ExUnit.Case, async: true

  alias Autoangel.Workers.Worker

  defp start(opts) do
    index = System.unique_integer([:positive])

    {:ok, pid} =
      Worker.start_link(
        [
          index: index,
          name: nil,
          queue_counter: :atomics.new(1, signed: true),
          dispatcher: nil,
          correlator: nil
        ] ++ opts
      )

    on_exit(fn ->
      :persistent_term.erase({Worker, index})
      :persistent_term.erase({Worker, :capacity, index})
    end)

    pid
  end

  defp pending_timer(pid) do
    case :sys.get_state(pid).sweep_ref do
      nil -> nil
      ref -> Process.read_timer(ref)
    end
  end

  test "explicit sweeps never stack extra timers" do
    pid = start(sweep_interval: 60_000)
    first = :sys.get_state(pid).sweep_ref
    for _ <- 1..50, do: send(pid, :sweep)
    _ = :sys.get_state(pid)

    # The original timer was cancelled and exactly one fresh timer is pending.
    assert Process.read_timer(first) == false
    assert is_integer(pending_timer(pid))
    {:messages, msgs} = Process.info(pid, :messages)
    assert msgs == []
  end

  test "with sweep_timer: false no timer is ever scheduled" do
    pid = start(sweep_interval: 10, sweep_timer: false)
    send(pid, :sweep)
    Process.sleep(50)
    assert :sys.get_state(pid).sweep_ref == nil
  end
end

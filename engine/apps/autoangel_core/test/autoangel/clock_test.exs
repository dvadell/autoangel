defmodule Autoangel.ClockTest do
  @moduledoc "The Engine clock boundary used by the Replay_Harness simulated clock."
  use ExUnit.Case, async: false

  alias Autoangel.Clock
  alias Autoangel.Clock.Simulated

  setup do
    on_exit(fn -> Application.delete_env(:autoangel_core, :now) end)
  end

  test "now_ms/0 is system time by default and the configured clock when set" do
    before = System.system_time(:millisecond)
    assert Clock.now_ms() >= before

    Application.put_env(:autoangel_core, :now, fn -> 42 end)
    assert Clock.now_ms() == 42
  end

  test "simulated clocks advance independently" do
    {:ok, first} = Simulated.start_link(1_000)
    {:ok, second} = Simulated.start_link(2_000)

    :ok = Simulated.advance(first, 250)
    :ok = Simulated.set(second, 3_500)

    assert Simulated.now_ms(first) == 1_250
    assert Simulated.now_ms(second) == 3_500
  end
end

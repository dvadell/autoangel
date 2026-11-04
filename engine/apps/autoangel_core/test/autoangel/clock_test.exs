defmodule Autoangel.ClockTest do
  use ExUnit.Case, async: true

  test "simulated clocks advance independently" do
    {:ok, first} = Autoangel.Clock.Simulated.start_link(1_000)
    {:ok, second} = Autoangel.Clock.Simulated.start_link(2_000)

    :ok = Autoangel.Clock.Simulated.advance(first, 250)
    :ok = Autoangel.Clock.Simulated.set(second, 3_500)

    assert Autoangel.Clock.Simulated.now_ms(first) == 1_250
    assert Autoangel.Clock.Simulated.now_ms(second) == 3_500
  end
end

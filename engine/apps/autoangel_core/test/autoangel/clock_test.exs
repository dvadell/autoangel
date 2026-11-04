defmodule Autoangel.ClockTest do
  use ExUnit.Case, async: true

  alias Autoangel.Clock.Simulated

  test "simulated clocks advance independently" do
    {:ok, first} = Simulated.start_link(1_000)
    {:ok, second} = Simulated.start_link(2_000)

    :ok = Simulated.advance(first, 250)
    :ok = Simulated.set(second, 3_500)

    assert Simulated.now_ms(first) == 1_250
    assert Simulated.now_ms(second) == 3_500
  end
end

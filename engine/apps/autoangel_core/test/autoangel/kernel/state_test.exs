defmodule Autoangel.Kernel.StateTest do
  use ExUnit.Case, async: true

  alias Autoangel.Kernel.State

  test "encodes and decodes the versioned state" do
    state = %State{
      kind: :counter,
      capacity: 4,
      head: 2,
      window: [1.0, 2.0, 3.0],
      prev: {123, 4.5},
      spacing: [10, 20],
      spacing_head: 1,
      level: %{cusum_pos: 1.0, cusum_neg: 2.0, pending: 3, pending_level: 4.0, pending_first_ts: 5}
    }

    encoded = State.encode(state)
    assert <<"AANG1", 1::little-unsigned-16, 1, _::binary>> = encoded
    assert {:ok, decoded} = State.decode(encoded)
    assert decoded.kind == state.kind
    assert decoded.capacity == state.capacity
    assert decoded.window == state.window
    assert decoded.prev == state.prev
    assert decoded.spacing == state.spacing
    assert decoded.level == state.level
  end

  test "round trips optional seasonal and long-term sections" do
    state = %State{
      capacity: 2,
      window: [1.0, 2.0],
      seasonal: %{period: 24, cells: Enum.map(1..24, fn n -> %{med: n / 1, mad: 0.1, n: n} end)},
      longterm: %{blocks: [%{n: 1, centroids: [{2.0, 3.0}]}]}
    }

    assert {:ok, decoded} = state |> State.encode() |> State.decode()
    assert decoded.seasonal == state.seasonal
    assert decoded.longterm == state.longterm
  end

  test "rejects truncated, corrupt, and invalid state without raising" do
    state = State.encode(%State{})
    <<prefix::binary-size(10), _::binary>> = state
    assert {:error, :bad_state} = State.decode(prefix)
    <<first, rest::binary>> = state
    assert {:error, :bad_state} = State.decode(<<Bitwise.bxor(first, 1), rest::binary>>)
    assert {:error, :bad_state} = State.decode(:not_binary)
  end
end

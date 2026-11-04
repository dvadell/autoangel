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

defmodule Autoangel.Kernel.StateMalformedPropertyTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Autoangel.Kernel.State

  property "Feature: v1, Property 48: malformed state is an error, not a crash" do
    # **Validates: Requirements 47.10**
    valid_state = State.encode(State.new())

    check all(malformed <- malformed_state_generator(valid_state), max_runs: 100) do
      assert {:error, :bad_state} = State.decode(malformed)
      assert Process.alive?(self())
    end
  end

  defp malformed_state_generator(valid_state) do
    size = byte_size(valid_state)

    one_of([
      map(binary(min_length: 0, max_length: 1_024), &<<"not-a-state", &1::binary>>),
      map(integer(0..(size - 1)), &binary_part(valid_state, 0, &1)),
      map(integer(0..(size - 1)), fn index ->
        <<prefix::binary-size(^index), byte, suffix::binary>> = valid_state
        <<prefix::binary, Bitwise.bxor(byte, 1), suffix::binary>>
      end),
      map(filter(integer(0..65_535), &(&1 != 1)), fn version ->
        <<prefix::binary-size(5), _old_version::little-unsigned-16, rest::binary>> = valid_state
        suffix = binary_part(rest, 0, byte_size(rest) - 4)
        body = <<prefix::binary, version::little-unsigned-16, suffix::binary>>
        body <> <<:erlang.crc32(body)::little-unsigned-32>>
      end),
      map(integer(0..7), fn bit ->
        <<body::binary-size(^size - 4), crc::binary-size(4)>> = valid_state

        <<body::binary, Bitwise.bxor(:binary.at(crc, 0), Bitwise.bsl(1, bit)),
          binary_part(crc, 1, 3)::binary>>
      end)
    ])
  end
end

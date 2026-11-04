defmodule Autoangel.Kernel.State do
  import Bitwise

  @moduledoc "Versioned, bounded binary state shared by kernel implementations."

  @magic "AANG1"
  @version 1
  @window_limit 65_535
  @max_window_bytes 4 * 1024 * 1024
  @spacing_capacity 16
  @seasonal_periods [24, 168]
  @max_longterm_blocks 255
  @max_centroids_per_block 65_535

  defstruct kind: :gauge,
            window: [],
            capacity: 256,
            head: 0,
            prev: nil,
            spacing: [],
            spacing_head: 0,
            level: %{
              cusum_pos: 0.0,
              cusum_neg: 0.0,
              pending: 0,
              pending_level: 0.0,
              pending_first_ts: 0
            },
            seasonal: nil,
            longterm: nil

  @type sample :: {integer(), float()}
  @type t :: %__MODULE__{
          kind: :gauge | :counter,
          window: [float()],
          capacity: pos_integer(),
          head: non_neg_integer(),
          prev: sample() | nil,
          spacing: [integer()],
          spacing_head: non_neg_integer(),
          level: map(),
          seasonal: map() | nil,
          longterm: map() | nil
        }

  @spec new(keyword()) :: t()
  def new(opts \\ []) do
    struct(__MODULE__, opts)
  end

  @spec encode(t()) :: binary()
  def encode(%__MODULE__{} = state) do
    body = encode_body(state)
    body <> <<:erlang.crc32(body)::little-unsigned-32>>
  end

  @spec decode(binary()) :: {:ok, t()} | {:error, :bad_state}
  def decode(binary) when is_binary(binary) do
    with true <- byte_size(binary) >= byte_size(@magic) + 2 + 1 + 1 + 6 + 4,
         {body, <<crc::little-unsigned-32>>} <- split_crc(binary),
         true <- :erlang.crc32(body) == crc,
         {:ok, state} <- decode_body(body) do
      {:ok, state}
    else
      _ -> {:error, :bad_state}
    end
  rescue
    _ -> {:error, :bad_state}
  catch
    _, _ -> {:error, :bad_state}
  end

  def decode(_), do: {:error, :bad_state}

  defp encode_body(%__MODULE__{} = s) do
    kind = kind_code(s.kind)

    flags =
      if(s.prev, do: 1, else: 0) |||
        if(s.seasonal, do: 2, else: 0) |||
        if s.longterm, do: 4, else: 0

    window = Enum.take(s.window, s.capacity)
    n = length(window)
    sorted = Enum.sort(window)
    level = s.level
    spacing = Enum.take(s.spacing, @spacing_capacity)

    ring = window ++ List.duplicate(0.0, s.capacity - n)

    @magic <>
      <<@version::little-unsigned-16, kind, flags, s.capacity::little-unsigned-16,
        n::little-unsigned-16, s.head::little-unsigned-16>> <>
      IO.iodata_to_binary(Enum.map(ring, &<<&1::little-float-size(64)>>)) <>
      IO.iodata_to_binary(Enum.map(sorted, &<<&1::little-float-size(64)>>)) <>
      encode_prev(s.prev, flags) <>
      <<length(spacing), s.spacing_head>> <>
      IO.iodata_to_binary(
        Enum.map(
          spacing ++ List.duplicate(0, @spacing_capacity - length(spacing)),
          &<<&1::little-signed-64>>
        )
      ) <>
      <<level.cusum_pos::little-float-size(64), level.cusum_neg::little-float-size(64),
        level.pending::little-unsigned-16, level.pending_level::little-float-size(64),
        level.pending_first_ts::little-signed-64>> <>
      encode_seasonal(s.seasonal, flags) <> encode_longterm(s.longterm, flags)
  end

  defp encode_prev(nil, _), do: <<>>
  defp encode_prev({ts, value}, _), do: <<ts::little-signed-64, value::little-float-size(64)>>

  defp encode_seasonal(nil, _), do: <<>>

  defp encode_seasonal(%{period: period, cells: cells}, _),
    do:
      <<period>> <>
        IO.iodata_to_binary(
          Enum.map(cells, fn c ->
            <<c.med::little-float-size(64), c.mad::little-float-size(64), c.n::little-unsigned-32>>
          end)
        )

  defp encode_longterm(nil, _), do: <<>>

  defp encode_longterm(%{blocks: blocks}, _),
    do:
      <<length(blocks)>> <>
        IO.iodata_to_binary(
          Enum.map(blocks, fn %{centroids: centroids} ->
            n = length(centroids)

            <<n::little-unsigned-16>> <>
              IO.iodata_to_binary(
                Enum.map(centroids, fn {mean, weight} ->
                  <<mean::little-float-size(64), weight::little-float-size(64)>>
                end)
              )
          end)
        )

  defp decode_body(body) do
    with {:ok,
          <<@magic, version::little-unsigned-16, kind, flags, capacity::little-unsigned-16,
            n::little-unsigned-16, head::little-unsigned-16>>, rest} <-
           take(body, 5 + 2 + 1 + 1 + 2 + 2 + 2),
         true <- version == @version and kind in [0, 1] and (flags &&& 0xF8) == 0,
         true <-
           capacity > 0 and capacity <= @window_limit and capacity <= div(@max_window_bytes, 8),
         true <- n <= capacity and head < capacity,
         {:ok, window, rest} <- take_floats(rest, capacity),
         {:ok, sorted, rest} <- take_floats(rest, n),
         true <- sorted == Enum.sort(Enum.take(window, n)),
         {:ok, prev, rest} <- take_prev(rest, flags),
         {:ok, <<spacing_n, spacing_head>>, rest} <- take(rest, 2),
         true <- spacing_n <= @spacing_capacity and spacing_head < @spacing_capacity,
         {:ok, spacing_slots, rest} <- take_ints(rest, @spacing_capacity),
         {:ok, level, rest} <- take_level(rest),
         {:ok, seasonal, rest} <- take_seasonal(rest, flags),
         {:ok, longterm, rest} <- take_longterm(rest, flags),
         <<>> <- rest do
      {:ok,
       %__MODULE__{
         kind: if(kind == 1, do: :counter, else: :gauge),
         capacity: capacity,
         head: head,
         window: Enum.take(window, n),
         prev: prev,
         spacing: Enum.take(spacing_slots, spacing_n),
         spacing_head: spacing_head,
         level: level,
         seasonal: seasonal,
         longterm: longterm
       }}
    else
      _ -> {:error, :bad_state}
    end
  end

  defp split_crc(binary),
    do:
      {binary_part(binary, 0, byte_size(binary) - 4), binary_part(binary, byte_size(binary) - 4, 4)}

  defp take(binary, n) when byte_size(binary) >= n,
    do: {:ok, binary_part(binary, 0, n), binary_part(binary, n, byte_size(binary) - n)}

  defp take(_, _), do: :error

  defp take_floats(binary, count), do: take_floats(binary, count, [])
  defp take_floats(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp take_floats(<<value::little-float-size(64), rest::binary>>, count, acc),
    do: take_floats(rest, count - 1, [value | acc])

  defp take_floats(_, _, _), do: :error

  defp take_ints(binary, count), do: take_ints(binary, count, [])
  defp take_ints(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp take_ints(<<value::little-signed-64, rest::binary>>, count, acc),
    do: take_ints(rest, count - 1, [value | acc])

  defp take_ints(_, _, _), do: :error

  defp take_prev(rest, flags) when (flags &&& 1) == 0, do: {:ok, nil, rest}

  defp take_prev(<<ts::little-signed-64, value::little-float-size(64), rest::binary>>, _),
    do: {:ok, {ts, value}, rest}

  defp take_prev(_, _), do: :error

  defp take_level(
         <<pos::little-float-size(64), neg::little-float-size(64), pending::little-unsigned-16,
           level::little-float-size(64), ts::little-signed-64, rest::binary>>
       ),
       do:
         {:ok,
          %{
            cusum_pos: pos,
            cusum_neg: neg,
            pending: pending,
            pending_level: level,
            pending_first_ts: ts
          }, rest}

  defp take_level(_), do: :error

  defp take_seasonal(rest, flags) when (flags &&& 2) == 0, do: {:ok, nil, rest}

  defp take_seasonal(<<period, rest::binary>>, _)
       when period in @seasonal_periods do
    case take_cells(rest, period, []) do
      {:ok, cells, rest2} -> {:ok, %{period: period, cells: cells}, rest2}
      _ -> :error
    end
  end

  defp take_seasonal(_, _), do: :error

  defp take_cells(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp take_cells(
         <<med::little-float-size(64), mad::little-float-size(64), n::little-unsigned-32,
           rest::binary>>,
         count,
         acc
       ),
       do: take_cells(rest, count - 1, [%{med: med, mad: mad, n: n} | acc])

  defp take_cells(_, _, _), do: :error

  defp take_longterm(rest, flags) when (flags &&& 4) == 0, do: {:ok, nil, rest}

  defp take_longterm(<<block_count, rest::binary>>, _) when block_count <= @max_longterm_blocks do
    take_blocks(rest, block_count, [])
    |> case do
      {:ok, blocks, rest2} -> {:ok, %{blocks: blocks}, rest2}
      _ -> :error
    end
  end

  defp take_longterm(_, _), do: :error

  defp take_blocks(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp take_blocks(
         <<count::little-unsigned-16, rest::binary>>,
         blocks,
         acc
       )
       when count <= @max_centroids_per_block do
    case take_centroids(rest, count, []) do
      {:ok, centroids, rest2} ->
        take_blocks(rest2, blocks - 1, [%{n: count, centroids: centroids} | acc])

      _ ->
        :error
    end
  end

  defp take_blocks(_, _, _), do: :error

  defp take_centroids(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp take_centroids(
         <<mean::little-float-size(64), weight::little-float-size(64), rest::binary>>,
         count,
         acc
       ),
       do: take_centroids(rest, count - 1, [{mean, weight} | acc])

  defp take_centroids(_, _, _), do: :error

  defp kind_code(:gauge), do: 0
  defp kind_code(:counter), do: 1
end

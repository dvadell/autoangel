defmodule AutoangelTsdb do
  @moduledoc """
  Small TimescaleDB write prototype used by the M0 storage gate.

  The prototype deliberately owns no schema or migrations. It assumes the eventual
  `samples(series_id, ts, value)` table and can be replaced by the production
  storage writer in M1.
  """

  @copy_header <<"PGCOPY\n", 0xFF, "\r\n\0", 0::signed-big-32, 0::signed-big-32>>
  @copy_trailer <<-1::signed-big-16>>
  @pg_epoch_us 946_684_800_000_000

  @type row :: {binary(), integer(), float()}
  @type write_result :: %{
          method: :copy | :insert,
          round_trips: pos_integer(),
          rows: non_neg_integer()
        }

  # This gate reports one client write round trip per Postgrex operation, not wire-level exchanges.
  @doc "Encodes rows using PostgreSQL's binary COPY format."
  @spec copy_binary([row()]) :: binary()
  def copy_binary(rows) do
    body = Enum.map(rows, &encode_row/1)
    IO.iodata_to_binary([@copy_header, body, @copy_trailer])
  end

  @doc "Writes one batch with COPY, falling back to a parameterised multi-row INSERT."
  @spec write_batch(Postgrex.conn(), [row()], keyword()) :: {:ok, write_result()} | {:error, term()}
  def write_batch(conn, rows, opts \\ []) when is_list(rows) do
    if Keyword.get(opts, :force_fallback, false) do
      insert_write(conn, rows)
    else
      copy_or_fallback(conn, rows, opts)
    end
  end

  defp copy_or_fallback(conn, rows, opts) do
    copy_sql =
      Keyword.get(opts, :copy_sql, "COPY samples (series_id, ts, value) FROM STDIN BINARY")

    payload = copy_binary(rows)

    case copy_write(conn, copy_sql, payload) do
      :ok ->
        {:ok, %{method: :copy, round_trips: 1, rows: length(rows)}}

      {:error, copy_reason} ->
        case insert_write(conn, rows) do
          {:ok, result} -> {:ok, Map.put(result, :copy_fallback_reason, copy_reason)}
          error -> error
        end
    end
  end

  @doc "Generates deterministic benchmark rows with intervals between 10 and 60 seconds."
  @spec benchmark_rows(pos_integer(), pos_integer()) :: [row()]
  def benchmark_rows(series_count, samples_per_series) do
    base_ts = DateTime.utc_now() |> DateTime.to_unix(:microsecond) |> Kernel.-(@pg_epoch_us)

    for series <- 1..series_count,
        sample <- 1..samples_per_series do
      interval = 10 + rem(series * 17, 51)
      ts = base_ts + sample * interval * 1_000_000
      {<<series::unsigned-big-128>>, ts, series * 1.0 + sample / 10}
    end
  end

  defp copy_write(conn, sql, payload) do
    Postgrex.transaction(conn, fn tx ->
      stream = Postgrex.stream(tx, sql, [])
      Enum.into([payload], stream)
    end)
    |> case do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, reason}
    end
  rescue
    error -> {:error, error}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp insert_write(conn, rows) do
    {values, params} =
      rows
      |> Enum.with_index(1)
      |> Enum.map_reduce([], fn {{series_id, ts, value}, index}, params ->
        date_time = DateTime.from_unix!(ts + @pg_epoch_us, :microsecond)

        {"($#{index * 3 - 2}, $#{index * 3 - 1}, $#{index * 3})",
         [series_id, date_time, value | params]}
      end)

    sql = "INSERT INTO samples (series_id, ts, value) VALUES #{Enum.join(values, ", ")}"

    case Postgrex.query(conn, sql, Enum.reverse(params)) do
      {:ok, _result} -> {:ok, %{method: :insert, round_trips: 1, rows: length(rows)}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp encode_row({series_id, ts, value}) when byte_size(series_id) == 16 do
    fields = [
      <<16::signed-big-32, series_id::binary>>,
      <<8::signed-big-32, ts::signed-big-64>>,
      <<8::signed-big-32, value::float-big>>
    ]

    [<<3::signed-big-16>>, fields]
  end
end

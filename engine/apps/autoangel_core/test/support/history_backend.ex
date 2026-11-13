defmodule Autoangel.Test.HistoryBackend do
  @moduledoc """
  A storage backend double for History tests. `init/1` returns the test pid as the
  handle; every read is reported to it as `{:read, kind, ids, from, to}` and
  answered with the rows kept by `put_rows/1`.
  """

  def put_rows(rows), do: :persistent_term.put({__MODULE__, :rows}, rows)
  def clear, do: :persistent_term.erase({__MODULE__, :rows})
  defp rows, do: :persistent_term.get({__MODULE__, :rows}, [])

  def init(%{report_to: pid}), do: {:ok, pid}
  def init(%{fail: reason}), do: {:error, reason}

  def read_range(pid, ids, from, to) do
    send(pid, {:read, :raw, ids, from, to})

    case rows() do
      {:error, reason} ->
        {:retryable, reason}

      {:sleep, ms, rows} ->
        Process.sleep(ms)
        {:ok, Enum.filter(rows, fn {id, ts, _} -> id in ids and ts >= from and ts < to end)}

      rows ->
        {:ok, Enum.filter(rows, fn {id, ts, _} -> id in ids and ts >= from and ts < to end)}
    end
  end

  def read_aggregates(pid, ids, from, to, resolution) do
    send(pid, {:read, resolution, ids, from, to})

    case rows() do
      {:error, reason} ->
        {:retryable, reason}

      rows ->
        {:ok,
         Enum.filter(rows, fn row ->
           elem(row, 0) in ids and elem(row, 1) >= from and elem(row, 1) < to
         end)}
    end
  end

  def read_anomaly_events(pid, from, to) do
    send(pid, {:read, :anomaly_events, [], from, to})
    {:ok, rows()}
  end
end

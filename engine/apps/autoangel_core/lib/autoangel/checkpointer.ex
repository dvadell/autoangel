defmodule Autoangel.Checkpointer do
  @moduledoc "Periodically persists Worker snapshots without pausing more than one Worker at a time."

  alias Autoangel.Workers.Worker
  require Logger
  use GenServer

  @default_interval_ms 60_000

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @spec checkpoint_now(GenServer.server()) :: :ok | {:error, term()}
  def checkpoint_now(server \\ __MODULE__), do: GenServer.call(server, :checkpoint_now, :infinity)

  @spec workers(GenServer.server()) :: [GenServer.server()]
  def workers(server \\ __MODULE__), do: GenServer.call(server, :workers)

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :interval_ms, @default_interval_ms)
    workers = Keyword.get(opts, :workers, [])
    workers_supervisor = Keyword.get(opts, :workers_supervisor)
    state_store = Keyword.fetch!(opts, :state_store)
    Process.send_after(self(), :checkpoint, interval)

    {:ok,
     %{
       interval_ms: interval,
       workers: workers,
       workers_supervisor: workers_supervisor,
       state_store: state_store
     }}
  end

  @impl true
  def handle_call(:workers, _from, state), do: {:reply, state.workers, state}

  def handle_call(:checkpoint_now, _from, state) do
    result = write_checkpoint(state)
    {:reply, result, state}
  end

  @impl true
  def handle_info(:checkpoint, state) do
    _ = write_checkpoint(state)
    Process.send_after(self(), :checkpoint, state.interval_ms)
    {:noreply, state}
  end

  defp write_checkpoint(state) do
    with {:ok, workers} <- current_workers(state),
         {:ok, entries} <- snapshots(workers),
         :ok <- Autoangel.StateStore.checkpoint(state.state_store, entries) do
      :ok
    else
      {:error, reason} = error ->
        Logger.error("checkpoint write failure: #{inspect(reason)}")
        error
    end
  catch
    :exit, reason ->
      Logger.error("checkpoint write failure: #{inspect(reason)}")
      {:error, reason}
  end

  defp current_workers(%{workers_supervisor: nil, workers: workers}), do: {:ok, workers}

  defp current_workers(%{workers_supervisor: supervisor}) do
    children = Supervisor.which_children(supervisor)

    if Enum.any?(children, fn {_id, pid, _type, _modules} -> not is_pid(pid) end) do
      {:error, :worker_restarting}
    else
      {:ok, Enum.map(children, fn {_id, pid, _type, _modules} -> pid end)}
    end
  catch
    :exit, reason -> {:error, reason}
  end

  defp snapshots(workers) do
    Enum.reduce_while(workers, {:ok, []}, fn worker, {:ok, entries} ->
      try do
        case Worker.snapshot(worker) do
          {:ok, worker_entries} ->
            {:cont, {:ok, entries ++ worker_entries}}

          {:error, reason} ->
            Logger.error(
              "checkpoint snapshot failure for worker #{inspect(worker)}: #{inspect(reason)}"
            )

            {:halt, {:error, reason}}
        end
      catch
        :exit, reason ->
          Logger.error(
            "checkpoint snapshot failure for worker #{inspect(worker)}: #{inspect(reason)}"
          )

          {:halt, {:error, reason}}
      end
    end)
  end
end

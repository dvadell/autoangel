defmodule Autoangel.Storage.Migrator do
  @moduledoc "Retries the configured backend migration until the schema is ready."
  use GenServer
  require Logger

  @retry_interval 60_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))

  @impl true
  def init(opts) do
    state = %{
      backend: Keyword.fetch!(opts, :backend),
      config: Keyword.get(opts, :config, %{}),
      writer: Keyword.get(opts, :writer),
      interval: Keyword.get(opts, :retry_interval, @retry_interval),
      ready?: false,
      handle: nil,
      retention: nil
    }

    send(self(), :migrate)
    {:ok, state}
  end

  @impl true
  def handle_info(:migrate, state) do
    case state.backend.init(state.config) do
      {:ok, handle} ->
        {:ok, retention} =
          AutoangelTsdb.Retention.start_link({Postgrex, handle.conn}, state.config)

        notify(state.writer, :migration_succeeded)
        {:noreply, %{state | handle: handle, retention: retention, ready?: true}}

      {:error, reason} ->
        Logger.error("storage migration failed: #{inspect(reason)}")
        notify(state.writer, :migration_failed)
        Process.send_after(self(), :migrate, state.interval)
        {:noreply, state}
    end
  end

  @doc "Returns whether the last migration attempt succeeded."
  def ready?(server), do: GenServer.call(server, :ready?)

  @impl true
  def handle_call(:ready?, _from, state), do: {:reply, state.ready?, state}

  defp notify(pid, message) when is_pid(pid), do: send(pid, message)
  defp notify(_, _), do: :ok
end

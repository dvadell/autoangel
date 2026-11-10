defmodule Autoangel.Storage.Heartbeat do
  @moduledoc """
  Writes the Engine Heartbeat to the storage backend (Requirement 41.2 to 41.4).

  Runs under `Autoangel.Storage.Supervisor` after the Storage_Writer, with its
  own backend handle, so a slow or failing Heartbeat write never blocks the
  Writer, ingestion, detection or alerting. The handle comes from `backend.init/1`,
  which only succeeds once every migration has been applied; until then no
  Heartbeat is written (Requirement 41.3).

  The first Heartbeat is written as soon as the handle is ready. Each later one
  is scheduled one interval after the previous attempt started, so consecutive
  Heartbeats are never more than one interval apart. A failure is logged and the
  next attempt happens at the next interval. Rows older than one day are pruned.
  """

  use GenServer

  require Logger

  @default_interval_ms 15_000
  @retention_ms 24 * 60 * 60 * 1_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @doc false
  @spec state(GenServer.server()) :: map()
  def state(server \\ __MODULE__), do: GenServer.call(server, :state)

  @impl true
  def init(opts) do
    backend = Keyword.fetch!(opts, :backend)

    if Code.ensure_loaded?(backend) and function_exported?(backend, :write_heartbeat, 3),
      do: init_state(backend, opts),
      else: :ignore
  end

  defp init_state(backend, opts) do
    state = %{
      backend: backend,
      config: Keyword.get(opts, :backend_config, %{}),
      handle: Keyword.get(opts, :handle),
      interval_ms: positive(Keyword.get(opts, :interval_ms), @default_interval_ms),
      version: Keyword.get(opts, :version, engine_version()),
      now: Keyword.get(opts, :now, fn -> System.system_time(:millisecond) end),
      written: 0,
      failed: 0
    }

    send(self(), :beat)
    {:ok, state}
  end

  @impl true
  def handle_call(:state, _from, state), do: {:reply, state, state}

  @impl true
  def handle_info(:beat, state) do
    Process.send_after(self(), :beat, state.interval_ms)
    {:noreply, beat(state)}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp beat(state) do
    case ensure_handle(state) do
      {:ok, state} -> write(state)
      {:error, state} -> state
    end
  end

  defp ensure_handle(%{handle: nil} = state) do
    case state.backend.init(state.config) do
      {:ok, handle} ->
        {:ok, %{state | handle: handle}}

      {:error, reason} ->
        Logger.error("heartbeat not written: storage not ready (#{inspect(reason)})")
        {:error, %{state | failed: state.failed + 1}}
    end
  rescue
    exception ->
      Logger.error("heartbeat not written: storage not ready (#{Exception.message(exception)})")
      {:error, %{state | failed: state.failed + 1}}
  catch
    kind, reason ->
      Logger.error("heartbeat not written: storage not ready (#{inspect({kind, reason})})")
      {:error, %{state | failed: state.failed + 1}}
  end

  defp ensure_handle(state), do: {:ok, state}

  defp write(state) do
    now = state.now.()

    case call(state, :write_heartbeat, [state.handle, now, state.version]) do
      :ok ->
        prune(state, now)
        %{state | written: state.written + 1}

      other ->
        Logger.error("heartbeat write failure: #{inspect(other)}")
        %{state | failed: state.failed + 1}
    end
  end

  defp prune(state, now) do
    case call(state, :prune_heartbeats, [state.handle, now - @retention_ms]) do
      :ok -> :ok
      other -> Logger.error("heartbeat prune failure: #{inspect(other)}")
    end
  end

  defp call(state, fun, args) do
    if function_exported?(state.backend, fun, length(args)),
      do: apply(state.backend, fun, args),
      else: {:permanent, {:unsupported, fun}}
  rescue
    exception -> {:retryable, {:exception, Exception.message(exception)}}
  catch
    kind, reason -> {:retryable, {kind, reason}}
  end

  defp engine_version do
    case Application.spec(:autoangel_core, :vsn) do
      nil -> "unknown"
      vsn -> to_string(vsn)
    end
  end

  defp positive(value, _default) when is_integer(value) and value > 0, do: value
  defp positive(_value, default), do: default
end

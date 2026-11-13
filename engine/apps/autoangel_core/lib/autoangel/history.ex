defmodule Autoangel.History do
  @moduledoc """
  Bounded reads of stored history for seasonality, warm start and correlation
  seeding (Requirements 37.1 to 37.6).

  Reads never run inside a Worker. `async/3` runs a read under
  `Autoangel.Background.Tasks`, and a failure is logged as a warning that names
  the analysis and the Series. With no storage backend (Topology_A) no read is
  ever issued: `read/4` answers `{:error, :no_storage}` without touching a backend.

  Every read is clamped before it reaches the backend: the range to
  `max_range_ms` (default 15 days) ending at `to`, and the Series to
  `max_series` (default 200).

  The backend handle comes from the `Autoangel.History.Server` (started with the
  storage configuration, connecting on first use), or from the `:handle` and
  `:backend` options (tests).
  """

  require Logger

  alias Autoangel.History.Server

  @default_range_ms 15 * 86_400_000
  @default_series 200

  @type row :: {binary(), integer(), number()}

  @doc "The default clamps: `{range_ms, series_count}`."
  @spec defaults() :: {1_296_000_000, 200}
  def defaults, do: {@default_range_ms, @default_series}

  @doc """
  Clamps a request. Returns the Series ids and `{from, to}` that will be read.
  The range keeps its end: an older start is moved forward.
  """
  @spec clamp([binary()], integer(), integer(), keyword()) ::
          {[binary()], {integer(), integer()}}
  def clamp(series_ids, from, to, opts \\ []) do
    max_range = Keyword.get(opts, :max_range_ms, @default_range_ms)
    max_series = Keyword.get(opts, :max_series, @default_series)
    {Enum.take(series_ids, max_series), {max(from, to - max_range), to}}
  end

  @doc """
  Reads raw Samples `{series_id, ts_ms, value}` of `series_ids` in `[from, to)`
  (milliseconds), after clamping. `opts`: `:backend`, `:handle`, `:server`,
  `:max_range_ms`, `:max_series`, `:resolution` (`:raw`, `:minute` or `:hour`).
  """
  @spec read([binary()], integer(), integer(), keyword()) ::
          {:ok, [tuple()]} | {:error, term()}
  def read(series_ids, from, to, opts \\ []) do
    with {:ok, backend, handle} <- connection(opts) do
      {ids, {from, to}} = clamp(series_ids, from, to, opts)

      if ids == [] do
        {:ok, []}
      else
        do_read(backend, handle, ids, from, to, Keyword.get(opts, :resolution, :raw))
      end
    end
  end

  defp do_read(backend, handle, ids, from, to, :raw),
    do: outcome(backend.read_range(handle, ids, from, to))

  defp do_read(backend, handle, ids, from, to, resolution),
    do: outcome(backend.read_aggregates(handle, ids, from, to, resolution))

  defp outcome({:ok, rows}), do: {:ok, rows}
  defp outcome({:retryable, reason}), do: {:error, reason}
  defp outcome({:permanent, reason}), do: {:error, reason}

  defp connection(opts) do
    case {Keyword.get(opts, :backend), Keyword.get(opts, :handle)} do
      {backend, handle} when is_atom(backend) and not is_nil(backend) and not is_nil(handle) ->
        {:ok, backend, handle}

      _ ->
        Server.connection(Keyword.get(opts, :server, Server))
    end
  end

  @doc """
  Runs `fun` as a background Task under `Autoangel.Background.Tasks`, never in the
  caller. A failure (an error return or a crash) logs a warning that names the
  `analysis` and the Series, and returns `{:error, reason}` to `reply_to`.
  """
  @spec async(String.t(), [term()], (-> term()), keyword()) :: {:ok, pid()} | {:error, term()}
  def async(analysis, series, fun, opts \\ []) do
    supervisor = Keyword.get(opts, :supervisor, Autoangel.Background.Tasks)
    reply_to = Keyword.get(opts, :reply_to)

    Task.Supervisor.start_child(supervisor, fn ->
      result = run_logged(analysis, series, fun)
      if reply_to, do: send(reply_to, {:history_result, analysis, series, result})
    end)
  catch
    :exit, reason -> {:error, reason}
  end

  defp run_logged(analysis, series, fun) do
    case fun.() do
      {:error, reason} = error ->
        warn(analysis, series, reason)
        error

      other ->
        other
    end
  rescue
    exception ->
      warn(analysis, series, Exception.message(exception))
      {:error, exception}
  catch
    kind, reason ->
      warn(analysis, series, {kind, reason})
      {:error, reason}
  end

  defp warn(analysis, series, reason) do
    Logger.warning(
      "history #{analysis} failed for series #{inspect(Enum.take(series, 5))}: #{inspect(reason)}"
    )
  end

  # --- pair seeding -----------------------------------------------------------

  @doc """
  Every pair of Series that had an Anomaly start within `window_ms` of each
  other, as `{{min_key, max_key}, count}`. `events` is a list of
  `{series_key, start_ms}`.
  """
  @spec cooccurring_pairs([{term(), integer()}], pos_integer()) :: [
          {{term(), term()}, pos_integer()}
        ]
  def cooccurring_pairs(events, window_ms) do
    sorted = Enum.sort_by(events, &elem(&1, 1))

    sorted
    |> pair_up(window_ms, [])
    |> Enum.frequencies()
    |> Map.to_list()
  end

  defp pair_up([], _window, acc), do: acc

  defp pair_up([{key, ts} | rest], window, acc) do
    near = rest |> Enum.take_while(fn {_k, t} -> t - ts <= window end)

    pairs =
      for {other, _} <- near, other != key do
        if key <= other, do: {key, other}, else: {other, key}
      end

    pair_up(rest, window, pairs ++ acc)
  end

  @doc """
  Seeds the Correlator's pair table from Anomalies stored in the last 15 days.
  Runs as a background Task; does nothing without a storage backend.
  """
  @spec seed_pairs(keyword()) :: {:ok, pid()} | {:error, term()}
  def seed_pairs(opts) do
    correlator = Keyword.get(opts, :correlator, Autoangel.Correlate)
    window_ms = Keyword.fetch!(opts, :window_ms)
    now = Keyword.get(opts, :now, System.system_time(:millisecond))
    {range, _} = defaults()

    async("pair seeding", [], fn ->
      with {:ok, backend, handle} <- connection(opts),
           true <- function_exported?(backend, :read_anomaly_events, 3) || {:error, :unsupported},
           {:ok, events} <- outcome(backend.read_anomaly_events(handle, now - range, now)) do
        pairs = cooccurring_pairs(events, window_ms)
        Autoangel.Correlate.seed_pairs(correlator, pairs)
      end
    end)
  end
end

defmodule Autoangel.History.Server do
  @moduledoc """
  Holds the storage handle History reads use. It connects on the first read, so
  the Engine starts without it, and returns `{:error, :no_storage}` when no
  backend is configured (Topology_A).
  """
  use GenServer

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @doc "The backend module and handle, connecting if needed."
  @spec connection(GenServer.server()) :: {:ok, module(), term()} | {:error, term()}
  def connection(server) do
    GenServer.call(server, :connection, 10_000)
  catch
    :exit, _ -> {:error, :no_storage}
  end

  @impl true
  def init(opts) do
    {:ok,
     %{
       backend: Keyword.get(opts, :backend),
       config: Keyword.get(opts, :backend_config, %{}),
       handle: nil
     }}
  end

  @impl true
  def handle_call(:connection, _from, %{backend: backend} = state) when backend in [nil, :none],
    do: {:reply, {:error, :no_storage}, state}

  def handle_call(:connection, _from, %{handle: nil} = state) do
    case state.backend.init(state.config) do
      {:ok, handle} -> {:reply, {:ok, state.backend, handle}, %{state | handle: handle}}
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  def handle_call(:connection, _from, state),
    do: {:reply, {:ok, state.backend, state.handle}, state}
end

defmodule Autoangel.Application do
  @moduledoc "Boots the Engine in dependency order and checkpoints on shutdown."
  use Application
  require Logger

  @impl true
  # credo:disable-for-next-line Credo.Check.Refactor.CyclomaticComplexity
  def start(_type, _args) do
    explicit_config? = is_map(Application.get_env(:autoangel_core, :config))
    config = load_config()
    Application.put_env(:autoangel_core, :config, config, persistent: true)
    Autoangel.Metrics.ensure_started()

    pool_size = get_in(config, [:workers, :pool_size]) || System.schedulers_online()

    state_store_path =
      get_in(config, [:state_store, :path]) || System.get_env("AUTOANGEL_STATE_PATH") ||
        "data/state"

    storage = configured_storage(config)
    # Only a configured backend has a Storage_Writer (none in Topology_A).
    storage_writer = if storage == :none, do: nil, else: Autoangel.Storage.Writer
    plugins = Map.get(config, :enabled_plugins, [])
    common = [config: config, pool_size: pool_size]

    children =
      if test_without_explicit_config?(explicit_config?) do
        []
      else
        [
          %{id: Autoangel.Boot, start: {Autoangel.Boot, :start_link, [[]]}, type: :worker},
          Supervisor.child_spec(
            {Autoangel.HTTP.Listener,
             Keyword.merge(common, role: :control_api, name: Autoangel.HTTP.ControlListener)},
            id: :control_listener
          ),
          {Autoangel.StateStore, path: state_store_path},
          {Autoangel.SeriesRegistry,
           per_metric_limit: get_in(config, [:cardinality, :per_metric]) || 2_000,
           total_limit: get_in(config, [:cardinality, :total]) || 50_000,
           pool_size: pool_size},
          {Autoangel.Workers,
           Keyword.merge(common,
             state_store: Autoangel.StateStore,
             sweep_interval: seconds(config, [:missing_data, :sweep_interval], 15),
             scrape_status_table: :autoangel_scrape_status,
             queue_capacity: 20_000,
             max_batch: 2,
             dispatcher: Autoangel.Alert.Dispatcher,
             storage_writer: storage_writer
           )},
          {Autoangel.Correlator.Supervisor,
           window_ms: seconds(config, [:correlation, :window], 600),
           pool_limit: get_in(config, [:correlation, :candidate_pool_limit]) || 20,
           support_threshold: get_in(config, [:correlation, :support_threshold]) || 2,
           storage_writer: storage_writer},
          {Autoangel.Alert.Supervisor,
           plugins: plugins,
           state_store: Autoangel.StateStore,
           console_base_url: get_in(config, [:alerts, :console_base_url]),
           max_candidates: get_in(config, [:alerts, :max_candidates]) || 5},
          {Autoangel.Feedback,
           state_store: Autoangel.StateStore, base_sensitivity: get_in(config, [:sensitivity])},
          storage_child(storage, config),
          scrape_child(config),
          {Autoangel.SelfMonitor,
           storage_configured?: storage != :none, plugins: plugins, config: config},
          {Autoangel.Background.Supervisor, []},
          {Autoangel.Checkpointer,
           state_store: Autoangel.StateStore,
           workers_supervisor: Autoangel.Workers,
           interval_ms: seconds(config, [:checkpoint_interval], 60),
           storage_writer: storage_writer},
          Supervisor.child_spec(
            {Autoangel.HTTP.Listener,
             Keyword.merge(common, role: :ingest, name: Autoangel.HTTP.IngestListener)},
            id: :ingest_listener
          )
        ]
        |> Enum.reject(&is_nil/1)
      end

    if storage == :none do
      Logger.info(
        "Topology A: DET-5 seasonality, COR-7 lagged correlation and STE-4 warm start unavailable because no storage backend is configured"
      )
    end

    Supervisor.start_link(children, strategy: :one_for_one, name: Autoangel.Supervisor)
    |> case do
      {:ok, _pid} = result ->
        Autoangel.Boot.ready()
        result

      error ->
        error
    end
  end

  @impl true
  def prep_stop(state) do
    case Process.whereis(Autoangel.Checkpointer) do
      nil ->
        state

      checkpointer ->
        try do
          case Autoangel.Checkpointer.checkpoint_now(checkpointer) do
            :ok ->
              state

            {:error, reason} ->
              Logger.error("final checkpoint write failure: #{inspect(reason)}")
              state
          end
        catch
          :exit, reason ->
            Logger.error("final checkpoint unavailable: #{inspect(reason)}")
            state
        end
    end
  end

  defp test_without_explicit_config?(explicit_config?) do
    Code.ensure_loaded?(Mix) and Mix.env() == :test and not explicit_config?
  end

  defp load_config do
    case Application.get_env(:autoangel_core, :config) do
      config when is_map(config) ->
        config

      source ->
        source = if is_binary(source), do: source, else: "\n"
        {:ok, config} = Autoangel.Config.load(source)
        config
    end
  end

  defp configured_storage(config) do
    case Autoangel.Storage.select_backend(config) do
      {:ok, :none} -> :none
      {:ok, backend} -> backend
      {:error, reason} -> exit({:shutdown, {:invalid_storage_configuration, reason}})
    end
  end

  defp storage_child(:none, _config), do: nil

  defp storage_child(backend, config) do
    {Autoangel.Storage.Supervisor,
     backend: backend,
     backend_config: Map.get(config, :storage, %{}),
     heartbeat_interval_ms: seconds(config, [:heartbeat_interval], 15),
     self_monitor: Autoangel.SelfMonitor}
  end

  defp scrape_child(config) do
    targets = Map.get(config, :scrape_targets, [])

    if targets == [],
      do: nil,
      else:
        {Autoangel.Scrape, targets: targets, processor_opts: [self_monitor: Autoangel.SelfMonitor]}
  end

  defp seconds(config, path, default) do
    case get_in(config, path) do
      value when is_number(value) -> round(value * 1_000)
      _ -> default * 1_000
    end
  end
end

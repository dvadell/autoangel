defmodule Autoangel.Correlator.Supervisor do
  @moduledoc "Supervision boundary for the in-memory Correlator."
  use Supervisor

  def start_link(opts \\ []),
    do: Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @impl true
  def init(opts) do
    correlate_opts =
      opts
      |> Keyword.take([
        :window_ms,
        :pool_limit,
        :support_threshold,
        :pair_cap,
        :dispatcher,
        :storage_writer
      ])
      |> Keyword.put(:name, Autoangel.Correlate)

    Supervisor.init([{Autoangel.Correlate, correlate_opts}], strategy: :one_for_one)
  end
end

defmodule Autoangel.Alert.Supervisor do
  @moduledoc "Supervises alert delivery and its plugin runners."
  use Supervisor

  def start_link(opts \\ []),
    do: Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @impl true
  def init(opts) do
    plugins = Keyword.get(opts, :plugins, [])

    Supervisor.init(
      [
        {Autoangel.Alert.PluginSupervisor,
         Keyword.put(opts, :name, Autoangel.Alert.PluginSupervisor)},
        {Autoangel.Alert.Dispatcher,
         [
           state_store: Keyword.get(opts, :state_store, Autoangel.StateStore),
           plugins: plugins
         ] ++ Keyword.take(opts, [:console_base_url, :max_candidates])}
      ],
      strategy: :one_for_one
    )
  end
end

defmodule Autoangel.Background.Supervisor do
  @moduledoc "Supervision boundary for optional history and seasonality work."
  use Supervisor

  def start_link(opts \\ []),
    do: Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @impl true
  def init(_opts), do: Supervisor.init([], strategy: :one_for_one)
end

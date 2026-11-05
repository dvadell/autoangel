defmodule Autoangel.Correlator.Supervisor do
  @moduledoc "Placeholder supervision boundary for the in-memory correlator."
  use Supervisor

  def start_link(opts \\ []),
    do: Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @impl true
  def init(_opts), do: Supervisor.init([], strategy: :one_for_one)
end

defmodule Autoangel.Alert.Supervisor do
  @moduledoc "Supervises alert delivery and its plugin runners."
  use Supervisor

  def start_link(opts \\ []),
    do: Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @impl true
  def init(opts) do
    Supervisor.init(
      [
        {Autoangel.Alert.PluginSupervisor,
         Keyword.put(opts, :name, Autoangel.Alert.PluginSupervisor)},
        {Autoangel.Alert.Dispatcher,
         [state_store: Keyword.get(opts, :state_store, Autoangel.StateStore), plugins: %{}]}
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

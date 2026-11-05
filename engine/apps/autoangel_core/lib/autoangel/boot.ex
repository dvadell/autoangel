defmodule Autoangel.Boot do
  @moduledoc "Tracks the Engine boot phase for readiness and startup diagnostics."

  @key {__MODULE__, :phase}

  @spec start_link(keyword()) :: Agent.on_start()
  def start_link(_opts \\ []) do
    :persistent_term.put(@key, :starting)
    Agent.start_link(fn -> :starting end, name: __MODULE__)
  end

  @spec phase() :: :starting | :ready
  def phase do
    :persistent_term.get(@key, :starting)
  end

  @spec ready?() :: boolean()
  def ready?, do: phase() == :ready

  @spec set_phase(:starting | :ready) :: :ok
  def set_phase(phase) when phase in [:starting, :ready] do
    :persistent_term.put(@key, phase)
    if pid = Process.whereis(__MODULE__), do: Agent.update(pid, fn _ -> phase end)
    :ok
  end

  @spec ready() :: :ok
  def ready, do: set_phase(:ready)
end

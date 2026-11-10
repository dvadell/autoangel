defmodule AutoangelReplay.Capture do
  @moduledoc "Holds the alerts captured during one replay."

  use Agent

  @spec start_link(keyword()) :: Agent.on_start()
  def start_link(_opts \\ []), do: Agent.start_link(fn -> [] end, name: __MODULE__)

  @spec record(Autoangel.Alert.t()) :: :ok
  def record(alert) do
    if Process.whereis(__MODULE__), do: Agent.update(__MODULE__, &[alert | &1])
    :ok
  end

  @spec alerts() :: [Autoangel.Alert.t()]
  def alerts, do: Agent.get(__MODULE__, &Enum.reverse/1)

  @spec count() :: non_neg_integer()
  def count, do: Agent.get(__MODULE__, &length/1)
end

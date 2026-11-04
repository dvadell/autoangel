defmodule Autoangel.Clock.Simulated do
  @moduledoc "A manually controlled clock for deterministic tests."
  @behaviour Autoangel.Clock

  use Agent

  @impl true
  def now_ms(clock), do: Agent.get(clock, & &1)

  @spec start_link(integer()) :: Agent.on_start()
  def start_link(initial_ms \\ 0), do: Agent.start_link(fn -> initial_ms end)

  @spec advance(Agent.agent(), integer()) :: :ok
  def advance(clock, milliseconds) when is_integer(milliseconds) do
    Agent.update(clock, &(&1 + milliseconds))
  end

  @spec set(Agent.agent(), integer()) :: :ok
  def set(clock, milliseconds) when is_integer(milliseconds) do
    Agent.update(clock, fn _ -> milliseconds end)
  end
end

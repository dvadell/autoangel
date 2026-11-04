defmodule Autoangel.Clock.System do
  @moduledoc "Wall-clock implementation used in production."
  @behaviour Autoangel.Clock

  @impl true
  def now_ms(_clock), do: System.system_time(:millisecond)
end

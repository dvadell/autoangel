defmodule Autoangel.Application do
  @moduledoc "Application lifecycle hooks used by the Engine supervision tree."

  require Logger

  @spec prep_stop(term()) :: term()
  def prep_stop(state) do
    case Process.whereis(Autoangel.Checkpointer) do
      nil ->
        state

      checkpointer ->
        case Autoangel.Checkpointer.checkpoint_now(checkpointer) do
          :ok ->
            state

          {:error, reason} ->
            Logger.error("final checkpoint write failure: #{inspect(reason)}")
            state
        end
    end
  end
end

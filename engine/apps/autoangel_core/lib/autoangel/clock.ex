defmodule Autoangel.Clock do
  @moduledoc """
  Clock boundary used by runtime code and deterministic tests.

  `now_ms/0` is the Engine's wall-clock time in milliseconds: the 0-arity
  function configured as `config :autoangel_core, :now` when present (the
  Replay_Harness uses a simulated clock), otherwise system time.
  """

  @callback now_ms(clock :: term()) :: integer()

  @spec now_ms() :: integer()
  def now_ms do
    case Application.get_env(:autoangel_core, :now) do
      now when is_function(now, 0) -> now.()
      _ -> System.system_time(:millisecond)
    end
  end
end

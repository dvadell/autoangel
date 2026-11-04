defmodule Autoangel.Clock do
  @moduledoc "Clock boundary used by runtime code and deterministic tests."

  @callback now_ms(clock :: term()) :: integer()
end

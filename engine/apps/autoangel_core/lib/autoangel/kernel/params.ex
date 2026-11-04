defmodule Autoangel.Kernel.Params do
  @moduledoc "Parameters used by the pure scoring kernel."

  @enforce_keys []
  defstruct window_size: 256,
            mad_scale: 1.4826,
            scale_floor: 1.0e-9,
            scale_fraction: 1.0e-6,
            score_threshold: 75.0

  @type t :: %__MODULE__{
          window_size: pos_integer(),
          mad_scale: number(),
          scale_floor: number(),
          scale_fraction: number(),
          score_threshold: number()
        }
end

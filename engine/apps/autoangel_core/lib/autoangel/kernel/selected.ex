defmodule Autoangel.Kernel.Selected do
  @moduledoc """
  Compile-time selected kernel implementation.

  The M0 NIF gate promotes kernels individually. An empty promotion set keeps
  the Elixir reference implementation selected and does not load a Rust NIF.
  """

  @selected_implementation :elixir
  @implementation Autoangel.Kernel.Scoring.Elixir

  @type implementation :: :elixir

  @spec implementation() :: implementation()
  def implementation, do: @selected_implementation

  def score(state, samples, params \\ %Autoangel.Kernel.Params{}),
    do: @implementation.score(state, samples, params)
end

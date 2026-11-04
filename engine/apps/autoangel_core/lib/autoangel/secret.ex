defmodule Autoangel.Secret do
  @moduledoc "A value that is safe to inspect and stringify in logs."

  defstruct [:value]

  @type t :: %__MODULE__{value: String.t()}

  def new(value) when is_binary(value), do: %__MODULE__{value: value}
  def unwrap(%__MODULE__{value: value}), do: value

  defimpl Inspect do
    def inspect(_secret, _opts), do: "[REDACTED]"
  end

  defimpl String.Chars do
    def to_string(_secret), do: "[REDACTED]"
  end
end

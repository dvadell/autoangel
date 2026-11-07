defmodule Autoangel.Test.CountingPlugin do
  @moduledoc """
  Test-only plugin used to exercise `Autoangel.Plugin.Registry.validate/1`.

  The real compiled plugin set is empty in every shipped config, so this module
  is compiled into the `:test` build and registered in
  `config :autoangel_core, :plugins` for `:test` only (see `config/test.exs`).
  That lets the registry resolve a plugin named "counting" and actually invoke
  `validate_config/1`, which is otherwise unreachable.

  `validate_config/1` increments a global counter on every call so a test can
  assert the callback runs exactly once per unique enabled plugin. Errors return
  only field-name atoms and never echo configuration values.
  """

  @behaviour Autoangel.Plugin

  @counter_key {__MODULE__, :validate_config_calls}

  @required_field "recipient"

  @doc "Resets the validate_config call counter to zero."
  @spec reset_counter() :: :ok
  def reset_counter do
    :persistent_term.put(@counter_key, 0)
    :ok
  end

  @doc "Returns the number of times validate_config/1 has been called since reset."
  @spec call_count() :: non_neg_integer()
  def call_count do
    :persistent_term.get(@counter_key, 0)
  end

  @impl Autoangel.Plugin
  def validate_config(config) when is_map(config) do
    :persistent_term.put(@counter_key, call_count() + 1)

    if has_required_field?(config) do
      :ok
    else
      # Return only the missing field name; never echo config values.
      {:error, [:recipient]}
    end
  end

  def validate_config(_config) do
    :persistent_term.put(@counter_key, call_count() + 1)
    {:error, [:recipient]}
  end

  @impl Autoangel.Plugin
  def deliver(_alert, _config), do: :ok

  defp has_required_field?(config) do
    Map.has_key?(config, @required_field) or Map.has_key?(config, :recipient)
  end
end

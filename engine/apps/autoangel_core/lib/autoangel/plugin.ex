defmodule Autoangel.Plugin do
  @moduledoc """
  Behaviour implemented by compile-time alert delivery plugins.

  Plugins are modules compiled into the release. `deliver/2` returns the outcome
  of one delivery attempt; the dispatcher uses retryable and permanent outcomes
  to decide whether another attempt is allowed.
  """

  @typedoc "The result of one delivery attempt."
  @type delivery_outcome :: :ok | {:retryable, term()} | {:permanent, term()}

  @doc """
  Validates a plugin configuration before the alert dispatcher starts.

  The argument is the plugin's configuration map. Return `:ok` for valid
  configuration or `{:error, fields}` where `fields` names invalid fields.
  Validation errors must contain field names, not secret values.
  """
  @callback validate_config(config :: map()) ::
              :ok | {:error, [atom() | String.t() | {atom() | String.t(), String.t()}]}

  @doc """
  Delivers one alert using the plugin configuration.

  The first argument is the alert and the second is the plugin configuration.
  Return `:ok` on success, `{:retryable, reason}` for a transient failure, or
  `{:permanent, reason}` when retrying cannot succeed.
  """
  @callback deliver(alert :: term(), config :: map()) :: delivery_outcome()
end

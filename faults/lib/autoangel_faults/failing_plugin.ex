defmodule AutoangelFaults.FailingPlugin do
  @moduledoc "Fault-test plugin whose delivery always fails (Requirement 73.7)."
  @behaviour Autoangel.Plugin

  @impl true
  def validate_config(_config), do: :ok

  @impl true
  def deliver(_alert, config), do: {:retryable, Map.get(config, :reason, :injected_failure)}
end

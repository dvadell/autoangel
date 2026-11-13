defmodule AutoangelReplay.CapturePlugin do
  @moduledoc """
  Replay-only alert plugin (`engine_plugin_capture` in the design). It records
  every Anomaly alert it receives in `AutoangelReplay.Capture`; self-monitoring
  alerts are ignored because they do not concern a replayed Series.
  """

  @behaviour Autoangel.Plugin

  @impl true
  def validate_config(_config), do: :ok

  @impl true
  # A resolution closes an Incident the harness already counted at its start.
  def deliver(%Autoangel.Alert{resolved?: true}, _config), do: :ok

  def deliver(%Autoangel.Alert{} = alert, _config) do
    AutoangelReplay.Capture.record(alert)
    :ok
  end

  def deliver(_other, _config), do: :ok
end

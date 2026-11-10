defmodule Autoangel.Alert do
  @moduledoc """
  Notification built from a detected anomaly.

  Built once by `Autoangel.Alert.Render.build/3`; every Plugin formats this same
  struct with `Autoangel.Alert.Render.text/1`.
  """

  @type t :: %__MODULE__{}
  defstruct name: nil,
            labels: %{},
            observed: nil,
            range: nil,
            baseline_type: nil,
            score: nil,
            start_time: nil,
            start_time_iso: nil,
            anomaly_id: nil,
            anomaly: nil,
            candidates: [],
            candidate_count: 0,
            console_url: nil
end

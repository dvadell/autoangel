defmodule Autoangel.Alert do
  @moduledoc "Notification built from a detected anomaly."

  @type t :: %__MODULE__{}
  defstruct name: nil,
            labels: %{},
            observed: nil,
            range: nil,
            score: nil,
            start_time: nil,
            anomaly_id: nil,
            anomaly: nil,
            candidates: []
end

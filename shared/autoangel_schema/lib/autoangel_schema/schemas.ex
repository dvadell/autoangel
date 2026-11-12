defmodule AutoangelSchema.Series do
  @moduledoc "A monitored Series (read-only view of the `series` table)."
  use Ecto.Schema

  @primary_key {:id, Ecto.UUID, autogenerate: false}
  schema "series" do
    field(:name, :string)
    field(:labels, :map)
    field(:kind, :string)
    field(:status, :string)
    field(:inserted_at, :utc_datetime_usec)
  end
end

defmodule AutoangelSchema.Sample do
  @moduledoc "One raw sample (read-only view of the `samples` hypertable)."
  use Ecto.Schema

  @primary_key false
  schema "samples" do
    field(:series_id, Ecto.UUID)
    field(:ts, :utc_datetime_usec)
    field(:value, :float)
  end
end

defmodule AutoangelSchema.Anomaly do
  @moduledoc "A stored Anomaly record (read-only view of the `anomalies` table)."
  use Ecto.Schema

  @primary_key {:id, Ecto.UUID, autogenerate: false}
  schema "anomalies" do
    field(:series_id, Ecto.UUID)
    field(:start_time, :utc_datetime_usec)
    field(:end_time, :utc_datetime_usec)
    field(:score, :float)
    field(:observed, :float)
    field(:expected_lower, :float)
    field(:expected_upper, :float)
    field(:baseline_type, :string)
    field(:status, :string)
    field(:kind, :string)
    field(:classification, :string)
    field(:learning, :boolean)
    field(:inserted_at, :utc_datetime_usec)
  end
end

defmodule AutoangelSchema.AnomalyCandidate do
  @moduledoc "A ranked correlated candidate stored for an Anomaly."
  use Ecto.Schema

  @primary_key false
  schema "anomaly_candidates" do
    field(:anomaly_id, Ecto.UUID)
    field(:series_id, Ecto.UUID)
    field(:rank, :integer)
    field(:evidence, :map)
    field(:weak, :boolean)
  end
end

defmodule AutoangelSchema.BaselineBand do
  @moduledoc "The expected band of a Series at one instant."
  use Ecto.Schema

  @primary_key false
  schema "baseline_bands" do
    field(:series_id, Ecto.UUID)
    field(:ts, :utc_datetime_usec)
    field(:lower_bound, :float)
    field(:upper_bound, :float)
    field(:baseline_type, :string)
  end
end

defmodule AutoangelSchema.EngineStatus do
  @moduledoc "One Engine Heartbeat row."
  use Ecto.Schema

  @primary_key false
  schema "engine_status" do
    field(:ts, :utc_datetime_usec)
    field(:version, :string)
  end
end

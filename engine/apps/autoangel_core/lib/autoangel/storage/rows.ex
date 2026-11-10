defmodule Autoangel.Storage.Rows do
  @moduledoc """
  Pure builders for the storage rows the Storage_Writer hands to a backend
  (Requirements 21.1, 21.3, 21.4, 38.2, 38.5, 38.6, 53.1).

  Rows are plain maps with 16-byte binary ids so that `autoangel_core` never
  depends on a concrete backend.

    * `{:series, series(...)}` — upserted at Series creation and on status
      changes, ahead of any dependent item.
    * `{:anomaly, anomaly_item(anomaly, candidates)}` — an Anomaly start with all
      its Candidates, written in one transaction.
    * `{:anomaly, anomaly_update(anomaly)}` — a later update (end time, status);
      it carries no `:candidates` key, so only the anomalies row is touched.
    * `{:band, band(...)}` — one baseline band row per Series per checkpoint.
  """

  alias Autoangel.SeriesRegistry

  @type series_key :: {String.t(), [{String.t(), String.t()}]}

  @type anomaly_row :: %{
          id: binary(),
          series_id: <<_::128>>,
          kind: term(),
          classification: term(),
          start_time: term(),
          end_time: term(),
          score: term(),
          observed: term(),
          lower: term(),
          upper: term(),
          baseline_type: term(),
          learning: boolean(),
          status: :open | :resolved
        }

  @spec series(series_key(), atom(), atom()) :: map()
  def series({name, labels} = key, kind, status) do
    %{
      id: SeriesRegistry.series_id(key),
      name: name,
      labels: Map.new(labels),
      kind: kind,
      status: status
    }
  end

  @doc "An Anomaly start together with every Candidate the Correlator produced."
  @spec anomaly_item(map(), [map()]) :: %{anomaly: anomaly_row(), candidates: [map()]}
  def anomaly_item(anomaly, candidates) when is_list(candidates) do
    %{anomaly: anomaly_row(anomaly, :open), candidates: Enum.map(candidates, &candidate_row/1)}
  end

  @doc "A later Anomaly update; touches only the anomalies row."
  @spec anomaly_update(map()) :: %{anomaly: anomaly_row()}
  def anomaly_update(anomaly) do
    status = if is_nil(Map.get(anomaly, :end_ms)), do: :open, else: :resolved
    %{anomaly: anomaly_row(anomaly, status)}
  end

  @spec band(series_key(), integer(), number(), number(), atom() | String.t()) :: map()
  def band(key, ts_ms, lower, upper, baseline_type) do
    %{
      series_id: SeriesRegistry.series_id(key),
      ts: ts_ms,
      lower: lower,
      upper: upper,
      baseline_type: baseline_type
    }
  end

  @doc """
  Maps an Engine anomaly id to the 16-byte uuid stored in the database.

  A 16-byte raw id passes through; 32 hex characters (optionally dashed) decode
  directly. Any other binary maps deterministically to a hash, so a start and
  its later update always address the same row.
  """
  @spec anomaly_uuid(binary()) :: binary()
  def anomaly_uuid(<<_::128>> = raw), do: raw

  def anomaly_uuid(id) when is_binary(id) do
    hex = String.replace(id, "-", "")

    case byte_size(hex) == 32 and Base.decode16(hex, case: :mixed) do
      {:ok, raw} -> raw
      _ -> binary_part(:crypto.hash(:sha256, id), 0, 16)
    end
  end

  defp anomaly_row(anomaly, status) do
    %{
      id: anomaly_uuid(Map.fetch!(anomaly, :id)),
      series_id: SeriesRegistry.series_id(Map.fetch!(anomaly, :series_key)),
      kind: Map.get(anomaly, :kind, :value),
      classification: Map.get(anomaly, :classification),
      start_time: Map.fetch!(anomaly, :start_ms),
      end_time: Map.get(anomaly, :end_ms),
      score: Map.get(anomaly, :score),
      observed: Map.get(anomaly, :observed),
      lower: Map.get(anomaly, :lower),
      upper: Map.get(anomaly, :upper),
      baseline_type: Map.get(anomaly, :baseline_type),
      learning: Map.get(anomaly, :learning?, false) == true,
      status: status
    }
  end

  defp candidate_row(candidate) do
    weak? = Map.get(candidate, :weak?, false) == true

    %{
      series_id: SeriesRegistry.series_id(Map.fetch!(candidate, :series_key)),
      rank: Map.fetch!(candidate, :rank),
      evidence: candidate |> Map.get(:evidence, %{}) |> Map.put(:weak?, weak?),
      weak: weak?
    }
  end
end

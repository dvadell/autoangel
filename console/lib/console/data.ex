defmodule Console.Data do
  @moduledoc """
  Everything the views read, behind one behaviour. The live implementation is
  `Console.Data.Repo`; tests swap in a fake with
  `Application.put_env(:autoangel_console, :data, Fake)`.

  Every callback returns `{:error, :database}` when the database cannot answer,
  so each view can show its database-error state instead of crashing.
  """

  @type result(t) :: {:ok, t} | {:error, :database}

  @callback list_anomalies(now :: DateTime.t(), recent_seconds :: pos_integer()) ::
              result([map()])
  @callback candidates(anomaly_ids :: [String.t()]) :: result(%{String.t() => [map()]})
  @callback list_series(search :: String.t()) :: result([map()])
  @callback series(ids :: [String.t()]) :: result([map()])
  @callback anomaly(id :: String.t()) :: result(map() | nil)
  @callback chart(series :: map(), from :: DateTime.t(), to :: DateTime.t(), now :: DateTime.t()) ::
              result(map())
  @callback latest_heartbeat() :: result(DateTime.t() | nil)

  @doc "The configured implementation."
  @spec impl() :: module()
  def impl, do: Application.get_env(:autoangel_console, :data, Console.Data.Repo)
end

defmodule Console.Data.Repo do
  @moduledoc "Reads through the read-only `Console.Repo` with bounded `AutoangelSchema.Query` builders."
  @behaviour Console.Data

  require Logger

  alias AutoangelSchema.Query
  alias AutoangelSchema.Query.Bounds
  alias Console.{Chart, Repo}

  @impl true
  def list_anomalies(now, recent_seconds) do
    safely(fn ->
      b = bounds(DateTime.add(now, -recent_seconds, :second), now)
      anomalies = Repo.all(Query.anomalies(b))
      series = series_map(Enum.map(anomalies, & &1.series_id))
      Enum.map(anomalies, &with_series(&1, series))
    end)
  end

  @impl true
  def candidates(anomaly_ids) do
    safely(fn ->
      b = bounds(DateTime.utc_now(), DateTime.utc_now())
      rows = Repo.all(Query.candidates(anomaly_ids, b))
      series = series_map(Enum.map(rows, & &1.series_id))

      rows
      |> Enum.map(&with_series(&1, series))
      |> Enum.group_by(& &1.anomaly_id)
    end)
  end

  @impl true
  def list_series(search) do
    safely(fn -> Repo.all(Query.series(bounds(DateTime.utc_now(), DateTime.utc_now()), search)) end)
  end

  @impl true
  def series(ids) do
    safely(fn ->
      Repo.all(Query.series_by_ids(ids, bounds(DateTime.utc_now(), DateTime.utc_now())))
    end)
  end

  @impl true
  def anomaly(id) do
    case Ecto.UUID.cast(id) do
      {:ok, uuid} -> safely(fn -> find_anomaly(uuid) end)
      :error -> {:ok, nil}
    end
  end

  defp find_anomaly(uuid) do
    case Repo.one(Query.anomaly(uuid)) do
      nil -> nil
      a -> with_series(a, series_map([a.series_id]))
    end
  end

  @impl true
  def chart(series, from, to, now) do
    safely(fn ->
      b = bounds(from, to)
      ids = [series.id]
      limit = setting(:chart_point_limit, 2_000)

      tier =
        Chart.tier(%{
          from: from,
          to: to,
          now: now,
          raw_count: Repo.one(Query.sample_count(ids, %{b | row_limit: limit})),
          raw_retention: setting(:raw_retention_seconds, 14 * 86_400),
          aggregate_retention: setting(:aggregate_retention_seconds, 395 * 86_400),
          point_limit: limit
        })

      points =
        case tier do
          :raw -> Repo.all(Query.samples(ids, b))
          other -> Repo.all(Query.aggregate(other, ids, b))
        end
        |> Enum.map(&%{ts: &1.ts, value: &1.value})
        |> then(&if(series.kind == "counter", do: Chart.counter_rates(&1), else: &1))
        |> Chart.downsample(limit)

      bands =
        if tier == :raw,
          do: Repo.all(Query.baseline_bands(ids, b)),
          else: Repo.all(Query.band_aggregate(ids, b))

      anomalies = Repo.all(Query.anomalies_overlapping(series.id, b))

      %{
        tier: tier,
        points: points,
        bands: Enum.map(bands, &Map.take(&1, [:ts, :lower_bound, :upper_bound])),
        markers: Chart.markers(anomalies, from, to, now)
      }
    end)
  end

  @impl true
  def latest_heartbeat do
    safely(fn ->
      case Repo.one(Query.latest_heartbeat()) do
        nil -> nil
        row -> row.ts
      end
    end)
  end

  defp bounds(from, to) do
    {:ok, b} = Bounds.resolve(range: {from, to}, series_limit: 50, row_limit: 10_000)
    b
  end

  defp series_map([]), do: %{}

  defp series_map(ids) do
    ids
    |> Enum.uniq()
    |> Enum.chunk_every(50)
    |> Enum.flat_map(
      &Repo.all(Query.series_by_ids(&1, bounds(DateTime.utc_now(), DateTime.utc_now())))
    )
    |> Map.new(&{&1.id, &1})
  end

  defp with_series(row, series),
    do: row |> Map.from_struct() |> Map.put(:series, series[row.series_id])

  defp setting(key, default), do: Application.get_env(:autoangel_console, key, default)

  defp safely(fun) do
    {:ok, fun.()}
  rescue
    e in [DBConnection.ConnectionError, Postgrex.Error] ->
      Logger.warning("Console database read failed: #{Exception.message(e)}")
      {:error, :database}
  catch
    :exit, reason ->
      Logger.warning("Console database read failed: #{inspect(reason)}")
      {:error, :database}
  end
end

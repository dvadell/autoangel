defmodule AutoangelSchema.Query.Bounds do
  @moduledoc """
  The bounds every Console query must carry: a time range, a Series limit and a
  row limit. Requested values above the Console maximums are clamped, and the
  struct records which clamp applied so a view can tell the operator.

  The query helpers accept only this struct, so an unbounded query cannot be built.
  """

  @max_range_seconds 400 * 86_400
  @max_series 50
  @max_rows 10_000

  @type clamp :: :range | :series_limit | :row_limit

  @type t :: %__MODULE__{
          from: DateTime.t(),
          to: DateTime.t(),
          series_limit: pos_integer(),
          row_limit: pos_integer(),
          clamps: [clamp()]
        }

  @enforce_keys [:from, :to, :series_limit, :row_limit, :clamps]
  defstruct [:from, :to, :series_limit, :row_limit, :clamps]

  @doc "The Console maximums: range in seconds, Series count and row count."
  @spec maximums() :: %{range_seconds: 34_560_000, series: 50, rows: 10_000}
  def maximums, do: %{range_seconds: @max_range_seconds, series: @max_series, rows: @max_rows}

  @doc """
  Validates and clamps the requested bounds.

  Requires `:range` (`{from, to}` DateTimes), `:series_limit` and `:row_limit`.
  A range longer than 400 days keeps its end and moves its start forward.
  """
  @spec resolve(keyword() | map()) :: {:ok, t()} | {:error, {:missing | :invalid, atom()}}
  def resolve(opts) when is_list(opts), do: opts |> Map.new() |> resolve()

  def resolve(opts) when is_map(opts) do
    with {:ok, {from, to}} <- fetch_range(opts),
         {:ok, series_limit} <- fetch_limit(opts, :series_limit),
         {:ok, row_limit} <- fetch_limit(opts, :row_limit) do
      {from, range_clamp} = clamp_range(from, to)
      {series_limit, series_clamp} = clamp_limit(series_limit, @max_series, :series_limit)
      {row_limit, row_clamp} = clamp_limit(row_limit, @max_rows, :row_limit)

      {:ok,
       %__MODULE__{
         from: from,
         to: to,
         series_limit: series_limit,
         row_limit: row_limit,
         clamps: range_clamp ++ series_clamp ++ row_clamp
       }}
    end
  end

  defp fetch_range(opts) do
    case Map.fetch(opts, :range) do
      :error ->
        {:error, {:missing, :range}}

      {:ok, {%DateTime{} = from, %DateTime{} = to}} ->
        if DateTime.compare(from, to) == :gt,
          do: {:error, {:invalid, :range}},
          else: {:ok, {from, to}}

      {:ok, _other} ->
        {:error, {:invalid, :range}}
    end
  end

  defp fetch_limit(opts, key) do
    case Map.fetch(opts, key) do
      :error -> {:error, {:missing, key}}
      {:ok, value} when is_integer(value) and value > 0 -> {:ok, value}
      {:ok, _other} -> {:error, {:invalid, key}}
    end
  end

  defp clamp_range(from, to) do
    if DateTime.diff(to, from, :second) > @max_range_seconds,
      do: {DateTime.add(to, -@max_range_seconds, :second), [:range]},
      else: {from, []}
  end

  defp clamp_limit(value, max, _clamp) when value <= max, do: {value, []}
  defp clamp_limit(_value, max, clamp), do: {max, [clamp]}
end

defmodule AutoangelSchema.Query do
  @moduledoc """
  Read-only, bounded query builders. Each takes `AutoangelSchema.Query.Bounds`
  and returns an `Ecto.Query`; none of them can run unbounded.
  """
  import Ecto.Query

  alias AutoangelSchema.{Anomaly, AnomalyCandidate, BaselineBand, EngineStatus, Sample, Series}
  alias AutoangelSchema.Query.Bounds

  @doc """
  Series ordered by name, at most `row_limit`. With a non-empty `search` term,
  only Series whose name, a label name or a label value contains the term
  (case-sensitive, `strpos`) are returned.
  """
  @spec series(Bounds.t(), String.t() | nil) :: Ecto.Query.t()
  def series(%Bounds{row_limit: limit}, search \\ nil) do
    query = from(s in Series, order_by: [asc: s.name, asc: s.id], limit: ^limit)

    case search do
      term when is_binary(term) and term != "" ->
        from(s in query,
          where:
            fragment("strpos(?, ?) > 0", s.name, ^term) or
              fragment(
                "EXISTS (SELECT 1 FROM jsonb_each_text(?) AS kv WHERE strpos(kv.key, ?) > 0 OR strpos(kv.value, ?) > 0)",
                s.labels,
                ^term,
                ^term
              )
        )

      _ ->
        query
    end
  end

  @doc "The given Series (at most `series_limit` of them)."
  @spec series_by_ids([Ecto.UUID.t()], Bounds.t()) :: Ecto.Query.t()
  def series_by_ids(ids, %Bounds{series_limit: limit}) when is_list(ids) do
    ids = Enum.take(ids, limit)
    from(s in Series, where: s.id in ^ids, limit: ^limit)
  end

  @doc """
  Aggregated points (`:minute` -> `samples_1m`, `:hour` -> `samples_1h`) of at most
  `series_limit` Series inside the range, as `%{series_id, ts, value}` with the
  bucket mean as value.
  """
  @spec aggregate(:minute | :hour, [Ecto.UUID.t()], Bounds.t()) :: Ecto.Query.t()
  def aggregate(tier, series_ids, %Bounds{} = b) when tier in [:minute, :hour] do
    ids = series_ids |> Enum.take(b.series_limit) |> Enum.map(&Ecto.UUID.dump!/1)
    source = if tier == :minute, do: "samples_1m", else: "samples_1h"

    from(s in source,
      where: s.series_id in ^ids and s.bucket >= ^b.from and s.bucket <= ^b.to,
      order_by: [asc: s.bucket],
      limit: ^b.row_limit,
      select: %{series_id: type(s.series_id, Ecto.UUID), ts: s.bucket, value: s.mean}
    )
  end

  @doc "Hourly aggregate of the expected band (`baseline_bands_1h`) inside the range."
  @spec band_aggregate([Ecto.UUID.t()], Bounds.t()) :: Ecto.Query.t()
  def band_aggregate(series_ids, %Bounds{} = b) do
    ids = series_ids |> Enum.take(b.series_limit) |> Enum.map(&Ecto.UUID.dump!/1)

    from(s in "baseline_bands_1h",
      where: s.series_id in ^ids and s.bucket >= ^b.from and s.bucket <= ^b.to,
      order_by: [asc: s.bucket],
      limit: ^b.row_limit,
      select: %{
        series_id: type(s.series_id, Ecto.UUID),
        ts: s.bucket,
        lower_bound: s.minimum,
        upper_bound: s.maximum
      }
    )
  end

  @doc "Number of raw samples in the range, counted up to `row_limit + 1`."
  @spec sample_count([Ecto.UUID.t()], Bounds.t()) :: Ecto.Query.t()
  def sample_count(series_ids, %Bounds{} = b) do
    inner = samples(series_ids, %{b | row_limit: b.row_limit + 1})
    from(s in subquery(select(inner, [s], %{ts: s.ts})), select: count())
  end

  @doc "Raw samples of at most `series_limit` of the given Series inside the range."
  @spec samples([Ecto.UUID.t()], Bounds.t()) :: Ecto.Query.t()
  def samples(series_ids, %Bounds{} = b) when is_list(series_ids) do
    ids = Enum.take(series_ids, b.series_limit)

    from(s in Sample,
      where: s.series_id in ^ids and s.ts >= ^b.from and s.ts <= ^b.to,
      order_by: [asc: s.ts],
      limit: ^b.row_limit
    )
  end

  @doc "Baseline bands of at most `series_limit` of the given Series inside the range."
  @spec baseline_bands([Ecto.UUID.t()], Bounds.t()) :: Ecto.Query.t()
  def baseline_bands(series_ids, %Bounds{} = b) when is_list(series_ids) do
    ids = Enum.take(series_ids, b.series_limit)

    from(s in BaselineBand,
      where: s.series_id in ^ids and s.ts >= ^b.from and s.ts <= ^b.to,
      order_by: [asc: s.ts],
      limit: ^b.row_limit
    )
  end

  @doc """
  Anomalies that are open, or that ended inside the range; open first, newest
  start first within each group.
  """
  @spec anomalies(Bounds.t()) :: Ecto.Query.t()
  def anomalies(%Bounds{} = b) do
    from(a in Anomaly,
      where: is_nil(a.end_time) or (a.end_time >= ^b.from and a.end_time <= ^b.to),
      order_by: [asc: not is_nil(a.end_time), desc: a.start_time, asc: a.id],
      limit: ^b.row_limit
    )
  end

  @doc "Anomalies of one Series whose interval overlaps the range (open ones end now)."
  @spec anomalies_overlapping(Ecto.UUID.t(), Bounds.t()) :: Ecto.Query.t()
  def anomalies_overlapping(series_id, %Bounds{} = b) do
    from(a in Anomaly,
      where:
        a.series_id == ^series_id and a.start_time <= ^b.to and
          (is_nil(a.end_time) or a.end_time >= ^b.from),
      order_by: [asc: a.start_time],
      limit: ^b.row_limit
    )
  end

  @doc "One Anomaly by id."
  @spec anomaly(Ecto.UUID.t()) :: Ecto.Query.t()
  def anomaly(id), do: from(a in Anomaly, where: a.id == ^id, limit: 1)

  @doc "Stored candidates of at most `row_limit` Anomalies, by rank."
  @spec candidates([Ecto.UUID.t()], Bounds.t()) :: Ecto.Query.t()
  def candidates(anomaly_ids, %Bounds{} = b) when is_list(anomaly_ids) do
    ids = Enum.take(anomaly_ids, b.row_limit)

    from(c in AnomalyCandidate,
      where: c.anomaly_id in ^ids,
      order_by: [asc: c.anomaly_id, asc: c.rank],
      limit: ^b.row_limit
    )
  end

  @doc "The most recent Heartbeat row."
  @spec latest_heartbeat() :: Ecto.Query.t()
  def latest_heartbeat, do: from(e in EngineStatus, order_by: [desc: e.ts], limit: 1)
end

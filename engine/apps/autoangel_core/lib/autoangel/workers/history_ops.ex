defmodule Autoangel.Workers.HistoryOps do
  @moduledoc """
  The pure parts of what Workers do with stored history: planning a warm start
  and applying a seasonality decision to a Series (Requirements 44.1 to 44.5 and
  12.1 to 12.7). Reading history is `Autoangel.History`; this module never reads.
  """

  alias Autoangel.Detect.Seasonality
  alias Autoangel.History

  @hour_ms 3_600_000

  @doc """
  Combines Samples read from storage with the live Samples buffered while the
  Series was warming. Returns `{stored_only, buffer}`: the stored Samples whose
  timestamp is not in the buffer, oldest first, then the buffered Samples,
  oldest first. Applying `stored_only` and then `buffer` applies each distinct
  timestamp exactly once (Property 44). `stored` holds `{ts, value}`; `ts_fun`
  gives the timestamp of a buffered item.
  """
  @spec plan([{integer(), term()}], [term()], (term() -> integer())) ::
          {[{integer(), term()}], [term()]}
  def plan(stored, buffer, ts_fun) do
    buffered = MapSet.new(buffer, ts_fun)

    stored_only =
      stored
      |> Enum.reject(fn {ts, _value} -> MapSet.member?(buffered, ts) end)
      |> Enum.uniq_by(&elem(&1, 0))
      |> Enum.sort_by(&elem(&1, 0))

    {stored_only, Enum.sort_by(buffer, ts_fun)}
  end

  @doc "The raw storage rows of one Series as `{ts_ms, value}`."
  @spec stored_samples([tuple()], binary()) :: [{integer(), number()}]
  def stored_samples(rows, series_id) do
    for {^series_id, ts, value} <- rows, do: {ts, value}
  end

  @doc """
  The seasonality analysis of the given Series (`%{series_id => key}`), run in a
  background Task: reads 1-hour aggregates for the last 15 days and decides per
  Series. Returns `{:ok, %{key => :clear | {:install, period, cells}}}`; a read
  failure is `{:error, reason}` and leaves every Baseline unchanged.
  """
  @spec analyse_seasonality(%{binary() => term()}, integer(), keyword()) ::
          {:ok, map()} | {:error, term()}
  def analyse_seasonality(ids_to_keys, now_ms, opts) do
    {range, _} = History.defaults()
    threshold = Keyword.get(opts, :threshold, Seasonality.default_threshold())

    case History.read(Map.keys(ids_to_keys), now_ms - range, now_ms, [{:resolution, :hour} | opts]) do
      {:ok, rows} ->
        hourly = Seasonality.hourly_from_rows(rows)

        decisions =
          for {id, key} <- ids_to_keys, into: %{} do
            {key, Seasonality.analyse(Map.get(hourly, id, []), threshold: threshold)}
          end

        {:ok, decisions}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Applies a seasonality decision to a Series entry (`kernel_state` and `meta`).
  `:clear` falls back to the robust Baseline; an install sets the per-phase cells
  and records the period so learning ends after the configured number of cycles.
  A state that cannot be decoded is left as it is.
  """
  @spec apply_decision(map(), :clear | {:install, 24 | 168, list()}) :: map()
  def apply_decision(entry, :clear) do
    case Seasonality.clear(entry.kernel_state) do
      {:ok, state} -> %{entry | kernel_state: state, meta: %{entry.meta | seasonal: nil}}
      {:error, _} -> entry
    end
  end

  def apply_decision(entry, {:install, period, cells}) do
    case Seasonality.install(entry.kernel_state, period, cells) do
      {:ok, state} ->
        type = if period == 24, do: :seasonal_daily, else: :seasonal_weekly
        %{entry | kernel_state: state, meta: %{entry.meta | seasonal: {type, period * @hour_ms}}}

      {:error, _} ->
        entry
    end
  end
end

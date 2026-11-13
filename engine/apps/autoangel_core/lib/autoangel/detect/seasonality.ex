defmodule Autoangel.Detect.Seasonality do
  @moduledoc """
  Seasonality detection and per-phase Baselines (Requirements 12.1 to 12.7).

  Input is a Series' 1-hour means `{hour_index, mean}` (`hour_index` is the Unix
  time in hours). A daily period (24 hours) or a weekly period (168 hours) is
  significant when the autocorrelation of the hourly means at that lag reaches
  the threshold (default 0.3) and the history covers at least two cycles.
  When both are significant the weekly period wins only if its autocorrelation
  is clearly higher (by 0.1); otherwise the shorter, daily period is used.

  A significant period installs one Baseline cell per phase, seeded from the
  hourly means: the median, the median absolute deviation and the count.
  `Autoangel.Kernel.Scoring.Elixir` then scores with the cell of the Sample's
  phase and reports `:seasonal_daily` or `:seasonal_weekly`. With no
  significant period the Series falls back to the robust Baseline.
  """

  alias Autoangel.Kernel.State

  @periods [24, 168]
  @default_threshold 0.3
  @weekly_margin 0.1

  @type hourly :: [{integer(), number()}]
  @type cell :: %{med: float(), mad: float(), n: non_neg_integer()}

  @doc "The analysis periods in hours."
  @spec periods() :: [24 | 168, ...]
  def periods, do: @periods

  @doc "The default significance threshold."
  @spec default_threshold() :: float()
  def default_threshold, do: @default_threshold

  @doc "True for a usable significance threshold: a number above 0 and at most 1."
  @spec valid_threshold?(term()) :: boolean()
  def valid_threshold?(value), do: is_number(value) and value > 0 and value <= 1

  @doc """
  Autocorrelation of the hourly means at `lag` hours. Only hours that have a
  partner `lag` hours later contribute. Returns `nil` with fewer than two
  cycles of history (`lag * 2` hours between the first and last mean), or when
  the means do not vary.
  """
  @spec acf(hourly(), pos_integer()) :: float() | nil
  def acf(hourly, lag) do
    by_hour = Map.new(hourly)
    hours = Map.keys(by_hour)

    if hours != [] and Enum.max(hours) - Enum.min(hours) + 1 >= 2 * lag,
      do: lagged_correlation(by_hour, lag),
      else: nil
  end

  defp lagged_correlation(by_hour, lag) do
    mean = Enum.sum(Map.values(by_hour)) / map_size(by_hour)
    pairs = for {h, v} <- by_hour, partner = by_hour[h + lag], partner != nil, do: {v, partner}
    variance = Enum.sum(for {_h, v} <- by_hour, do: (v - mean) * (v - mean))

    if variance > 1.0e-12 and pairs != [], do: correlation(pairs, mean), else: nil
  end

  # Normalised by the variance of the paired hours, so a full-length series of a
  # pure periodic signal gives 1.0.
  defp correlation(pairs, mean) do
    covariance = Enum.sum(for {a, b} <- pairs, do: (a - mean) * (b - mean))

    scale =
      Enum.sum(for {a, b} <- pairs, do: ((a - mean) * (a - mean) + (b - mean) * (b - mean)) / 2)

    if scale <= 1.0e-12, do: nil, else: covariance / scale
  end

  @doc """
  The significant period (24 or 168) of the hourly means, or `:none`.
  """
  @spec classify(hourly(), keyword()) :: {:ok, 24 | 168, float()} | :none
  def classify(hourly, opts \\ []) do
    threshold = Keyword.get(opts, :threshold, @default_threshold)

    significant =
      for period <- @periods,
          value = acf(hourly, period),
          value != nil and value >= threshold,
          do: {period, value}

    case significant do
      [] -> :none
      [{period, value}] -> {:ok, period, value}
      [{24, daily}, {168, weekly}] when weekly > daily + @weekly_margin -> {:ok, 168, weekly}
      [{24, daily}, {168, _weekly}] -> {:ok, 24, daily}
    end
  end

  @doc "The per-phase cells for `period`, seeded from the hourly means."
  @spec cells(hourly(), 24 | 168) :: [cell()]
  def cells(hourly, period) do
    by_phase = Enum.group_by(hourly, fn {hour, _} -> Integer.mod(hour, period) end, &elem(&1, 1))

    for phase <- 0..(period - 1) do
      case Map.get(by_phase, phase, []) do
        [] ->
          %{med: 0.0, mad: 0.0, n: 0}

        values ->
          med = median(values)
          %{med: med * 1.0, mad: median(Enum.map(values, &abs(&1 - med))) * 1.0, n: length(values)}
      end
    end
  end

  @doc """
  Analyses `hourly` and returns the install decision:
  `{:install, period, cells}` or `:clear` (no significant period).
  """
  @spec analyse(hourly(), keyword()) :: {:install, 24 | 168, [cell()]} | :clear
  def analyse(hourly, opts \\ []) do
    case classify(hourly, opts) do
      {:ok, period, _acf} -> {:install, period, cells(hourly, period)}
      :none -> :clear
    end
  end

  @doc "Installs the cells into an encoded kernel state."
  @spec install(binary(), 24 | 168, [cell()]) :: {:ok, binary()} | {:error, :bad_state}
  def install(state_bin, period, cells) when period in @periods and length(cells) == period do
    with {:ok, state} <- State.decode(state_bin) do
      {:ok, State.encode(%{state | seasonal: %{period: period, cells: cells}})}
    end
  end

  @doc "Removes the seasonal Baseline from an encoded kernel state (robust fallback)."
  def clear(state_bin) do
    with {:ok, state} <- State.decode(state_bin), do: {:ok, State.encode(%{state | seasonal: nil})}
  end

  @doc "The period of an encoded kernel state in hours, or `nil`."
  @spec period(binary()) :: 24 | 168 | nil
  def period(state_bin) do
    case State.decode(state_bin) do
      {:ok, %{seasonal: %{period: period}}} -> period
      _ -> nil
    end
  end

  defp median(values) do
    sorted = Enum.sort(values)
    n = length(sorted)

    if rem(n, 2) == 1,
      do: Enum.at(sorted, div(n, 2)),
      else: (Enum.at(sorted, div(n, 2) - 1) + Enum.at(sorted, div(n, 2))) / 2
  end

  @doc "Hourly means `{hour_index, mean}` from 1-hour aggregate rows `{series_id, ts_ms, min, max, mean, count}`."
  @spec hourly_from_rows([tuple()]) :: %{binary() => hourly()}
  def hourly_from_rows(rows) do
    rows
    |> Enum.group_by(&elem(&1, 0), fn {_id, ts, _min, _max, mean, _count} ->
      {Integer.floor_div(ts, 3_600_000), mean}
    end)
  end
end

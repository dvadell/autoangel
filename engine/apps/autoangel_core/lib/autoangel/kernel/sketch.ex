defmodule Autoangel.Kernel.Sketch do
  @moduledoc """
  A small bounded quantile sketch for the long-term reference Baseline
  (Requirements 16.1 to 16.5).

  It is a sorted list of centroids `{mean, weight}`. Adding a value inserts a
  weight-1 centroid; when the total weight passes the window length every weight
  is scaled down so old values fade, and when there are more than 100 centroids
  the two neighbours closest together are merged. The state stays bounded and
  encodes into the `longterm` section of `Autoangel.Kernel.State`.
  """

  @max_centroids 100

  @type centroids :: [{float(), float()}]

  @doc "The centroid limit."
  @spec max_centroids() :: 100
  def max_centroids, do: @max_centroids

  @doc "Adds `value`, keeping the total weight at most `window`."
  @spec add(centroids(), number(), pos_integer()) :: centroids()
  def add(centroids, value, window) do
    centroids
    |> insert({value * 1.0, 1.0})
    |> fade(window)
    |> compress()
  end

  @doc "A sketch of `values`, in order, for a window of `window`."
  @spec from_values([number()], pos_integer()) :: centroids()
  def from_values(values, window), do: Enum.reduce(values, [], &add(&2, &1, window))

  @doc "The total weight."
  @spec total(centroids()) :: float()
  def total(centroids), do: Enum.reduce(centroids, 0.0, fn {_m, w}, acc -> acc + w end)

  @doc "The value at quantile `q` (0 to 1), or `nil` for an empty sketch."
  @spec quantile(centroids(), number()) :: float() | nil
  def quantile([], _q), do: nil
  def quantile([{mean, _}], _q), do: mean

  def quantile(centroids, q) do
    target = q * total(centroids)
    walk(centroids, target, 0.0)
  end

  # Interpolates between the midpoints of neighbouring centroids.
  defp walk([{mean, _weight}], _target, _before), do: mean

  defp walk([{m1, w1}, {m2, w2} | rest], target, before) do
    mid1 = before + w1 / 2
    mid2 = before + w1 + w2 / 2

    cond do
      target <= mid1 -> m1
      target <= mid2 -> m1 + (m2 - m1) * (target - mid1) / (mid2 - mid1)
      true -> walk([{m2, w2} | rest], target, before + w1)
    end
  end

  @doc "`{median, MAD}` of the sketch, or `nil` when it is empty."
  @spec median_mad(centroids()) :: {float(), float()} | nil
  def median_mad([]), do: nil

  def median_mad(centroids) do
    median = quantile(centroids, 0.5)

    deviations =
      centroids |> Enum.map(fn {mean, weight} -> {abs(mean - median), weight} end) |> Enum.sort()

    {median, weighted_median(deviations)}
  end

  defp weighted_median(sorted) do
    half = total(sorted) / 2
    find(sorted, half, 0.0)
  end

  defp find([{value, _}], _half, _acc), do: value

  defp find([{value, weight} | rest], half, acc) do
    if acc + weight >= half, do: value, else: find(rest, half, acc + weight)
  end

  defp insert(centroids, {value, _} = centroid) do
    {below, above} = Enum.split_while(centroids, fn {mean, _} -> mean < value end)
    below ++ [centroid | above]
  end

  defp fade(centroids, window) do
    total = total(centroids)

    if total > window,
      do: Enum.map(centroids, fn {mean, weight} -> {mean, weight * window / total} end),
      else: centroids
  end

  defp compress(centroids) when length(centroids) <= @max_centroids, do: centroids

  defp compress(centroids) do
    index =
      centroids
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.with_index()
      |> Enum.min_by(fn {[{m1, _}, {m2, _}], _i} -> m2 - m1 end)
      |> elem(1)

    {before, [{m1, w1}, {m2, w2} | rest]} = Enum.split(centroids, index)
    weight = w1 + w2
    before ++ [{(m1 * w1 + m2 * w2) / weight, weight} | rest]
  end
end

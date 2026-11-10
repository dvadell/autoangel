defmodule AutoangelReplay.Inject do
  @moduledoc """
  Seeded anomaly injectors (Requirements 77.2, 77.3, 77.6).

  `run/3` injects at least one anomaly of every type (spike, drop, level shift,
  silence and slow drift) into the given Series and returns the modified Series
  together with one ground-truth label per injected anomaly:

      %{series_key: key, type: type, start_ms: start, end_ms: finish}

  All randomness comes from an explicit `:rand` state seeded with
  `:rand.seed_s(:exsss, seed)`, so the same input and seed always produce the
  same Series and labels. If the requested injections do not fit into the
  Series, `run/3` returns `{:error, {:insufficient_capacity, requested,
  available}}` instead of a replay with fewer anomalies or missing types. Each injection is placed in the later part of a Series
  (after the first 40% of its Samples) so the Engine has history to learn from,
  and injections in the same Series never overlap.

  The magnitude scale of a Series is its robust spread (1.4826 × MAD of the
  finite values), falling back to 10% of the absolute median and then to 1.0.
  """

  alias AutoangelReplay.Series

  @types [:spike, :drop, :level_shift, :silence, :drift]

  @type type :: :spike | :drop | :level_shift | :silence | :drift
  @type label :: %{series_key: Series.key(), type: type(), start_ms: integer(), end_ms: integer()}

  @spec types() :: [type(), ...]
  def types, do: @types

  @doc """
  Injects every type at least once, assigning types to Series round-robin.
  Options: `:per_type` (default 1) injections of each type.

  Each Series' injectable region (after its first 40% of Samples) is split into
  one slot per injection assigned to it; each injection is placed inside its
  own slot and uses at most half of it, so injections never overlap and are
  followed by calm Samples.
  """
  @spec run([Series.t()], integer(), keyword()) ::
          {:ok, {[Series.t()], [label()]}}
          | {:error, {:insufficient_capacity, pos_integer(), non_neg_integer()}}
          | {:error, {:missing_types, [type()]}}
  def run(series, seed, opts \\ []) when is_list(series) and series != [] do
    per_type = Keyword.get(opts, :per_type, 1)

    unless is_integer(per_type) and per_type >= 1,
      do: raise(ArgumentError, ":per_type must be a positive integer, got: #{inspect(per_type)}")

    case assign(series, per_type) do
      {:ok, assignments} -> inject_planned(series, assignments, seed)
      {:error, _} = error -> error
    end
  end

  defp inject_planned(series, assignments, seed) do
    {out, labels, _rng} =
      series
      |> Enum.with_index()
      |> Enum.reduce({[], [], :rand.seed_s(:exsss, seed)}, fn {s, i}, {out, labels, rng} ->
        {s, new_labels, rng} = inject_all(s, Map.get(assignments, i, []), rng)
        {[s | out], labels ++ new_labels, rng}
      end)

    case @types -- Enum.map(labels, & &1.type) do
      [] -> {:ok, {Enum.reverse(out), labels}}
      missing -> {:error, {:missing_types, missing}}
    end
  end

  # Each injection needs a slot of at least @min_slot Samples in the region after
  # a Series' first 40%, so a Series of n Samples holds div(div(n * 6, 10), 8)
  # injections. Planned injections are dealt round-robin over the Series that
  # still have capacity; if the plan does not fit, nothing is injected.
  @min_slot 8

  defp assign(series, per_type) do
    capacity = Map.new(Enum.with_index(series), fn {s, i} -> {i, capacity(s)} end)
    total = capacity |> Map.values() |> Enum.sum()
    requested = per_type * length(@types)

    if total < requested do
      {:error, {:insufficient_capacity, requested, total}}
    else
      plan = for _ <- 1..per_type, type <- @types, do: type

      {assigned, _capacity, _cursor} =
        Enum.reduce(plan, {%{}, capacity, 0}, fn type, {assigned, capacity, cursor} ->
          i = next_with_capacity(capacity, cursor, map_size(capacity))

          {Map.update(assigned, i, [type], &(&1 ++ [type])), Map.update!(capacity, i, &(&1 - 1)),
           i + 1}
        end)

      {:ok, assigned}
    end
  end

  defp capacity(%Series{samples: samples}), do: div(div(length(samples) * 6, 10), @min_slot)

  defp next_with_capacity(capacity, cursor, count) do
    Enum.find(0..(count - 1), fn step ->
      Map.fetch!(capacity, rem(cursor + step, count)) > 0
    end)
    |> then(&rem(cursor + &1, count))
  end

  defp inject_all(series, [], rng), do: {series, [], rng}

  defp inject_all(%Series{samples: samples} = series, types, rng) do
    n = length(samples)
    lo = div(n * 4, 10)
    slot = div(n - lo, length(types))
    scale = scale(samples)

    # Later slots first: a silence removes Samples, which would otherwise shift
    # the indices of every slot after it.
    types
    |> Enum.with_index()
    |> Enum.reverse()
    |> Enum.reduce({series, [], rng}, fn {type, j}, {s, labels, rng} ->
      case inject(s, type, rng, {lo + j * slot, lo + (j + 1) * slot}, scale) do
        {:ok, s, label, rng} -> {s, [label | labels], rng}
        {:skip, rng} -> {s, labels, rng}
      end
    end)
  end

  @doc """
  Injects one anomaly of `type` into `series` inside the Sample index range
  `{from, to}` (end exclusive) using `rng`. Returns `{:ok, series, label, rng}`,
  or `{:skip, rng}` when the range is too small for the type.
  """
  @spec inject(
          Series.t(),
          type(),
          :rand.state(),
          {non_neg_integer(), non_neg_integer()},
          float() | nil
        ) :: {:ok, Series.t(), label(), :rand.state()} | {:skip, :rand.state()}
  def inject(%Series{samples: samples} = series, type, rng, {from, to}, scale \\ nil)
      when type in @types do
    room = div(to - from, 2)
    {width, rng} = width(type, room, rng)

    if length(samples) < 10 or width < 1 or to > length(samples) do
      {:skip, rng}
    else
      {start_i, rng} = uniform_int(from, from + room - width, rng)
      window = Enum.slice(samples, start_i, width)
      {start_ms, _} = hd(window)
      {end_ms, _} = List.last(window)
      {k, rng} = uniform_float(4.0, 8.0, rng)
      delta = k * (scale || scale(samples))
      samples = apply_type(type, samples, start_i, width, delta)
      label = %{series_key: Series.key(series), type: type, start_ms: start_ms, end_ms: end_ms}
      {:ok, %{series | samples: samples}, label, rng}
    end
  end

  # Spikes and drops are short, but at least three Samples so they can satisfy
  # the Engine's default Persistence_Rule; level shifts, silences and drifts are
  # sustained, using up to the whole room of the slot.
  defp width(type, room, rng) when type in [:spike, :drop], do: cap(uniform_int(3, 5, rng), room)

  defp width(_sustained, room, rng),
    do: cap(uniform_int(max(div(room, 2), 4), max(room, 4), rng), room)

  defp cap({w, rng}, room), do: {min(w, room), rng}

  @doc false
  def apply_type(type, samples, start_i, width, delta) do
    samples
    |> Enum.with_index()
    |> Enum.flat_map(fn {{ts, v} = sample, i} ->
      inside? = i >= start_i and i < start_i + width

      cond do
        not inside? -> [sample]
        type == :silence -> []
        not is_number(v) -> [sample]
        type == :spike -> [{ts, v + delta}]
        type == :drop -> [{ts, v - delta}]
        type == :level_shift -> [{ts, v + delta}]
        type == :drift -> [{ts, v + delta * (i - start_i + 1) / width}]
      end
    end)
  end

  @doc "Robust spread of the finite values (1.4826 × MAD), with fallbacks."
  @spec scale([Series.sample()]) :: float()
  def scale(samples) do
    values = for {_ts, v} <- samples, is_number(v), do: v * 1.0

    case values do
      [] ->
        1.0

      _ ->
        med = median(values)
        mad = values |> Enum.map(&abs(&1 - med)) |> median()

        cond do
          mad > 0 -> 1.4826 * mad
          abs(med) > 0 -> 0.1 * abs(med)
          true -> 1.0
        end
    end
  end

  defp median(values) do
    sorted = Enum.sort(values)
    n = length(sorted)
    mid = div(n, 2)

    if rem(n, 2) == 1,
      do: Enum.at(sorted, mid),
      else: (Enum.at(sorted, mid - 1) + Enum.at(sorted, mid)) / 2
  end

  defp uniform_int(lo, hi, rng) when hi <= lo, do: {lo, rng}

  defp uniform_int(lo, hi, rng) do
    {u, rng} = :rand.uniform_s(hi - lo + 1, rng)
    {lo + u - 1, rng}
  end

  defp uniform_float(lo, hi, rng) do
    {u, rng} = :rand.uniform_s(rng)
    {lo + (hi - lo) * u, rng}
  end
end

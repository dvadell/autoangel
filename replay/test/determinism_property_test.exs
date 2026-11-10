defmodule AutoangelReplay.DeterminismPropertyTest do
  @moduledoc """
  Property 70: replay is deterministic (Requirement 77.6). For any small replay
  input and injection seed, two replays with the same input, Engine version,
  configuration and seed produce identical ground-truth labels and identical
  reports. Each replay runs a real in-process Engine, so inputs stay small
  (one or two Series of a little over a day, past the default learning period).
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  # 100 runs of two real replays each take roughly a minute.
  @moduletag timeout: 600_000

  alias AutoangelReplay.Series

  @base 1_700_000_000_000
  @day 86_400_000

  defp series_gen do
    gen all(
          count <- integer(1..2),
          step_min <- member_of([5, 10]),
          days <- member_of([1.2, 1.6]),
          noise_seed <- integer(1..1_000),
          amplitude <- float(min: 0.0, max: 30.0)
        ) do
      step = step_min * 60_000
      n = round(days * @day / step)
      noise = :rand.seed_s(:exsss, noise_seed)

      {list, _} =
        Enum.map_reduce(1..count, noise, fn i, rng ->
          {samples, rng} =
            Enum.map_reduce(0..(n - 1), rng, fn j, rng ->
              {u, rng} = :rand.uniform_s(rng)
              t = @base + j * step
              v = 100.0 + amplitude * :math.sin(2 * :math.pi() * rem(t, @day) / @day + i) + u
              {{t, Float.round(v, 3)}, rng}
            end)

          {Series.new("m#{i}", %{"host" => "h"}, samples), rng}
        end)

      list
    end
  end

  property "two replays of the same input and seed are identical" do
    check all(series <- series_gen(), seed <- integer(1..1_000_000), max_runs: 100) do
      first = AutoangelReplay.replay(series, seed)
      assert {:ok, %{labels: labels, report: report}} = first
      assert labels != []
      assert report.seed == seed
      assert AutoangelReplay.replay(series, seed) == first
    end
  end
end

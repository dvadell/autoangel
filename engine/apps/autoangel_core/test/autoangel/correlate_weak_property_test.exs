defmodule Autoangel.CorrelateWeakPropertyTest do
  @moduledoc """
  Property 25: weak evidence marking (Req 19.1, 19.2, 19.3, 20.4).
  A Candidate is weak if and only if the selected pool exceeds the pool limit
  and its support is below the support threshold, event-like or not.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Autoangel.Correlate

  @window 1_000
  @final 100_000_000
  @target {"target", [{"host", "h"}]}

  property "pure marking: weak iff n > pool_limit and support < threshold" do
    check all(
            supports <- list_of(integer(0..6), max_length: 15),
            pool_limit <- integer(1..15),
            threshold <- integer(0..6),
            event_flags <- list_of(boolean(), length: 15),
            max_runs: 200
          ) do
      cands =
        supports
        |> Enum.zip(event_flags)
        |> Enum.with_index(1)
        |> Enum.map(fn {{support, event_like?}, rank} ->
          %{
            series_key: {"s#{rank}", []},
            rank: rank,
            evidence: %{support: support, event_like?: event_like?, weak?: false}
          }
        end)

      n = length(cands)

      marked =
        Correlate.mark_weak(cands, n, %{pool_limit: pool_limit, support_threshold: threshold})

      assert length(marked) == n

      for {m, c} <- Enum.zip(marked, cands) do
        expected = n > pool_limit and c.evidence.support < threshold
        assert m.weak? == expected
        assert m.evidence.weak? == expected
        assert m.rank == c.rank and m.series_key == c.series_key
      end

      if n <= pool_limit, do: refute(Enum.any?(marked, & &1.weak?))
      for m <- marked, m.evidence.support >= threshold, do: refute(m.weak?)
    end
  end

  property "end to end through the Correlator, with real support history" do
    check all(
            n <- integer(1..6),
            priors <- list_of(integer(0..3), length: 6),
            pool_limit <- integer(1..6),
            threshold <- integer(0..3),
            event_like? <- boolean(),
            max_runs: 40
          ) do
      {:ok, pid} =
        Correlate.start_link(
          name: nil,
          dispatcher: nil,
          window_ms: @window,
          pool_limit: pool_limit,
          support_threshold: threshold,
          now: fn -> 0 end
        )

      names = for i <- 1..n, do: "c#{i}"
      priors = Enum.take(priors, n)

      # Prior co-occurrence rounds, each far from the final window and from
      # each other, so support for candidate i is exactly priors[i].
      for {{name, count}, idx} <- Enum.with_index(Enum.zip(names, priors)), r <- 1..count//1 do
        s = (idx * 100 + r) * 1_000_000

        Correlate.correlate(pid, %{
          id: "#{name}-h#{r}",
          series_key: {name, []},
          start_ms: s,
          score: 5.0
        })

        Correlate.correlate(pid, %{
          id: "t#{name}-h#{r}",
          series_key: @target,
          start_ms: s + 1,
          score: 5.0
        })
      end

      for name <- names do
        Correlate.correlate(pid, %{
          id: "f-#{name}",
          series_key: {name, []},
          start_ms: @final,
          score: 5.0
        })
      end

      result =
        Correlate.correlate(pid, %{
          id: "f-t",
          series_key: @target,
          start_ms: @final,
          score: 5.0,
          event_like?: event_like?
        })

      assert length(result) == n
      support_by_name = Map.new(Enum.zip(names, priors))

      for c <- result do
        support = Map.fetch!(support_by_name, elem(c.series_key, 0))
        assert c.evidence.support == support
        assert c.weak? == (n > pool_limit and support < threshold)
        assert c.evidence.weak? == c.weak?
        assert c.evidence.event_like? == event_like?
      end

      GenServer.stop(pid)
    end
  end
end

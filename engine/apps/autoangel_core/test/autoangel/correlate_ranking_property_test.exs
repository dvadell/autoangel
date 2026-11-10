defmodule Autoangel.CorrelateRankingPropertyTest do
  @moduledoc """
  Property 24: ranking is a permutation with one-factor dominance
  (Req 18.4, 18.5, 20.4). Dominance scenarios run through the real Correlator
  with two Candidates that are equal in every ranking factor but one.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Autoangel.Correlate

  @window 1_000
  @final 100_000_000
  @target {"target", [{"host", "h"}]}

  defp cand_key(name, true), do: {name, [{"host", "h"}]}
  defp cand_key(name, false), do: {name, [{"host", "other"}]}

  # Builds prior co-occurrence history of `count` rounds between `name` and the
  # target, each round far outside the final window.
  defp history(pid, name, shared?, count, base_round) do
    for r <- 1..count//1 do
      s = (base_round + r) * 1_000_000

      Correlate.correlate(pid, %{
        id: "#{name}-h#{r}",
        series_key: cand_key(name, shared?),
        start_ms: s,
        score: 50.0
      })

      Correlate.correlate(pid, %{
        id: "t-#{name}-h#{r}",
        series_key: @target,
        start_ms: s + 1,
        score: 50.0
      })
    end
  end

  # Spec: %{offset, score, shared?, prior}. Returns candidates in rank order.
  defp rank_pair(a, b) do
    {:ok, pid} =
      Correlate.start_link(name: nil, dispatcher: nil, window_ms: @window, now: fn -> 0 end)

    history(pid, "a", a.shared?, a.prior, 0)
    history(pid, "b", b.shared?, b.prior, 100)

    for {name, spec} <- [{"a", a}, {"b", b}] do
      Correlate.correlate(pid, %{
        id: "final-#{name}",
        series_key: cand_key(name, spec.shared?),
        start_ms: @final + spec.offset,
        score: spec.score
      })
    end

    result =
      Correlate.correlate(pid, %{id: "final-t", series_key: @target, start_ms: @final, score: 50.0})

    GenServer.stop(pid)
    result
  end

  defp base, do: %{offset: 100, score: 50.0, shared?: true, prior: 0}

  defp assert_b_better(a, b) do
    [first, second] = rank_pair(a, b)
    assert first.rank == 1 and second.rank == 2
    assert elem(first.series_key, 0) == "b", "the better candidate must have the lower rank"
  end

  property "closer start ranks higher" do
    check all(d1 <- integer(1..900), gap <- integer(1..99), max_runs: 40) do
      assert_b_better(%{base() | offset: d1 + gap}, %{base() | offset: d1})
    end
  end

  property "started before ranks higher (equal distance)" do
    check all(d <- integer(1..1_000), max_runs: 40) do
      assert_b_better(%{base() | offset: d}, %{base() | offset: -d})
    end
  end

  property "higher score ranks higher" do
    check all(s1 <- integer(0..99), gap <- integer(1..100), max_runs: 40) do
      assert_b_better(%{base() | score: s1 * 1.0}, %{base() | score: (s1 + gap) * 1.0})
    end
  end

  property "more shared host/job/service labels ranks higher" do
    check all(_ <- constant(:ok), max_runs: 5) do
      assert_b_better(%{base() | shared?: false}, %{base() | shared?: true})
    end
  end

  property "more historical co-occurrence ranks higher" do
    check all(k1 <- integer(0..4), gap <- integer(1..3), max_runs: 15) do
      assert_b_better(%{base() | prior: k1}, %{base() | prior: k1 + gap})
    end
  end

  property "ranks are a permutation of 1..n and follow the documented order" do
    check all(
            specs <-
              list_of(
                tuple(
                  {integer(0..4), integer(0..4), integer(0..3), integer(0..2),
                   member_of([0.0, 1.0])}
                ),
                max_length: 20
              ),
            max_runs: 100
          ) do
      cands =
        specs
        |> Enum.with_index()
        |> Enum.map(fn {{s, p, sh, h, bf}, i} ->
          factors = {p / 4, bf, s / 4, sh / 3, h / 5}
          {prox, before, mag, shared, hist} = factors

          %{
            series_id: <<i::32>>,
            factors: factors,
            strength: 0.35 * prox + 0.15 * before + 0.2 * mag + 0.15 * shared + 0.15 * hist
          }
        end)

      ranked = Correlate.rank(cands)
      assert Enum.map(ranked, & &1.rank) == Enum.to_list(1..length(cands)//1)
      assert ranked |> Enum.map(&Map.delete(&1, :rank)) |> Enum.sort() == Enum.sort(cands)

      keys =
        Enum.map(
          ranked,
          &{-&1.strength, Enum.map(Tuple.to_list(&1.factors), fn x -> -x end), &1.series_id}
        )

      assert keys == Enum.sort(keys)
      # Deterministic: input order does not change the result.
      assert Correlate.rank(Enum.reverse(cands)) == ranked
    end
  end
end

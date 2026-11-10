defmodule Autoangel.CorrelateSelectionPropertyTest do
  @moduledoc """
  Property 23: Candidate selection invariant (Req 18.1, 18.3, 18.7, 18.8, 20.4).

  The oracle is an independent model: the expected Candidate Series are exactly
  the non-target Series with at least one event whose start lies in the
  inclusive window around the target start.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Autoangel.Correlate

  @window 1_000

  defp key(i), do: {"m#{i}", [{"host", "h#{rem(i, 2)}"}]}

  defp event_gen do
    gen all(
          series <- integer(0..5),
          offset <-
            frequency([
              {3, integer(-2_500..2_500)},
              {2,
               member_of([-@window - 1, -@window, -@window + 1, @window - 1, @window, @window + 1])}
            ]),
          score <- integer(0..100),
          event_like? <- boolean()
        ) do
      %{series: series, offset: offset, score: score * 1.0, event_like?: event_like?}
    end
  end

  defp start!,
    do:
      elem(
        Correlate.start_link(name: nil, dispatcher: nil, window_ms: @window, now: fn -> 0 end),
        1
      )

  defp feed(pid, events, target_start) do
    events
    |> Enum.with_index()
    |> Enum.each(fn {e, i} ->
      Correlate.correlate(pid, %{
        id: "e#{i}",
        series_key: key(e.series),
        start_ms: target_start + e.offset,
        score: e.score,
        event_like?: e.event_like?
      })
    end)
  end

  property "one list with exactly the other in-window series, each once" do
    check all(
            events <- list_of(event_gen(), max_length: 25),
            target <- integer(0..5),
            max_runs: 150
          ) do
      pid = start!()
      t = 100_000
      feed(pid, events, t)

      candidates =
        Correlate.correlate(pid, %{id: "target", series_key: key(target), start_ms: t, score: 50.0})

      expected =
        events
        |> Enum.filter(&(&1.series != target and abs(&1.offset) <= @window))
        |> Enum.map(&key(&1.series))
        |> Enum.uniq()
        |> Enum.sort()

      got = candidates |> Enum.map(& &1.series_key) |> Enum.sort()

      assert got == expected
      assert got == Enum.uniq(got)
      refute key(target) in got

      assert Enum.map(candidates, & &1.rank) |> Enum.sort() ==
               Enum.to_list(1..length(candidates)//1)
    end
  end

  property "pure select_candidates keeps the closest event per series (ties: earlier, higher score)" do
    check all(
            events <- list_of(event_gen(), max_length: 25),
            target <- integer(0..5),
            max_runs: 150
          ) do
      t = 50_000

      evs =
        for e <- events,
            do: %{
              series_key: key(e.series),
              start_ms: t + e.offset,
              score: e.score,
              event_like?: false
            }

      selected = Correlate.select_candidates(evs, key(target), t, @window)
      keys = Enum.map(selected, & &1.series_key)
      assert keys == Enum.uniq(keys)

      for s <- selected do
        assert abs(s.start_ms - t) <= @window

        same =
          Enum.filter(evs, &(&1.series_key == s.series_key and abs(&1.start_ms - t) <= @window))

        best = Enum.min_by(same, &{abs(&1.start_ms - t), &1.start_ms, -&1.score})

        assert {abs(s.start_ms - t), s.start_ms, s.score} ==
                 {abs(best.start_ms - t), best.start_ms, best.score}
      end
    end
  end

  property "an event stays selectable for one window after its start (Req 18.8)" do
    check all(
            delta <- integer(0..@window),
            target_event_like? <- boolean(),
            max_runs: 60
          ) do
      start = 10_000
      # The clock tracks the target's arrival, so pruning really runs at
      # now - 2*window and must not drop an event younger than that.
      {:ok, pid} =
        Correlate.start_link(
          name: nil,
          dispatcher: nil,
          window_ms: @window,
          now: fn -> start + delta end
        )

      Correlate.correlate(pid, %{id: "a", series_key: key(1), start_ms: start, score: 10.0})

      got =
        Correlate.correlate(pid, %{
          id: "t",
          series_key: key(2),
          start_ms: start + delta,
          score: 10.0,
          event_like?: target_event_like?
        })

      assert Enum.map(got, & &1.series_key) == [key(1)]
      GenServer.stop(pid)
    end
  end
end

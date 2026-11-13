defmodule Autoangel.Detect.SeasonalityTest do
  @moduledoc "Task 33.3 and 33.4: seasonality classification, cells and per-phase scoring."
  use ExUnit.Case, async: true

  alias Autoangel.Detect.Decision.Meta
  alias Autoangel.Detect.Seasonality
  alias Autoangel.Kernel.{Params, Scoring, State}
  alias Autoangel.Test.HistoryBackend
  alias Autoangel.Workers.HistoryOps

  @hour 3_600_000
  # A Monday 00:00 UTC, so `rem(div(hour, 24), 7)` is the weekday index.
  @base_hour 20_000 * 24 - rem(20_000 * 24, 168)

  defp hours(count, fun), do: for(h <- 0..(count - 1), do: {@base_hour + h, fun.(h)})

  defp noise(seed, count) do
    :rand.seed(:exsss, {seed, seed, seed})
    for _ <- 1..count, do: :rand.uniform()
  end

  describe "classification" do
    test "a pure daily sine is daily" do
      series = hours(24 * 5, &(10.0 + 5.0 * :math.sin(2 * :math.pi() * &1 / 24)))
      assert {:ok, 24, acf} = Seasonality.classify(series)
      assert acf > 0.9
    end

    test "a weekly pattern is weekly" do
      weekday = fn h -> rem(div(h, 24), 7) end

      series =
        hours(168 * 3, fn h ->
          if weekday.(h) in [5, 6], do: 2.0, else: 10.0 + :math.sin(2 * :math.pi() * h / 24)
        end)

      assert {:ok, 168, acf} = Seasonality.classify(series)
      assert acf > 0.8
    end

    test "noise alone has no period" do
      series =
        Enum.zip_with(0..(24 * 14 - 1), noise(1, 24 * 14), fn h, v -> {@base_hour + h, v} end)

      assert Seasonality.classify(series) == :none
    end

    test "less than two cycles of history cannot be significant" do
      assert Seasonality.classify(hours(47, &(10.0 + :math.sin(2 * :math.pi() * &1 / 24)))) == :none
      assert Seasonality.acf(hours(47, &(1.0 * &1)), 24) == nil
      series = hours(24 * 6, &(10.0 + :math.sin(2 * :math.pi() * &1 / 24)))
      assert Seasonality.acf(series, 168) == nil
    end

    test "a flat series has no period, and so does an empty one" do
      assert Seasonality.classify(hours(24 * 5, fn _ -> 5.0 end)) == :none
      assert Seasonality.classify([]) == :none
    end

    test "the threshold decides" do
      series = hours(24 * 5, &(10.0 + 5.0 * :math.sin(2 * :math.pi() * &1 / 24)))
      assert {:ok, 24, _} = Seasonality.classify(series, threshold: 0.99)
      noisy = Enum.zip_with(series, noise(2, length(series)), fn {h, v}, n -> {h, v + 50 * n} end)
      assert Seasonality.classify(noisy, threshold: 0.99) == :none
    end

    test "the threshold must be above 0 and at most 1" do
      assert Seasonality.valid_threshold?(0.3)
      assert Seasonality.valid_threshold?(1)
      refute Seasonality.valid_threshold?(0)
      refute Seasonality.valid_threshold?(1.5)
      refute Seasonality.valid_threshold?("0.3")
    end
  end

  describe "cells and Baselines" do
    setup do
      series = hours(24 * 5, &(100.0 + 50.0 * :math.sin(2 * :math.pi() * &1 / 24)))
      {:install, 24, cells} = Seasonality.analyse(series)
      {:ok, series: series, cells: cells}
    end

    test "one cell per phase, seeded from the hourly means", %{cells: cells} do
      assert length(cells) == 24
      assert Enum.all?(cells, &(&1.n == 5))
      phase = Integer.mod(@base_hour + 6, 24)
      assert_in_delta Enum.at(cells, phase).med, 150.0, 1.0e-6
    end

    test "scoring uses the cell of the Sample's phase", %{cells: cells} do
      state = State.encode(State.new(kind: :gauge))
      {:ok, state} = Seasonality.install(state, 24, cells)
      assert Seasonality.period(state) == 24

      ts = (@base_hour + 6 + 24 * 10) * @hour
      params = %Params{}

      # The robust window is empty, so give it a few quiet Samples at the same level.
      warm = for i <- 1..20, do: {ts - i * 60_000, 150.0 + rem(i, 3) * 0.1, :gauge}
      {:ok, state, _} = Scoring.Elixir.score(state, Enum.reverse(warm), params)

      {:ok, state, [normal]} = Scoring.Elixir.score(state, [{ts, 150.2, :gauge}], params)
      assert normal.baseline_type == :seasonal_daily
      assert normal.score < 50

      {:ok, _state, [high]} = Scoring.Elixir.score(state, [{ts + 60_000, 400.0, :gauge}], params)
      assert high.baseline_type == :seasonal_daily
      assert high.score > 90
    end

    test "a weekly period reports a weekly Baseline" do
      series = hours(168 * 3, &if(rem(div(&1, 24), 7) in [5, 6], do: 2.0, else: 10.0))
      {:install, 168, cells} = Seasonality.analyse(series)
      assert length(cells) == 168
      state = State.encode(State.new(kind: :gauge))
      {:ok, state} = Seasonality.install(state, 168, cells)
      ts = (@base_hour + 168 * 5 + 30) * @hour
      {:ok, _, [result]} = Scoring.Elixir.score(state, [{ts, 10.0, :gauge}], %Params{})
      assert result.baseline_type == :seasonal_weekly
    end

    test "no significant period clears the cells and falls back to robust" do
      noisy = Enum.zip_with(0..(24 * 10 - 1), noise(3, 24 * 10), fn h, v -> {@base_hour + h, v} end)
      assert Seasonality.analyse(noisy) == :clear

      state = State.encode(State.new(kind: :gauge))

      {:ok, state} =
        Seasonality.install(state, 24, for(_ <- 1..24, do: %{med: 1.0, mad: 1.0, n: 5}))

      assert Seasonality.period(state) == 24
      {:ok, cleared} = Seasonality.clear(state)
      assert Seasonality.period(cleared) == nil

      {:ok, _, [result]} =
        Scoring.Elixir.score(cleared, [{@base_hour * @hour, 1.0, :gauge}], %Params{})

      assert result.baseline_type == :robust
    end

    test "cells survive the state encoding and keep learning from live Samples", %{cells: cells} do
      state = State.encode(State.new(kind: :gauge))
      {:ok, state} = Seasonality.install(state, 24, cells)
      ts = (@base_hour + 3 + 24 * 9) * @hour
      {:ok, state, _} = Scoring.Elixir.score(state, [{ts, 120.0, :gauge}], %Params{})
      {:ok, decoded} = State.decode(state)
      cell = Enum.at(decoded.seasonal.cells, Integer.mod(@base_hour + 3, 24))
      assert cell.n == 6
    end
  end

  describe "applying a decision to a Series" do
    test "install records the period for learning; clear removes it" do
      entry = %{
        kernel_state: State.encode(State.new(kind: :gauge)),
        meta: %Meta{}
      }

      cells = for _ <- 1..24, do: %{med: 1.0, mad: 1.0, n: 3}
      installed = HistoryOps.apply_decision(entry, {:install, 24, cells})
      assert installed.meta.seasonal == {:seasonal_daily, 24 * @hour}
      assert Seasonality.period(installed.kernel_state) == 24

      cleared = HistoryOps.apply_decision(installed, :clear)
      assert cleared.meta.seasonal == nil
      assert Seasonality.period(cleared.kernel_state) == nil
    end

    test "a state that cannot be decoded is left alone" do
      entry = %{kernel_state: "garbage", meta: %Meta{}}
      assert HistoryOps.apply_decision(entry, :clear) == entry

      assert HistoryOps.apply_decision(
               entry,
               {:install, 24, List.duplicate(%{med: 0.0, mad: 0.0, n: 0}, 24)}
             ) == entry
    end
  end

  describe "background analysis" do
    setup do
      on_exit(&HistoryBackend.clear/0)
    end

    test "decides per Series from hourly aggregates" do
      daily =
        for h <- 0..(24 * 5 - 1),
            do:
              {<<1::128>>, (@base_hour + h) * @hour, 0, 0,
               10 + 5.0 * :math.sin(2 * :math.pi() * h / 24), 60}

      flat = for h <- 0..(24 * 5 - 1), do: {<<2::128>>, (@base_hour + h) * @hour, 0, 0, 5.0, 60}
      HistoryBackend.put_rows(daily ++ flat)
      now = (@base_hour + 24 * 5) * @hour

      assert {:ok, decisions} =
               HistoryOps.analyse_seasonality(%{<<1::128>> => :sine, <<2::128>> => :flat}, now,
                 backend: HistoryBackend,
                 handle: self()
               )

      assert {:install, 24, _cells} = decisions.sine
      assert decisions.flat == :clear
      assert_received {:read, :hour, _ids, _from, ^now}
    end

    test "a read failure keeps every Baseline" do
      HistoryBackend.put_rows({:error, :down})

      assert {:error, :down} =
               HistoryOps.analyse_seasonality(%{<<1::128>> => :a}, 10 * @hour,
                 backend: HistoryBackend,
                 handle: self()
               )
    end
  end
end

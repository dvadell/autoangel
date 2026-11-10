defmodule Autoangel.Alert.RenderPropertyTest do
  @moduledoc """
  Property 32: alert rendering is complete and uses neutral wording
  (Req 17.4, 17.5, 17.7, 19.4, 19.5, 19.7, 26.1-26.5, 32.6).

  Metric names, label values and candidate names are drawn from an alphabet that
  cannot spell a banned word, so any banned word found in the rendered text comes
  from the template itself.

  Scope note: resolution notifications and grouped-Incident members (P1 tasks
  36.3/36.4) and Control_API alert JSON are not implemented yet; this property
  must be extended to cover them when they are.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Autoangel.Alert.Render
  alias Autoangel.SelfMonitor

  @banned ~r/cause|probability|likelihood|chance|%/i
  @alphabet [?a..?f, ?x, ?0..?9, ?_]

  defp word(min \\ 1, max \\ 8), do: string(@alphabet, min_length: min, max_length: max)

  defp anomaly_gen do
    gen all(
          name <- word(),
          labels <- uniq_list_of({word(1, 5), word(0, 8)}, uniq_fun: &elem(&1, 0), max_length: 5),
          observed <- one_of([float(min: -1.0e6, max: 1.0e6), integer(-1000..1000)]),
          lower <- float(min: -1.0e6, max: 1.0e6),
          upper <- float(min: -1.0e6, max: 1.0e6),
          score <- one_of([float(min: -50.0, max: 150.0), integer(-20..120)]),
          start_ms <- integer(0..4_102_444_800_000),
          id <- word(8, 16),
          baseline <- member_of([:short_term, :long_term, :seasonal])
        ) do
      %{
        id: id,
        series_key: {name, Enum.sort(labels)},
        start_ms: start_ms,
        score: score,
        observed: observed,
        lower: lower,
        upper: upper,
        baseline_type: baseline
      }
    end
  end

  defp candidates_gen do
    gen all(flags <- list_of(boolean(), max_length: 12), names <- list_of(word(), length: 12)) do
      flags
      |> Enum.with_index(1)
      |> Enum.zip(names)
      |> Enum.map(fn {{weak?, rank}, name} ->
        %{
          series_key: {"c#{rank}_#{name}", [{"host", "h#{rank}"}]},
          rank: rank,
          weak?: weak?,
          evidence: %{}
        }
      end)
      |> Enum.shuffle()
    end
  end

  property "every required field appears with neutral wording" do
    check all(
            anomaly <- anomaly_gen(),
            candidates <- candidates_gen(),
            base <- member_of([nil, "https://c.example", "http://c.example:8080/"]),
            max <- integer(1..8),
            max_runs: 200
          ) do
      alert = Render.build(anomaly, candidates, console_base_url: base, max_candidates: max)
      text = Render.text(alert)
      {name, labels} = anomaly.series_key

      # 26.1: name, every label, observed, both bounds, baseline type.
      assert text =~ "metric: #{name}\n"
      for {k, v} <- labels, do: assert(text =~ "#{k}=#{v}")
      assert text =~ "observed value: #{anomaly.observed}\n"
      assert text =~ "expected range lower: #{anomaly.lower}\n"
      assert text =~ "expected range upper: #{anomaly.upper}\n"
      assert text =~ "baseline type: #{anomaly.baseline_type}\n"

      # Score: labelled, 0-100, integer or one decimal, no percent sign.
      [_, shown] = Regex.run(~r/anomaly score: (\S+)/, text)
      assert shown =~ ~r/\A\d{1,3}(\.\d)?\z/
      {shown_value, ""} = Float.parse(shown)
      assert shown_value >= 0 and shown_value <= 100
      assert abs(shown_value - (anomaly.score |> max(0) |> min(100))) <= 0.0500001
      refute shown =~ ".0"

      # Start time: date, time to the second, explicit zone.
      expected_iso = anomaly.start_ms |> div(1000) |> DateTime.from_unix!() |> DateTime.to_iso8601()
      assert text =~ "start: #{expected_iso}\n"

      # 26.2 / 26.5: link exactly when configured.
      if base do
        link = String.trim_trailing(base, "/") <> "/anomalies/" <> anomaly.id
        assert text =~ "console: #{link}"
      else
        refute text =~ "console:"
      end

      # 26.3 / 26.4 / 19.4 / 19.5: candidates.
      shown_ranks = candidates |> Enum.map(& &1.rank) |> Enum.sort() |> Enum.take(max)

      if candidates == [] do
        assert text =~ "no candidates found inside the Correlation_Window"
        refute text =~ "candidates:"
      else
        assert text =~ "candidates:\n"
        refute text =~ "no candidates found"
        lines = text |> String.split("\n") |> Enum.drop_while(&(&1 != "candidates:")) |> tl()

        assert Enum.map(lines, &(&1 |> String.split(".", parts: 2) |> hd() |> String.to_integer())) ==
                 shown_ranks

        for {line, rank} <- Enum.zip(lines, shown_ranks) do
          c = Enum.find(candidates, &(&1.rank == rank))
          assert line =~ ~r/^#{rank}\. #{elem(c.series_key, 0)} \[host=h#{rank}\]/
          assert String.ends_with?(line, "(weak evidence)") == c.weak?
        end
      end

      # 17.7 / 19.7: neutral wording only.
      refute text =~ @banned
    end
  end

  defmodule Collector do
    @moduledoc false
    use GenServer
    def start_link(test), do: GenServer.start_link(__MODULE__, test)
    @impl true
    def init(test), do: {:ok, test}
    @impl true
    def handle_call({:enqueue, alert}, _from, test) do
      send(test, {:alert, alert})
      {:reply, :ok, test}
    end
  end

  property "self-monitoring alerts state they concern the Engine and use neutral wording" do
    check all(scenario <- member_of([:no_ingest, :plugin_failure]), max_runs: 20) do
      {:ok, collector} = Collector.start_link(self())
      {:ok, agent} = Agent.start_link(fn -> 0 end)
      clock = fn -> Agent.get_and_update(agent, fn v -> {1_000_000_000 + v, v + 1} end) end

      {:ok, monitor} =
        SelfMonitor.start_link(
          name: nil,
          plugins: ["email", "telegram"],
          runners: %{"email" => collector, "telegram" => collector},
          clock: clock,
          plugin_failure_count: 1,
          no_ingest_ms: 1
        )

      case scenario do
        :no_ingest ->
          send(monitor, :tick)

        :plugin_failure ->
          # The failure threshold must be exceeded, as in the recipients suite.
          for _ <- 1..2 do
            send(monitor, {:plugin_failure, "email"})
            SelfMonitor.state(monitor)
          end
      end

      SelfMonitor.state(monitor)
      assert_receive {:alert, alert}, 1_000

      text = Render.text(alert)
      assert text =~ "concerns the Engine itself"
      assert text =~ ~r/\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(\.\d+)?(Z|[+-]\d\d:\d\d)/

      case scenario do
        :no_ingest -> assert text =~ ~r/no.ingest/
        :plugin_failure -> assert text =~ ~r/plugin.failure/ and text =~ "email"
      end

      refute text =~ @banned

      GenServer.stop(monitor)
      GenServer.stop(collector)
      Agent.stop(agent)
      flush_alerts()
    end
  end

  # Both plugins share one collector, so an alert may arrive twice; drain any
  # leftovers so they cannot leak into the next generated run.
  defp flush_alerts do
    receive do
      {:alert, _} -> flush_alerts()
    after
      0 -> :ok
    end
  end
end

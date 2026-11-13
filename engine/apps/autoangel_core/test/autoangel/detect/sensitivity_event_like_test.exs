defmodule Autoangel.Detect.SensitivityEventLikeTest do
  @moduledoc """
  Tasks 36.1, 36.2 and 36.6: sensitivity precedence and restore order, and
  event-like classification with its wording.
  """
  use ExUnit.Case, async: true

  alias Autoangel.Alert.Render
  alias Autoangel.Detect.Decision
  alias Autoangel.Detect.Decision.Meta
  alias Autoangel.Kernel.{Params, State}
  alias Autoangel.Kernel.Scoring.Elixir, as: ScoringElixir
  alias Autoangel.{SeriesRegistry, StateStore}
  alias Autoangel.Workers.Worker

  @key {"errors_total", []}

  defp scored(score, extra \\ %{}) do
    Map.merge(
      %{
        ts: 0,
        score: score,
        observed: 10.0,
        lower: 0.0,
        upper: 2.0,
        baseline_type: :robust,
        kind: :gauge,
        recv_ms: 0
      },
      extra
    )
  end

  # Three Samples in a row at `score`: an Anomaly opens when the score clears the
  # effective threshold (persistence 3).
  defp opens?(meta, cfg, score) do
    cfg = Map.merge(%{learning: %{duration: 0}, persistence: %{n: 3, m: 5}}, cfg)

    {_meta, events} =
      Enum.reduce(0..2, {meta, []}, fn i, {m, acc} ->
        {m, ev} =
          Decision.step(m, scored(score, %{ts: i * 1_000, recv_ms: i * 1_000}), i * 1_000, cfg)

        {m, acc ++ ev}
      end)

    Enum.any?(events, &match?({:anomaly_started, _}, &1))
  end

  describe "sensitivity precedence" do
    test "the default threshold is 75" do
      refute opens?(%Meta{key: @key}, %{}, 70.0)
      assert opens?(%Meta{key: @key}, %{}, 80.0)
    end

    test "the global setting replaces the default" do
      assert opens?(%Meta{key: @key}, %{sensitivity: 2.0}, 60.0)
      refute opens?(%Meta{key: @key}, %{sensitivity: 0.5}, 80.0)
    end

    test "a per-Series override beats the global setting, both ways" do
      assert opens?(%Meta{key: @key, sensitivity_override: 4.0}, %{sensitivity: 0.5}, 40.0)
      refute opens?(%Meta{key: @key, sensitivity_override: 0.25}, %{sensitivity: 4.0}, 90.0)
    end

    test "the limits 0.25 and 4.0 give thresholds of 93.75 and 1.0" do
      refute opens?(%Meta{key: @key, sensitivity_override: 0.25}, %{}, 93.0)
      assert opens?(%Meta{key: @key, sensitivity_override: 0.25}, %{}, 94.0)
      assert opens?(%Meta{key: @key, sensitivity_override: 4.0}, %{}, 2.0)
    end
  end

  describe "in a Worker" do
    setup do
      dir = Path.join(System.tmp_dir!(), "aa-sens-#{System.unique_integer([:positive])}")
      {:ok, store} = StateStore.start_link(name: nil, path: dir)
      on_exit(fn -> File.rm_rf(dir) end)
      {:ok, store: store}
    end

    defp start_worker(opts) do
      index = System.unique_integer([:positive])

      {:ok, pid} =
        Worker.start_link(
          Keyword.merge(
            [
              index: index,
              name: nil,
              kernel_params: %Params{},
              sweep_interval: 3_600_000,
              queue_counter: :atomics.new(1, signed: true),
              dispatcher: nil,
              correlator: nil
            ],
            opts
          )
        )

      on_exit(fn ->
        :persistent_term.erase({Worker, index})
        :persistent_term.erase({Worker, :capacity, index})
      end)

      pid
    end

    defp first_sample(pid) do
      Worker.enqueue(pid, [{@key, 1.0, 1_000, :gauge}])
      Worker.series(pid, @key).meta
    end

    test "configured per-Series settings apply to the matching metric only" do
      overrides = [%{name: "errors_total", sensitivity: 3.0, direction: "high"}]
      pid = start_worker(decision_cfg: %{series_overrides: overrides})
      meta = first_sample(pid)
      assert meta.sensitivity_override == 3.0
      assert meta.direction_override == :high

      Worker.enqueue(pid, [{{"other_metric", []}, 1.0, 1_000, :gauge}])
      other = Worker.series(pid, {"other_metric", []}).meta
      assert other.sensitivity_override == nil
      assert other.direction_override == nil
    end

    test "a setting that names only a direction leaves sensitivity unset" do
      pid =
        start_worker(decision_cfg: %{series_overrides: [%{name: "errors_total", direction: "low"}]})

      meta = first_sample(pid)
      assert meta.direction_override == :low
      assert meta.sensitivity_override == nil
    end

    test "an override saved by feedback is restored before the first score and beats the configured one",
         %{store: store} do
      :ok = StateStore.put_sensitivity(store, SeriesRegistry.series_id(@key), 0.5)
      overrides = [%{name: "errors_total", sensitivity: 3.0}]
      pid = start_worker(state_store: store, decision_cfg: %{series_overrides: overrides})
      assert first_sample(pid).sensitivity_override == 0.5
    end
  end

  describe "event-like classification" do
    defp classify(result) do
      {meta, _} =
        Decision.step(%Meta{key: @key}, scored(10.0, result), 0, %{learning: %{duration: 1}})

      meta.event_like?
    end

    test "a Typical_Interval of 10 minutes or more is event-like" do
      assert classify(%{typical_interval_ms: 600_000})
      refute classify(%{typical_interval_ms: 599_999})
    end

    test "irregular spacing (coefficient of variation above 1.0) is event-like" do
      assert classify(%{typical_interval_ms: 1_000, spacing_cv: 1.01})
      refute classify(%{typical_interval_ms: 1_000, spacing_cv: 1.0})
      refute classify(%{typical_interval_ms: 1_000, spacing_cv: nil})
    end

    test "the flag is carried by the Anomaly" do
      meta = %Meta{key: @key, event_like?: false}
      cfg = %{learning: %{duration: 0}, persistence: %{n: 1, m: 5}}

      {_meta, events} =
        Decision.step(meta, scored(95.0, %{typical_interval_ms: 900_000}), 0, cfg)

      assert [{:anomaly_started, %{event_like?: true}} | _] =
               Enum.filter(events, &match?({:anomaly_started, _}, &1))
    end

    test "the kernel reports the spacing variation" do
      samples = for ts <- [0, 1_000, 2_000, 3_000, 100_000, 100_500], do: {ts, 1.0, :gauge}

      {:ok, _state, results} =
        ScoringElixir.score(
          State.encode(State.new()),
          samples,
          %Params{}
        )

      assert Enum.at(results, 1).spacing_cv == nil
      assert Enum.at(results, 5).spacing_cv > 1.0

      regular = for i <- 0..9, do: {i * 1_000, 1.0, :gauge}

      {:ok, _, results} =
        ScoringElixir.score(
          State.encode(State.new()),
          regular,
          %Params{}
        )

      assert List.last(results).spacing_cv == 0.0
    end

    test "event-like evidence is described as co-occurrence inside the window" do
      candidate = fn event_like? ->
        %{
          series_key: {"deploys", [{"host", "a"}]},
          rank: 1,
          weak?: false,
          evidence: %{event_like?: event_like?}
        }
      end

      alert = fn candidate ->
        Render.build(%{id: "a1", series_key: @key, start_ms: 0}, [candidate], [])
      end

      assert Render.text(alert.(candidate.(true))) =~
               "1. deploys [host=a] (co-occurrence inside the window)"

      refute Render.text(alert.(candidate.(false))) =~ "co-occurrence"
    end
  end
end

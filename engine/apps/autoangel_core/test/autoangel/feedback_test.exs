defmodule Autoangel.FeedbackTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Autoangel.Alert.{Dispatcher, Policy}
  alias Autoangel.Detect.Decision
  alias Autoangel.{Feedback, SeriesRegistry, StateStore}
  alias Autoangel.Kernel.State
  alias Autoangel.Workers.Worker

  @series %{name: "latency", labels: %{"host" => "h1"}}
  @key SeriesRegistry.canonical_key("latency", %{"host" => "h1"})
  @other SeriesRegistry.canonical_key("errors", %{"host" => "h1"})

  # A State_Store stand-in that fails every write while `fail` is set.
  defmodule FlakyStore do
    @moduledoc false
    use GenServer
    def start_link(real), do: GenServer.start_link(__MODULE__, real)
    def fail(pid, value), do: GenServer.call(pid, {:fail, value})
    # Lets `n` more puts succeed, then fails every later one.
    def fail_after(pid, n), do: GenServer.call(pid, {:fail_after, n})
    @impl true
    def init(real), do: {:ok, %{real: real, fail: false, allow: nil}}
    @impl true
    def handle_call({:fail, v}, _from, s), do: {:reply, :ok, %{s | fail: v}}
    def handle_call({:fail_after, n}, _from, s), do: {:reply, :ok, %{s | allow: n}}

    def handle_call(msg, _from, %{fail: true} = s) when elem(msg, 0) in [:put, :put_many],
      do: {:reply, {:error, :disk_full}, s}

    def handle_call(msg, _from, %{allow: n} = s)
        when elem(msg, 0) in [:put, :put_many] and is_integer(n) do
      if n > 0,
        do: {:reply, GenServer.call(s.real, msg), %{s | allow: n - 1}},
        else: {:reply, {:error, :disk_full}, s}
    end

    def handle_call(msg, _from, s), do: {:reply, GenServer.call(s.real, msg), s}
  end

  defp tmp_store do
    dir = Path.join(System.tmp_dir!(), "aa-fb-#{System.unique_integer([:positive])}")
    on_exit(fn -> File.rm_rf!(dir) end)
    {:ok, store} = StateStore.start_link(name: nil, path: dir)
    store
  end

  defp ref(key), do: %{id: SeriesRegistry.series_id(key), key: key, worker: 0, kind: :gauge}

  defp start_feedback(store, opts \\ []) do
    test = self()
    known = Keyword.get(opts, :known, [@key])

    {:ok, pid} =
      Feedback.start_link(
        [
          name: nil,
          state_store: store,
          lookup_series: fn key -> if key in known, do: {:ok, ref(key)}, else: :error end,
          incident_series: fn
            {:incident, 7} -> {:ok, [@key, @other]}
            _ -> :error
          end,
          apply_mute: &send(test, {:muted, &1}),
          apply_sensitivity: &send(test, {:sensitivity, &1.key, &2})
        ] ++ Keyword.take(opts, [:base_sensitivity])
      )

    pid
  end

  describe "mute" do
    test "persists first, then updates memory, then replies" do
      store = tmp_store()
      fb = start_feedback(store)

      assert {:ok, %{action: :mute, target: {:series, @key}}} =
               Feedback.apply(fb, "mute", %{series: @series})

      assert_received {:muted, {:series, @key}}
      assert [{{:series, @key}, true}] = StateStore.records(store, :mute)
    end

    test "an Incident can be muted by its id" do
      store = tmp_store()
      fb = start_feedback(store, known: [@key, @other])
      assert {:ok, _} = Feedback.apply(fb, "mute", %{"incident_id" => "7"})
      assert_received {:muted, {:incident, 7}}
    end
  end

  describe "this was normal" do
    test "halves sensitivity from the base and is idempotent" do
      store = tmp_store()
      fb = start_feedback(store)
      id = SeriesRegistry.series_id(@key)

      assert {:ok, _} = Feedback.apply(fb, "this was normal", %{series: @series})
      assert_received {:sensitivity, @key, 0.5}
      assert Feedback.overrides(fb) == %{id => 0.5}

      assert {:ok, _} = Feedback.apply(fb, "this was normal", %{series: @series})
      assert Feedback.overrides(fb) == %{id => 0.5}
      assert [{^id, 0.5}] = StateStore.records(store, :sensitivity)
    end

    test "is floored at 0.25 and never raises sensitivity" do
      store = tmp_store()

      assert {:ok, _} =
               start_feedback(store, base_sensitivity: 0.4)
               |> Feedback.apply("this was normal", %{series: @series})

      assert_received {:sensitivity, @key, 0.25}

      store2 = tmp_store()

      assert {:ok, _} =
               start_feedback(store2, base_sensitivity: 0.1)
               |> Feedback.apply("this was normal", %{series: @series})

      assert_received {:sensitivity, @key, value}
      assert value <= 0.1
    end

    test "applies to every Series of an Incident" do
      store = tmp_store()
      fb = start_feedback(store, known: [@key, @other])
      assert {:ok, %{series: series}} = Feedback.apply(fb, "this was normal", %{incident_id: 7})
      assert Enum.sort(series) == Enum.sort([@key, @other])
      assert_received {:sensitivity, @key, 0.5}
      assert_received {:sensitivity, @other, 0.5}
    end
  end

  describe "invalid requests change nothing" do
    for {label, action, target, field} <- [
          {"unknown action", "delete", %{series: @series}, :action},
          {"missing action", nil, %{series: @series}, :action},
          {"missing target", "mute", nil, :target},
          {"empty target", "mute", %{}, :target},
          {"unknown series", "mute", %{series: %{name: "nope", labels: %{}}}, :target},
          {"unknown incident", "this was normal", %{incident_id: 99}, :target},
          {"malformed incident", "mute", %{incident_id: "x"}, :target},
          {"nested label value", "mute",
           %{series: %{name: "latency", labels: %{"x" => %{"y" => 1}}}}, :target},
          {"numeric label value", "mute", %{series: %{name: "latency", labels: %{"host" => 1}}},
           :target},
          {"labels not a map", "mute", %{series: %{name: "latency", labels: [1]}}, :target}
        ] do
      test label do
        store = tmp_store()
        fb = start_feedback(store)

        assert {:error, :invalid, unquote(field)} =
                 Feedback.apply(fb, unquote(action), unquote(Macro.escape(target)))

        refute_received {:muted, _}
        refute_received {:sensitivity, _, _}
        assert StateStore.records(store, :mute) == []
        assert StateStore.records(store, :sensitivity) == []
      end
    end
  end

  describe "State_Store failure" do
    test "replies store_failed and leaves memory unchanged" do
      {:ok, flaky} = FlakyStore.start_link(tmp_store())
      fb = start_feedback(flaky, known: [@key, @other])
      FlakyStore.fail(flaky, true)

      log =
        capture_log(fn ->
          assert {:error, :store_failed} = Feedback.apply(fb, "mute", %{series: @series})
          assert {:error, :store_failed} = Feedback.apply(fb, "this was normal", %{incident_id: 7})
        end)

      assert log =~ "feedback State_Store write failed"
      refute_received {:muted, _}
      refute_received {:sensitivity, _, _}
      assert Feedback.overrides(fb) == %{}
    end

    test "a multi-Series write is atomic: on failure nothing is stored" do
      real = tmp_store()
      {:ok, flaky} = FlakyStore.start_link(real)
      fb = start_feedback(flaky, known: [@key, @other])
      FlakyStore.fail_after(flaky, 0)

      capture_log(fn ->
        assert {:error, :store_failed} = Feedback.apply(fb, "this was normal", %{incident_id: 7})
      end)

      assert StateStore.records(real, :sensitivity) == []
      assert Feedback.overrides(fb) == %{}
      refute_received {:sensitivity, _, _}
    end

    test "an unavailable State_Store is a failure, not a crash" do
      store = tmp_store()
      fb = start_feedback(store)
      GenServer.stop(store)

      capture_log(fn ->
        assert {:error, :store_failed} = Feedback.apply(fb, "mute", %{series: @series})
      end)

      assert Process.alive?(fb)
    end
  end

  test "overrides are restored from the State_Store at start" do
    store = tmp_store()
    :ok = StateStore.put_sensitivity(store, SeriesRegistry.series_id(@key), 0.5)
    fb = start_feedback(store)
    assert Feedback.overrides(fb) == %{SeriesRegistry.series_id(@key) => 0.5}
  end

  describe "in-memory targets" do
    test "a Series created after the feedback picks up the persisted override" do
      store = tmp_store()
      :ok = StateStore.put_sensitivity(store, SeriesRegistry.series_id(@key), 0.5)
      index = System.unique_integer([:positive])

      {:ok, w} =
        Worker.start_link(
          index: index,
          name: nil,
          sweep_interval: 3_600_000,
          queue_counter: :atomics.new(1, signed: true),
          state_store: store,
          dispatcher: nil,
          correlator: nil
        )

      on_exit(fn ->
        :persistent_term.erase({Worker, index})
        :persistent_term.erase({Worker, :capacity, index})
      end)

      Worker.enqueue(w, [{@key, 1.0, 1_000, :gauge}])
      assert Worker.series(w, @key).meta.sensitivity_override == 0.5
      GenServer.stop(w)
    end

    test "Worker.set_sensitivity sets the override Decision uses" do
      index = System.unique_integer([:positive])

      {:ok, w} =
        Worker.start_link(
          index: index,
          name: nil,
          sweep_interval: 3_600_000,
          queue_counter: :atomics.new(1, signed: true),
          dispatcher: nil,
          correlator: nil
        )

      on_exit(fn ->
        :persistent_term.erase({Worker, index})
        :persistent_term.erase({Worker, :capacity, index})
      end)

      :ok =
        Worker.put_series(w, @key, State.encode(State.new(kind: :gauge)), %Decision.Meta{key: @key})

      assert :ok = Worker.set_sensitivity(w, @key, 0.5)
      assert Worker.series(w, @key).meta.sensitivity_override == 0.5
      assert :error = Worker.set_sensitivity(w, @other, 0.5)
      GenServer.stop(w)
    end

    test "a Worker restores persisted overrides with its checkpoint" do
      store = tmp_store()
      index = System.unique_integer([:positive])

      meta = Map.put(%Decision.Meta{key: @key}, :worker_index, index)
      entry = {@key, {1, State.encode(State.new(kind: :gauge)), :erlang.term_to_binary(meta)}}
      :ok = StateStore.checkpoint(store, [entry])
      :ok = StateStore.put_sensitivity(store, SeriesRegistry.series_id(@key), 0.5)

      {:ok, w} =
        Worker.start_link(
          index: index,
          name: nil,
          sweep_interval: 3_600_000,
          queue_counter: :atomics.new(1, signed: true),
          state_store: store,
          dispatcher: nil,
          correlator: nil
        )

      on_exit(fn ->
        :persistent_term.erase({Worker, index})
        :persistent_term.erase({Worker, :capacity, index})
      end)

      assert Worker.series(w, @key).meta.sensitivity_override == 0.5
      GenServer.stop(w)
    end

    test "Decision applies the override to the effective threshold" do
      meta = %Decision.Meta{key: @key, status: :healthy, sensitivity_override: 0.5}
      cfg = %{score_threshold: 75, persistence: %{n: 1, m: 5}}
      # 100 - (100 - 75) * 0.5 = 87.5: a score of 80 alerts by default but not now.
      {_m, events} =
        Decision.step(meta, %{score: 80.0, ts: 1, observed: 1.0, lower: 0.0, upper: 0.5}, 1, cfg)

      refute Enum.any?(events, &match?({:anomaly_started, _}, &1))

      {_m, events} =
        Decision.step(
          %{meta | sensitivity_override: nil},
          %{score: 80.0, ts: 1, observed: 1.0, lower: 0.0, upper: 0.5},
          1,
          cfg
        )

      assert Enum.any?(events, &match?({:anomaly_started, _}, &1))
    end

    test "Dispatcher mutes are recorded and restored from the State_Store" do
      store = tmp_store()
      {:ok, d} = Dispatcher.start_link(name: nil, state_store: store)
      :ok = Dispatcher.put_mute(d, {:series, @key})
      assert Policy.muted?(Dispatcher.state(d), {:series, @key})
      GenServer.stop(d)

      :ok = StateStore.put_mute(store, {:incident, 3}, true)
      {:ok, d2} = Dispatcher.start_link(name: nil, state_store: store)
      assert Policy.muted?(Dispatcher.state(d2), {:incident, 3})
      GenServer.stop(d2)
    end
  end
end

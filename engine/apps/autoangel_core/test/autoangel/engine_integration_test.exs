defmodule Autoangel.EngineIntegrationTest do
  @moduledoc """
  Topology_A (DB-free) integration and unit tests for the wired Engine Control
  plane and the push-to-alert path.

  Validates Requirements 35.1, 45.2, 45.3, 45.4, 74.6:

    * 45.4 / 45.2 — readiness returns 503 before boot and 200 after, health is
      200 when authenticated; both require control credentials.
    * 45.3 — the /metrics endpoint is valid Prometheus exposition and contains
      every required metric family.
    * 74.6 — only GET /healthz and /readyz are exemptable, only when listed in
      :auth_exemptions; /metrics is never exempt and exempted responses carry no
      metric data.
    * 35.1 — a pushed anomalous sample stream is admitted through the real
      Ingest admission pipeline into a real Worker, detection raises an anomaly,
      the Worker forwards the anomaly event to the real Alert.Dispatcher, which
      delivers through a real PluginRunner to a capture plugin — all with NO
      storage backend configured (Topology_A).
  """
  use ExUnit.Case, async: false

  import Plug.Conn
  import Plug.Test

  alias Autoangel.Alert.{Dispatcher, PluginRunner}
  alias Autoangel.Exposition.Sample
  alias Autoangel.HTTP.Control

  @config %{control_api: %{token: "control-secret"}, auth_exemptions: []}
  @creds "Bearer control-secret"

  # A trivial capture plugin: deliver/2 always succeeds and additionally forwards
  # the alert to a test process so the push-to-alert flow can be observed. The
  # test process pid is carried in config so no module-level state is needed.
  # When no test pid is configured it still succeeds, which drives the global
  # per-plugin delivery counter (production /metrics path).
  defmodule CapturePlugin do
    @moduledoc false
    @behaviour Autoangel.Plugin

    @impl Autoangel.Plugin
    @spec validate_config(map()) :: :ok
    def validate_config(_config), do: :ok

    @impl Autoangel.Plugin
    @spec deliver(term(), map()) :: :ok
    def deliver(alert, %{test_pid: pid}) when is_pid(pid) do
      send(pid, {:delivered, alert})
      :ok
    end

    def deliver(_alert, _config), do: :ok
  end

  setup do
    Autoangel.Boot.set_phase(:starting)
    Autoangel.Metrics.ensure_started()
    # Restore the boot phase for any suite that relies on the default.
    on_exit(fn -> Autoangel.Boot.set_phase(:starting) end)
    :ok
  end

  # Starts the (default-named) SeriesRegistry, tolerating a lingering instance from
  # an interleaved async:false suite whose teardown raced this setup. Stops any
  # running registry first and retries once on {:already_started}. Mirrors the
  # idiom in series_registry_test.exs.
  defp start_registry!(opts) do
    if pid = Process.whereis(Autoangel.SeriesRegistry), do: GenServer.stop(pid)

    case Autoangel.SeriesRegistry.start_link(opts) do
      {:ok, pid} ->
        Process.unlink(pid)
        pid

      {:error, {:already_started, running}} ->
        GenServer.stop(running)
        {:ok, pid} = Autoangel.SeriesRegistry.start_link(opts)
        Process.unlink(pid)
        pid
    end
  end

  defp safe_stop(nil), do: :ok

  defp safe_stop(pid) when is_pid(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid, :normal, 1_000)
    :ok
  catch
    :exit, _reason -> :ok
  end

  # ── 45.4 / 45.2 — health & readiness ─────────────────────────────────────────

  test "readiness is 503 before boot and 200 after, health is 200 when authed" do
    # **Validates: Requirements 45.2, 45.4** — /readyz reflects the boot phase and
    # both control endpoints require credentials.

    # No credentials -> 401 for both endpoints, in both phases.
    assert conn(:get, "/healthz") |> Control.call(config: @config) |> Map.fetch!(:status) == 401
    assert conn(:get, "/readyz") |> Control.call(config: @config) |> Map.fetch!(:status) == 401

    # BEFORE boot: health 200, readiness 503.
    health_before =
      conn(:get, "/healthz")
      |> put_req_header("authorization", @creds)
      |> Control.call(config: @config)

    assert health_before.status == 200
    assert health_before.resp_body == "ok\n"

    ready_before =
      conn(:get, "/readyz")
      |> put_req_header("authorization", @creds)
      |> Control.call(config: @config)

    assert ready_before.status == 503
    assert ready_before.resp_body == "not ready\n"

    # AFTER boot: readiness flips to 200, health stays 200.
    Autoangel.Boot.set_phase(:ready)

    ready_after =
      conn(:get, "/readyz")
      |> put_req_header("authorization", @creds)
      |> Control.call(config: @config)

    assert ready_after.status == 200
    assert ready_after.resp_body == "ready\n"

    health_after =
      conn(:get, "/healthz")
      |> put_req_header("authorization", @creds)
      |> Control.call(config: @config)

    assert health_after.status == 200
    assert health_after.resp_body == "ok\n"

    # Still 401 without credentials after boot.
    assert conn(:get, "/readyz") |> Control.call(config: @config) |> Map.fetch!(:status) == 401
  end

  # ── 45.3 — /metrics contents ──────────────────────────────────────────────────

  test "/metrics is valid exposition and contains every required metric family" do
    # **Validates: Requirement 45.3** — after populating counters, gauges, a
    # histogram and a per-plugin delivery outcome, /metrics parses as exposition
    # 0.0.4 and exposes ingest rate, series count, detection-latency histogram,
    # dropped samples, storage queue depth, storage failed ops, and per-plugin
    # delivery outcomes with plugin+outcome labels.
    #
    # Two per-plugin paths are exercised:
    #   * the OPT-DRIVEN path (Control mounted WITH :plugins/:plugin_runners),
    #     used by assertions that want a deterministic label; and
    #   * the PRODUCTION path (Control mounted with config only), where
    #     per-plugin series come from the global Metrics counter keyed by the
    #     plugin MODULE name (how the real listener mounts Control).
    start_registry!(per_metric_limit: 10, total_limit: 50, pool_size: 1)
    on_exit(fn -> safe_stop(Process.whereis(Autoangel.SeriesRegistry)) end)

    ts = start_supervised!({Task.Supervisor, []})

    {:ok, runner} =
      PluginRunner.start_link(
        name: nil,
        plugin: CapturePlugin,
        config: %{},
        task_supervisor: ts,
        observer: self()
      )

    on_exit(fn -> safe_stop(runner) end)

    # Populate metrics across every family the Control plane exposes.
    Autoangel.Metrics.increment(:ingest_samples_accepted, 5)
    Autoangel.Metrics.increment(:samples_dropped, 2)
    Autoangel.Metrics.increment(:samples_load_shed, 1)
    Autoangel.Metrics.increment({:storage, :successful}, 3)
    Autoangel.Metrics.increment({:storage, :retryable}, 1)
    Autoangel.Metrics.increment({:storage, :permanent}, 1)
    Autoangel.Metrics.set(:storage_queue_depth, 7)
    Autoangel.Metrics.observe(:detection_latency_seconds, 0.02)

    # Drive a real per-plugin success through the runner so BOTH the runner's
    # own counters (opt-driven path) and the global Metrics.plugin_delivery
    # counter (production path) are populated.
    assert :ok = PluginRunner.enqueue(runner, %{anomaly_id: "a-1"})
    assert_receive {:plugin_delivery, CapturePlugin, :success}, 1_000

    # Create a series so the series-count gauge is non-zero.
    assert {:created, _ref} =
             Autoangel.SeriesRegistry.resolve_or_create({"integration_metric", %{"id" => "1"}})

    # ── Opt-driven /metrics (Control told about the runner explicitly) ─────────
    response =
      conn(:get, "/metrics")
      |> put_req_header("authorization", @creds)
      |> Control.call(
        config: @config,
        plugins: ["capture"],
        plugin_runners: %{"capture" => runner}
      )

    assert response.status == 200
    assert [content_type] = get_resp_header(response, "content-type")
    assert content_type =~ "text/plain; version=0.0.4"

    body = response.resp_body
    assert {:ok, families} = Autoangel.Exposition.parse(body)

    # Parse-level assertions: every required family is present with the right
    # type, not merely a matching substring.
    assert family_of(families, "autoangel_ingest_rate_samples_per_second").type == :gauge
    assert family_of(families, "autoangel_series").type == :gauge
    assert family_of(families, "autoangel_detection_latency_seconds").type == :histogram
    assert family_of(families, "autoangel_storage_queue_depth").type == :gauge
    assert family_of(families, "autoangel_storage_operations_failed_total").type == :counter

    # Detection-latency histogram exposes bucket/count/sum series.
    latency = family_of(families, "autoangel_detection_latency_seconds")
    assert Enum.any?(latency.samples, &(&1.name == "autoangel_detection_latency_seconds_bucket"))
    assert Enum.any?(latency.samples, &(&1.name == "autoangel_detection_latency_seconds_count"))

    # Storage failed-ops counter aggregates retryable + permanent. The Metrics
    # table is a shared global singleton, so other tests in the suite may have
    # incremented these counters; assert the family shape and that our two
    # increments are reflected (value is monotonic, hence >= 2.0).
    failed = family_of(families, "autoangel_storage_operations_failed_total")
    assert [%{labels: %{}, value: value}] = failed.samples
    assert value >= 2.0

    # Dropped samples: either the dedicated counter or the load-shed counter.
    assert has_family?(families, "autoangel_samples_dropped_total") or
             has_family?(families, "autoangel_samples_load_shed_total")

    # Per-plugin delivery outcomes carry plugin+outcome labels. On the opt-driven
    # path the plugin label is the configured name "capture".
    plugin_family = family_of(families, "autoangel_plugin_deliveries_total")
    assert plugin_family.type == :counter

    assert Enum.any?(plugin_family.samples, fn sample ->
             sample.labels["plugin"] == "capture" and
               sample.labels["outcome"] == "success" and sample.value >= 1.0
           end)

    # ── Production /metrics (Control mounted with config only) ─────────────────
    # The real listener mounts Control WITHOUT :plugins/:plugin_runners, so
    # per-plugin series must still appear, sourced from the global Metrics
    # counter keyed by the plugin MODULE (underscored last segment).
    prod =
      conn(:get, "/metrics")
      |> put_req_header("authorization", @creds)
      |> Control.call(config: @config)

    assert prod.status == 200
    assert {:ok, prod_families} = Autoangel.Exposition.parse(prod.resp_body)
    prod_plugin = family_of(prod_families, "autoangel_plugin_deliveries_total")
    assert prod_plugin.type == :counter

    # The module-derived plugin name: "Elixir.*.CapturePlugin" -> "capture_plugin".
    assert Enum.any?(prod_plugin.samples, fn sample ->
             sample.labels["plugin"] == "capture_plugin" and
               sample.labels["outcome"] == "success" and sample.value >= 1.0
           end)
  end

  # ── 74.6 — exemptions ─────────────────────────────────────────────────────────

  test "only /healthz and /readyz are exemptable and exempt bodies carry no metrics" do
    # **Validates: Requirement 74.6** — with /healthz and /readyz exempted, an
    # unauthenticated GET returns only the status line body; /metrics is never
    # exempt (401) and a non-GET method is never exempt (401). Exempt bodies must
    # contain no series/label/sample/metric content.
    config = %{control_api: %{token: "control-secret"}, auth_exemptions: ["/healthz", "/readyz"]}

    # Pre-populate metrics so any leak would be visible in an exempt body.
    Autoangel.Metrics.increment(:ingest_samples_accepted, 4)
    Autoangel.Metrics.observe(:detection_latency_seconds, 0.01)

    # Unauthenticated GET /healthz -> exempt, 200, body is exactly "ok\n".
    health = conn(:get, "/healthz") |> Control.call(config: config)
    assert health.status == 200
    assert health.resp_body == "ok\n"

    # Unauthenticated GET /readyz -> exempt, reflects boot phase, no metric data.
    ready_starting = conn(:get, "/readyz") |> Control.call(config: config)
    assert ready_starting.status == 503
    assert ready_starting.resp_body == "not ready\n"

    Autoangel.Boot.set_phase(:ready)
    ready = conn(:get, "/readyz") |> Control.call(config: config)
    assert ready.status == 200
    assert ready.resp_body == "ready\n"

    # None of the exempt bodies contain metric/series/label/sample content.
    for body <- [health.resp_body, ready_starting.resp_body, ready.resp_body] do
      refute body =~ "autoangel_"
      refute body =~ "# TYPE"
      refute body =~ "{"
    end

    # /metrics is NEVER exemptable, even listed credentials would be required.
    metrics_unauth =
      conn(:get, "/metrics")
      |> Control.call(config: config, plugins: ["capture"])

    assert metrics_unauth.status == 401

    # Even if /metrics were (wrongly) listed, it must still require auth.
    metrics_listed_config = %{
      control_api: %{token: "control-secret"},
      auth_exemptions: ["/healthz", "/readyz", "/metrics"]
    }

    assert conn(:get, "/metrics")
           |> Control.call(config: metrics_listed_config)
           |> Map.fetch!(:status) == 401

    # A non-GET method is never exempt.
    assert conn(:post, "/healthz") |> Control.call(config: config) |> Map.fetch!(:status) == 401
  end

  # ── 35.1 — Topology_A push-to-alert through the REAL supervision tree ─────────

  test "Topology_A: a pushed anomaly flows ingest -> Worker -> Dispatcher -> plugin" do
    # **Validates: Requirement 35.1** — the DB-free (Topology_A) push-to-alert
    # path, proven against the REAL wiring. A pushed anomalous sample stream is
    # admitted through the real Ingest admission pipeline into a real Worker;
    # detection raises an anomaly; the Worker forwards the anomaly event to the
    # real Alert.Dispatcher (started by the real Alert.Supervisor with an enabled
    # capture plugin); the Dispatcher resolves the registered PluginRunner by
    # name and delivers the alert to the plugin. No storage backend is wired.
    test_pid = self()

    start_registry!(per_metric_limit: 100, total_limit: 1_000, pool_size: 1)
    on_exit(fn -> safe_stop(Process.whereis(Autoangel.SeriesRegistry)) end)

    # ── Real alert subtree: PluginSupervisor (+ PluginTasks) + Dispatcher ───────
    # Enable the capture plugin. The PluginSupervisor registers its runner under
    # Module.concat(Autoangel.Alert.PluginRunner, :capture_plugin); the Dispatcher
    # derives the SAME key/name and resolves the runner at delivery time. The
    # observer flows to every runner, so a success is reported back to the test.
    _alert_sup =
      start_supervised!(
        {Autoangel.Alert.Supervisor,
         plugins: [%{name: "capture", module: CapturePlugin, config: %{test_pid: test_pid}}],
         observer: test_pid,
         state_store: nil}
      )

    # The Dispatcher's policy must have derived exactly the capture plugin key
    # from the enabled plugins (proves Alert.Supervisor -> Dispatcher wiring).
    assert Process.whereis(Dispatcher) != nil
    dispatcher_policy = Dispatcher.state(Dispatcher)
    assert dispatcher_policy.plugins == [:capture_plugin]

    # The runner really is registered under the supervisor's naming scheme.
    runner_name = Module.concat(PluginRunner, :capture_plugin)
    assert is_pid(Process.whereis(runner_name))

    # ── Real Worker pool wired to the real Dispatcher ───────────────────────────
    # A short learning duration and n=1 persistence let a single anomalous sample
    # (after a flat baseline) cross learning and open an anomaly immediately. A
    # large sweep interval keeps the missing-data sweep out of the way.
    decision_cfg = %{
      learning: %{duration: 0},
      persistence: %{n: 1, m: 5},
      score_threshold: 75,
      missing_data: %{multiple: 3.0}
    }

    workers =
      start_supervised!(
        {Autoangel.Workers,
         pool_size: 1,
         dispatcher: Dispatcher,
         decision_cfg: decision_cfg,
         sweep_interval: 3_600_000,
         queue_capacity: 20_000,
         max_batch: 8,
         state_store: nil,
         scrape_status_table: nil}
      )

    assert is_pid(Autoangel.Workers.worker(0, workers))

    # ── Push path: flat baseline then a spike, through real admission ───────────
    # No :storage_writer and no :worker_fun -> admission uses the DEFAULT worker
    # lookup (Autoangel.Workers.worker/1) and no storage backend (Topology_A).
    name = "integration_push"
    labels = %{"id" => "topology-a"}

    baseline =
      for i <- 0..19 do
        %Sample{
          name: name,
          labels: labels,
          value: 1.0,
          timestamp: 1_000 + i * 1_000,
          type_hint: :gauge
        }
      end

    spike = %Sample{
      name: name,
      labels: labels,
      value: 5_000.0,
      timestamp: 1_000 + 20 * 1_000,
      type_hint: :gauge
    }

    assert {:ok, %{accepted: 20, rejected_cardinality: 0}} =
             Autoangel.Ingest.process_samples(baseline, pool_size: 1)

    assert {:ok, %{accepted: 1, rejected_cardinality: 0}} =
             Autoangel.Ingest.process_samples([spike], pool_size: 1)

    # Ingestion created exactly one series with no storage backend in the path.
    assert Autoangel.SeriesRegistry.count() == 1

    # ── Alert path assertions ───────────────────────────────────────────────────
    # The anomaly reached the capture plugin via the full real chain, and the
    # runner reported a successful delivery to the observer.
    assert_receive {:delivered, %Autoangel.Alert{} = alert}, 2_000
    assert alert.name == name
    assert alert.labels == labels
    assert is_binary(alert.anomaly_id)

    assert_receive {:plugin_delivery, CapturePlugin, :success}, 2_000
  end

  # ── helpers ───────────────────────────────────────────────────────────────────

  defp family_of(families, name) do
    Enum.find(families, &(&1.name == name)) ||
      flunk("expected metric family #{inspect(name)} in exposition")
  end

  defp has_family?(families, name), do: Enum.any?(families, &(&1.name == name))
end

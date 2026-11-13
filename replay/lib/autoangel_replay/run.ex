defmodule AutoangelReplay.Run do
  @moduledoc """
  Replays Series through an in-process Engine (Requirements 77.1, 77.5, 77.6).

  A replay stops any `autoangel_core` application already running in the node
  and starts its own; it is meant to run in a dedicated harness node.

  The Engine starts with its default configuration plus only what the harness
  needs to talk to it (ingest and Control_API tokens, local ports, a high ingest
  rate limit) and the replay-only capture plugin. Engine time comes from a
  simulated clock, set to each Sample timestamp before its push, so detection,
  receive times, cooldowns and missing-data sweeps follow replayed time. Samples
  are pushed in timestamp order through the real ingest HTTP API; between
  pushes the harness waits until Workers, the Correlator and the Dispatcher have
  handled everything, and runs a missing-data sweep at every sweep interval of
  simulated time, including across gaps where no Series reports.

  The report identifies the detection configuration by `config_digest`, the
  SHA-256 of the canonical configuration with transport settings removed.
  """

  alias Autoangel.Alert.Dispatcher
  alias Autoangel.Clock.Simulated
  alias AutoangelReplay.{Capture, Report, Series}

  @transport_keys [
    :http,
    :ingest,
    :control_api,
    :ingest_rate_limit,
    :tls,
    :plugins,
    :enabled_plugins,
    :auth_exemptions,
    :storage,
    :scrape_targets
  ]

  @doc """
  Replays `series` and reports. Options (used by the fault-injection harness):

    * `:config_toml` - extra TOML appended to the harness configuration
    * `:plugins` - plugin names to enable (default `["capture"]`)
    * `:on_step` - `fun.(step, total, ctx)` called after the Engine has settled
      following each timestamp group; `ctx` is `%{restart_engine: fun, kill_worker: fun}`
    * `:include_alerts` - add the delivered alerts as `:found` (`%{series_key, start_ms}`)
    * `:keep_running` - leave the Engine running afterwards and add `:stop` (a 0-arity fun)
  """
  @spec run([Series.t()], [map()], integer(), keyword()) :: map()
  def run(series, labels, seed, opts \\ []) do
    {ingest_port, control_port} = ports()
    # Fresh credentials per run, so nothing else on the network can push into
    # or control the Engine under test.
    tokens = %{ingest: token(), control: token()}
    config = engine_config(ingest_port, control_port, tokens, opts)
    dir = Path.join(System.tmp_dir!(), "autoangel-replay-#{System.unique_integer([:positive])}")
    samples = merged(series)
    {first_ts, _, _} = hd(samples)
    {:ok, clock} = Simulated.start_link(first_ts)
    {:ok, capture} = Capture.start_link()

    previous_state_path = System.get_env("AUTOANGEL_STATE_PATH")

    cleanup = fn ->
      stop_engine(previous_state_path)
      Agent.stop(capture)
      Agent.stop(clock)
      File.rm_rf(dir)
    end

    try do
      start_engine(config, clock, dir)
      sweep_ms = round(Map.fetch!(config.missing_data, :sweep_interval) * 1_000)
      ctx = step_context(config, clock, dir)
      feed(samples, clock, {ingest_port, tokens.ingest}, sweep_ms, Keyword.get(opts, :on_step), ctx)
      alerts = await_alerts()
      {report(series, labels, alerts, config, seed, sweep_ms), alerts}
    rescue
      error ->
        cleanup.()
        reraise error, __STACKTRACE__
    else
      {result, alerts} ->
        unless Keyword.get(opts, :keep_running, false), do: cleanup.()
        finish(result, alerts, opts, cleanup)
    end
  end

  defp finish(result, alerts, opts, cleanup) do
    result =
      if Keyword.get(opts, :include_alerts, false),
        do: Map.put(result, :found, found(alerts)),
        else: result

    if Keyword.get(opts, :keep_running, false), do: Map.put(result, :stop, cleanup), else: result
  end

  defp found(alerts),
    do: for(a <- alerts, do: %{series_key: a.anomaly.series_key, start_ms: a.start_time})

  # Handles for a fault hook: restart the Engine on the same State_Store path, or
  # kill one Worker and wait for its supervisor to bring it back.
  defp step_context(config, clock, dir) do
    %{
      restart_engine: fn ->
        _ = Application.stop(:autoangel_core)
        Process.delete(:autoangel_replay_workers)
        start_engine(config, clock, dir)
      end,
      kill_worker: fn index ->
        pids = workers()
        victim = Autoangel.Workers.worker(index)
        Process.delete(:autoangel_replay_workers)
        ref = Process.monitor(victim)
        Process.exit(victim, :kill)
        receive do: ({:DOWN, ^ref, _, _, _} -> :ok)

        wait_until(
          fn ->
            new = Autoangel.Workers.worker(index)
            is_pid(new) and new != victim and length(workers_now()) == length(pids)
          end,
          10_000
        )

        Process.delete(:autoangel_replay_workers)
        victim
      end
    }
  end

  defp workers_now do
    for {_id, pid, _, _} <- Supervisor.which_children(Autoangel.Workers), is_pid(pid), do: pid
  end

  # --- Engine lifecycle ---------------------------------------------------------

  defp engine_config(ingest_port, control_port, tokens, opts) do
    plugins = opts |> Keyword.get(:plugins, ["capture"]) |> Enum.map_join(", ", &~s("#{&1}"))

    toml =
      """
      [ingest]
      token = "#{tokens.ingest}"
      [control_api]
      token = "#{tokens.control}"
      [http]
      ingest_port = #{ingest_port}
      control_port = #{control_port}
      [ingest_rate_limit]
      rate = 1000000
      burst = 1000000
      [plugins]
      enabled = [#{plugins}]
      """ <> "\n" <> Keyword.get(opts, :config_toml, "")

    {:ok, config} = Autoangel.Config.load(toml, %{})
    config
  end

  defp start_engine(config, clock, dir) do
    _ = Application.stop(:autoangel_core)
    Application.put_env(:autoangel_core, :config, config)
    Application.put_env(:autoangel_core, :now, fn -> Simulated.now_ms(clock) end)
    System.put_env("AUTOANGEL_STATE_PATH", dir)
    {:ok, _} = Application.ensure_all_started(:autoangel_core)
    wait_until(&Autoangel.Boot.ready?/0, 10_000)
  end

  defp stop_engine(previous_state_path) do
    Process.delete(:autoangel_replay_workers)
    _ = Application.stop(:autoangel_core)
    Application.delete_env(:autoangel_core, :config)
    Application.delete_env(:autoangel_core, :now)

    if previous_state_path,
      do: System.put_env("AUTOANGEL_STATE_PATH", previous_state_path),
      else: System.delete_env("AUTOANGEL_STATE_PATH")
  end

  # --- feeding ------------------------------------------------------------------

  # Every Sample as {ts, series, value}, in ascending timestamp order; ties are
  # ordered by Series identity so the push order is deterministic.
  defp merged(series) do
    series
    |> Enum.flat_map(fn s -> for {ts, v} <- s.samples, do: {ts, s, v} end)
    |> Enum.sort_by(fn {ts, s, _v} -> {ts, Series.key(s)} end)
  end

  defp feed(samples, clock, target, sweep_ms, on_step, ctx) do
    {first, _, _} = hd(samples)
    groups = Enum.chunk_by(samples, &elem(&1, 0))
    total = length(groups)

    next_sweep =
      groups
      |> Enum.with_index(1)
      |> Enum.reduce(first + sweep_ms, fn {[{ts, _, _} | _] = group, step}, next_sweep ->
        next_sweep = sweep_until(clock, next_sweep, ts, sweep_ms)
        Simulated.set(clock, ts)
        push(target, group)
        settle()
        if on_step, do: on_step.(step, total, ctx)
        next_sweep
      end)

    {last, _, _} = List.last(samples)
    sweep_until(clock, next_sweep, last + 4 * sweep_ms, sweep_ms)
  end

  # Runs a sweep at every sweep boundary up to and including `until`.
  defp sweep_until(clock, next_sweep, until, sweep_ms) when next_sweep <= until do
    Simulated.set(clock, next_sweep)
    Enum.each(workers(), &send(&1, :sweep))
    settle()
    sweep_until(clock, next_sweep + sweep_ms, until, sweep_ms)
  end

  defp sweep_until(_clock, next_sweep, _until, _sweep_ms), do: next_sweep

  defp push({port, token}, group) do
    body = Enum.map_join(group, "", fn {ts, s, v} -> "#{Series.display(s)} #{value(v)} #{ts}\n" end)
    url = ~c"http://127.0.0.1:#{port}/api/v1/push"
    headers = [{~c"authorization", String.to_charlist("Bearer " <> token)}]

    case :httpc.request(:post, {url, headers, ~c"text/plain", body}, [timeout: 30_000], []) do
      {:ok, {{_, status, _}, _, _}} when status in 200..299 -> :ok
      other -> raise "replay push failed: #{inspect(other)}"
    end
  end

  defp value(:nan), do: "NaN"
  defp value(:inf), do: "+Inf"
  defp value(:neg_inf), do: "-Inf"
  defp value(v) when is_float(v), do: Float.to_string(v)
  defp value(v) when is_integer(v), do: Integer.to_string(v)

  # A synchronous round trip through each stage, in pipeline order, so all work
  # caused by the previous step has been handled before the clock moves on.
  defp settle do
    Enum.each(workers(), &:sys.get_state/1)
    :sys.get_state(Autoangel.Correlate)
    _ = Dispatcher.state()
    :ok
  end

  # Worker pids are resolved once per replay (the pool is fixed while it runs).
  defp workers do
    case Process.get(:autoangel_replay_workers) do
      nil ->
        pids =
          for {_id, pid, _, _} <- Supervisor.which_children(Autoangel.Workers), is_pid(pid), do: pid

        Process.put(:autoangel_replay_workers, pids)
        pids

      pids ->
        pids
    end
  end

  # Deliveries run asynchronously (a dispatch task, then the plugin runner).
  # Every Incident the Dispatcher opened for an unmuted Series yields exactly
  # one delivery per enabled plugin, so wait for that exact count.
  defp await_alerts do
    settle()

    expected =
      Dispatcher.state().incidents |> Map.values() |> Enum.count(&(not Map.get(&1, :muted?, false)))

    wait_until(fn -> Capture.count() >= expected end, 30_000)
    Capture.alerts()
  end

  # --- report -------------------------------------------------------------------

  defp report(series, labels, alerts, config, seed, sweep_ms) do
    timestamps = for s <- series, {ts, _} <- s.samples, do: ts

    params = %{
      persistence_ms: round(config.persistence.m * 60_000),
      persistence_n: config.persistence.n,
      sweep_ms: sweep_ms,
      typical_interval_ms:
        Map.new(series, fn s ->
          {Series.key(s), Report.typical_interval(Enum.map(s.samples, &elem(&1, 0)))}
        end),
      span_ms: Enum.max(timestamps) - Enum.min(timestamps)
    }

    found = found(alerts)

    Map.merge(Report.compute(labels, found, params), %{
      config_digest: digest(config),
      seed: seed,
      alerts: length(found)
    })
  end

  @doc "SHA-256 of the canonical detection configuration (transport settings removed)."
  @spec digest(map()) :: String.t()
  def digest(config) do
    config
    |> Map.drop(@transport_keys)
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  # --- helpers ------------------------------------------------------------------

  defp token, do: :crypto.strong_rand_bytes(24) |> Base.url_encode64(padding: false)

  defp ports do
    case {free_port(), free_port()} do
      {same, same} -> ports()
      pair -> pair
    end
  end

  defp free_port do
    {:ok, socket} = :gen_tcp.listen(0, ip: {127, 0, 0, 1}, reuseaddr: true)
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end

  defp wait_until(fun, timeout) do
    cond do
      fun.() ->
        :ok

      timeout <= 0 ->
        raise "replay timed out waiting for the Engine"

      true ->
        Process.sleep(20)
        wait_until(fun, timeout - 20)
    end
  end
end

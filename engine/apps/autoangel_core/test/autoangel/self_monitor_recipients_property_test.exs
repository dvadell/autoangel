defmodule Autoangel.SelfMonitorRecipientsPropertyTest do
  @moduledoc """
  Property 36 (Requirements 32.4, 32.5, 32.8): for any set of enabled and
  self-monitoring Plugins and any failing Plugin named in an alert, the
  recipients are the self-monitoring Plugins minus the failing one, and when
  that set is empty the alert is written to the log instead.
  """

  use ExUnit.Case, async: false
  use ExUnitProperties

  import ExUnit.CaptureLog

  alias Autoangel.SelfMonitor

  # A collector GenServer stands in for a PluginRunner. It answers the
  # {:enqueue, alert} call with :ok and reports to the test which plugin-key
  # collector received which alert.
  defmodule Collector do
    @moduledoc false
    use GenServer

    def start_link(opts), do: GenServer.start_link(__MODULE__, opts)

    @impl true
    def init(opts) do
      {:ok, %{plugin: Keyword.fetch!(opts, :plugin), test: Keyword.fetch!(opts, :test)}}
    end

    @impl true
    def handle_call({:enqueue, alert}, _from, state) do
      send(state.test, {:enqueued, state.plugin, alert})
      {:reply, :ok, state}
    end
  end

  # A monotonic, always-advancing clock backed by an Agent. Returning a huge
  # value guarantees the no_ingest elapsed check fires on the next :tick.
  # Returns {clock_fun, agent_pid} so the caller can tear the agent down.
  defp far_future_clock do
    {:ok, agent} = Agent.start_link(fn -> 0 end)

    fun = fn ->
      Agent.get_and_update(agent, fn v -> {1_000_000_000 + v, v + 1} end)
    end

    {fun, agent}
  end

  # Build collectors for each plugin key, returning the runners map plugin => pid.
  defp start_collectors(plugins) do
    test = self()

    Map.new(plugins, fn plugin ->
      {:ok, pid} = Collector.start_link(plugin: plugin, test: test)
      {plugin, pid}
    end)
  end

  # Stop a process started with start_link, ignoring already-dead processes.
  defp stop(pid) when is_pid(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
  catch
    :exit, _ -> :ok
  end

  # Independent model of recipient selection: plugins minus failing, filtered to
  # those that actually have a runner.
  defp expected_recipients(plugins, failing, runners) do
    plugins
    |> Enum.reject(fn p -> not is_nil(failing) and p == failing end)
    |> Enum.filter(fn p -> Map.has_key?(runners, p) end)
  end

  # Collect all {:enqueued, plugin, alert} currently in the mailbox.
  defp drain_enqueued(acc \\ []) do
    receive do
      {:enqueued, plugin, _alert} -> drain_enqueued([plugin | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end

  # Raise a plugin-failure episode for `failing` by driving failures beyond the
  # threshold, using the monitor state as a sync barrier between sends.
  defp drive_plugin_failure(monitor, failing) do
    for _ <- 1..2 do
      send(monitor, {:plugin_failure, failing})
      SelfMonitor.state(monitor)
    end

    :ok
  end

  property "recipients are self-monitoring plugins minus the failing one" do
    check all(
            plugins <-
              uniq_list_of(string(:alphanumeric, min_length: 1, max_length: 6),
                min_length: 2,
                max_length: 4
              ),
            failing_index <- integer(0..(length(plugins) - 1)),
            scenario <- member_of([:plugin_failure, :non_plugin]),
            max_runs: 50
          ) do
      runners = start_collectors(plugins)
      {clock, clock_agent} = far_future_clock()

      common_opts = [
        name: nil,
        plugins: plugins,
        runners: runners,
        clock: clock,
        plugin_failure_count: 1,
        no_ingest_ms: 1
      ]

      {:ok, monitor} = SelfMonitor.start_link(common_opts)

      {failing, expected} =
        case scenario do
          :plugin_failure ->
            failing = Enum.at(plugins, failing_index)
            drive_plugin_failure(monitor, failing)
            {failing, expected_recipients(plugins, failing, runners)}

          :non_plugin ->
            # no_ingest episode: failing plugin is nil, so all plugins receive it.
            send(monitor, :tick)
            SelfMonitor.state(monitor)
            {nil, expected_recipients(plugins, nil, runners)}
        end

      received = drain_enqueued() |> Enum.sort()

      assert received == Enum.sort(expected),
             "scenario=#{scenario} failing=#{inspect(failing)} plugins=#{inspect(plugins)}"

      # The failing plugin, if any, must never be a recipient.
      if failing, do: refute(failing in received)

      stop(monitor)
      stop(clock_agent)
      Enum.each(Map.values(runners), &stop/1)
    end
  end

  property "an empty recipient set logs the alert instead of delivering it" do
    check all(
            only <- string(:alphanumeric, min_length: 1, max_length: 6),
            max_runs: 50
          ) do
      # Single self-monitoring plugin; when it is the failing plugin the
      # recipient set is empty, so the alert must fall back to the log.
      runners = start_collectors([only])
      {clock, clock_agent} = far_future_clock()

      {:ok, monitor} =
        SelfMonitor.start_link(
          name: nil,
          plugins: [only],
          runners: runners,
          clock: clock,
          plugin_failure_count: 1
        )

      log =
        capture_log(fn ->
          drive_plugin_failure(monitor, only)
        end)

      # Confirm the episode actually raised.
      assert Map.has_key?(SelfMonitor.state(monitor).conditions, {:plugin_failure, only})

      # No collector received the alert.
      assert drain_enqueued() == []

      # The alert message was written to the log as the fallback.
      assert log =~ "Autoangel Engine self-monitoring alert"
      assert log =~ "failing plugin: #{only}"

      stop(monitor)
      stop(clock_agent)
      Enum.each(Map.values(runners), &stop/1)
    end
  end
end

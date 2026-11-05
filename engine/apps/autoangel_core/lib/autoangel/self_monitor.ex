defmodule Autoangel.Alert.SelfMonitor do
  @moduledoc "Alert raised when the Engine detects a failure in itself."

  @enforce_keys [:condition, :began_at, :message]
  defstruct [:condition, :began_at, :plugin, :dropped, :message]
end

defmodule Autoangel.SelfMonitor do
  @moduledoc "Detects Engine failures and delivers one alert per failure episode."

  use GenServer
  require Logger

  alias Autoangel.Alert.PluginRunner
  alias Autoangel.Alert.SelfMonitor, as: SelfMonitorAlert

  @tick_ms 5_000
  @default_no_ingest_ms :timer.minutes(10)
  @default_database_unreachable_ms :timer.minutes(2)
  @default_plugin_failure_count 3

  defstruct started_at: nil,
            started_wall_at: nil,
            last_ingest_at: nil,
            last_ingest_wall_at: nil,
            last_database_success_at: nil,
            last_database_success_wall_at: nil,
            conditions: %{},
            plugin_failures: %{},
            dropped: 0,
            no_ingest_ms: @default_no_ingest_ms,
            database_unreachable_ms: @default_database_unreachable_ms,
            plugin_failure_count: @default_plugin_failure_count,
            storage_configured?: false,
            runners: %{},
            plugins: [],
            clock: nil

  @type event ::
          :accepted_sample
          | :database_success
          | {:storage_dropped, non_neg_integer()}
          | {:plugin_success, term()}
          | {:plugin_failure, term()}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @spec accepted_sample(GenServer.server()) :: :ok
  def accepted_sample(server \\ __MODULE__), do: send(server, :accepted_sample)

  @spec database_success(GenServer.server()) :: :ok
  def database_success(server \\ __MODULE__), do: send(server, :database_success)

  @spec storage_dropped(non_neg_integer(), GenServer.server()) :: :ok
  def storage_dropped(count, server \\ __MODULE__) when is_integer(count) and count > 0,
    do: send(server, {:storage_dropped, count})

  @spec plugin_success(term(), GenServer.server()) :: :ok
  def plugin_success(plugin, server \\ __MODULE__), do: send(server, {:plugin_success, plugin})

  @spec plugin_failure(term(), GenServer.server()) :: :ok
  def plugin_failure(plugin, server \\ __MODULE__), do: send(server, {:plugin_failure, plugin})

  @spec event(event(), GenServer.server()) :: :ok
  def event(event, server \\ __MODULE__), do: send(server, event)

  @spec state(GenServer.server()) :: map()
  def state(server \\ __MODULE__), do: GenServer.call(server, :state)

  @impl true
  def init(opts) do
    now = clock(opts).()
    wall_now = System.system_time(:millisecond)

    storage_configured? =
      Keyword.get(opts, :storage_configured?, Keyword.has_key?(opts, :storage))

    plugins = Keyword.get(opts, :plugins, []) |> Enum.map(&plugin_key/1)
    runners = Keyword.get(opts, :runners, %{}) |> normalize_runners()

    state = %__MODULE__{
      started_at: now,
      started_wall_at: wall_now,
      last_ingest_at: now,
      last_ingest_wall_at: wall_now,
      last_database_success_at: now,
      last_database_success_wall_at: wall_now,
      no_ingest_ms: positive(opts, :no_ingest_ms, @default_no_ingest_ms),
      database_unreachable_ms:
        positive(opts, :database_unreachable_ms, @default_database_unreachable_ms),
      plugin_failure_count: positive(opts, :plugin_failure_count, @default_plugin_failure_count),
      storage_configured?: storage_configured?,
      runners: runners,
      plugins: if(plugins == [], do: Map.keys(runners), else: plugins),
      clock: clock(opts)
    }

    schedule_tick()
    {:ok, state}
  end

  @impl true
  def handle_call(:state, _from, state), do: {:reply, state, state}

  @impl true
  def handle_info(:tick, state) do
    state = check_elapsed_conditions(state)
    schedule_tick()
    {:noreply, state}
  end

  def handle_info(:accepted_sample, state),
    do:
      {:noreply,
       recover(state, :no_ingest, %{
         last_ingest_at: state.clock.(),
         last_ingest_wall_at: System.system_time(:millisecond)
       })}

  def handle_info(:database_success, state),
    do:
      {:noreply,
       recover(state, :database_unreachable, %{
         last_database_success_at: state.clock.(),
         last_database_success_wall_at: System.system_time(:millisecond)
       })}

  def handle_info({:storage_dropped, count}, %{storage_configured?: true} = state)
      when is_integer(count) and count > 0 do
    state = %{state | dropped: state.dropped + count}
    {:noreply, activate(state, :storage_overflow, %{dropped: state.dropped})}
  end

  def handle_info({:storage_dropped, _count}, state), do: {:noreply, state}

  def handle_info({:storage_enqueued, true}, state),
    do: {:noreply, recover(state, :storage_overflow, %{})}

  def handle_info({:storage_enqueued, false}, state), do: {:noreply, state}

  def handle_info({:plugin_success, plugin}, state) do
    plugin = canonical_plugin(plugin, state.plugins)
    failures = Map.delete(state.plugin_failures, plugin)

    {:noreply, recover(state, {:plugin_failure, plugin}, %{plugin_failures: failures})}
  end

  def handle_info({:plugin_failure, plugin}, state) do
    plugin = canonical_plugin(plugin, state.plugins)
    count = Map.get(state.plugin_failures, plugin, 0) + 1
    state = %{state | plugin_failures: Map.put(state.plugin_failures, plugin, count)}

    if count > state.plugin_failure_count,
      do: {:noreply, activate(state, {:plugin_failure, plugin}, %{plugin: plugin})},
      else: {:noreply, state}
  end

  def handle_info({:plugin_delivery, plugin, :success}, state),
    do: handle_info({:plugin_success, plugin}, state)

  def handle_info({:plugin_delivery, plugin, :failure}, state),
    do: handle_info({:plugin_failure, plugin}, state)

  def handle_info(_message, state), do: {:noreply, state}

  defp check_elapsed_conditions(state) do
    now = state.clock.()

    state =
      if now - state.last_ingest_at >= state.no_ingest_ms,
        do: activate(state, :no_ingest, %{began_at_ms: state.last_ingest_wall_at}),
        else: recover(state, :no_ingest, %{})

    if state.storage_configured? and
         now - state.last_database_success_at >= state.database_unreachable_ms,
       do:
         activate(state, :database_unreachable, %{
           began_at_ms: state.last_database_success_wall_at
         }),
       else: recover(state, :database_unreachable, %{})
  end

  defp activate(state, condition, details) do
    case Map.get(state.conditions, condition) do
      %{alerted?: true} ->
        state

      nil ->
        began_at =
          details
          |> Map.get(:began_at_ms, System.system_time(:millisecond))
          |> DateTime.from_unix!(:millisecond)
          |> DateTime.truncate(:second)

        alert = build_alert(condition, began_at, details)
        deliver(state, alert, details[:plugin])
        put_in(state.conditions[condition], %{began_at: began_at, alerted?: true})

      _active ->
        state
    end
  end

  defp recover(state, condition, updates) do
    state = Map.merge(state, updates)

    if Map.has_key?(state.conditions, condition),
      do: %{state | conditions: Map.delete(state.conditions, condition)},
      else: state
  end

  defp build_alert(condition, began_at, details) do
    plugin = details[:plugin]
    dropped = details[:dropped]
    condition_name = condition_name(condition)

    message =
      "Autoangel Engine self-monitoring alert: this alert concerns the Engine itself, not a monitored Series; " <>
        "condition #{condition_name} began at " <>
        DateTime.to_iso8601(began_at) <>
        if(plugin, do: "; failing plugin: #{plugin}", else: "") <>
        if(dropped, do: "; samples dropped: #{dropped}", else: "")

    %SelfMonitorAlert{
      condition: condition,
      began_at: began_at,
      plugin: plugin,
      dropped: dropped,
      message: message
    }
  end

  defp deliver(state, alert, failing_plugin) do
    recipients = recipients(state, failing_plugin)

    if recipients == [] do
      Logger.error(alert.message)
    else
      Enum.each(recipients, &deliver_to_runner(&1, alert))
    end
  end

  defp recipients(state, failing_plugin) do
    state.plugins
    |> Enum.reject(&(not is_nil(failing_plugin) and &1 == failing_plugin))
    |> Enum.map(&{&1, Map.get(state.runners, &1)})
    |> Enum.filter(fn {_plugin, runner} -> not is_nil(runner) end)
  end

  defp deliver_to_runner({_plugin, runner}, alert) do
    result =
      try do
        PluginRunner.enqueue(runner, alert)
      catch
        :exit, reason -> {:error, {:runner_unavailable, reason}}
      end

    case result do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("self-monitoring alert delivery failed: #{inspect(reason)}; #{alert.message}")
    end
  end

  defp condition_name({:plugin_failure, _}), do: "plugin-failure"
  defp condition_name(condition), do: Atom.to_string(condition) |> String.replace("_", "-")

  defp normalize_runners(runners) when is_map(runners),
    do: Map.new(runners, fn {plugin, runner} -> {plugin_key(plugin), runner} end)

  defp normalize_runners(runners) when is_list(runners), do: normalize_runners(Map.new(runners))
  defp normalize_runners(_), do: %{}

  defp plugin_key(%{name: name}), do: plugin_key(name)
  defp plugin_key({name, _config}), do: plugin_key(name)

  defp plugin_key(plugin) when is_atom(plugin) do
    name = Atom.to_string(plugin)

    if String.starts_with?(name, "Elixir.") do
      plugin |> Module.split() |> List.last() |> Macro.underscore()
    else
      name
    end
  end

  defp plugin_key(plugin) when is_binary(plugin), do: plugin
  defp plugin_key(plugin), do: to_string(plugin)

  defp canonical_plugin(plugin, configured) do
    key = plugin_key(plugin)

    Enum.find(configured, key, fn name ->
      String.ends_with?(key, "_" <> name)
    end)
  end

  defp positive(opts, key, default) do
    case Keyword.get(opts, key, default) do
      value when is_integer(value) and value > 0 -> value
      _ -> default
    end
  end

  defp clock(opts), do: Keyword.get(opts, :clock, fn -> System.monotonic_time(:millisecond) end)
  defp schedule_tick, do: Process.send_after(self(), :tick, @tick_ms)
end

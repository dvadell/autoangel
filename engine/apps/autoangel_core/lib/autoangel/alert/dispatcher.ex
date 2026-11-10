defmodule Autoangel.Alert.Dispatcher do
  @moduledoc "Serializes alert policy transitions and dispatches alerts asynchronously."

  use GenServer

  require Logger

  alias Autoangel.Alert.PluginRunner
  alias Autoangel.Alert.Policy

  @type option ::
          {:name, GenServer.server()}
          | {:state_store, GenServer.server()}
          | {:plugins, map() | list()}
          | {:cooldown_ms, pos_integer()}
          | {:now, (-> integer())}
          | {:policy_state, map()}
          | {:console_base_url, String.t() | nil}
          | {:max_candidates, pos_integer()}

  defstruct policy: nil,
            runners: %{},
            state_store: nil,
            now: &__MODULE__.current_time/0

  @spec current_time() :: integer()
  def current_time, do: System.system_time(:millisecond)

  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @spec event(GenServer.server(), tuple()) :: :ok
  def event(server \\ __MODULE__, event), do: GenServer.cast(server, {:event, event})

  @spec handle_event(GenServer.server(), tuple(), integer()) :: :ok
  def handle_event(server \\ __MODULE__, event, now_ms),
    do: GenServer.call(server, {:event, event, now_ms})

  @spec state(GenServer.server()) :: map()
  def state(server \\ __MODULE__), do: GenServer.call(server, :state)

  @impl true
  def init(opts) do
    cooldown = Keyword.get(opts, :cooldown_ms, 30 * 60 * 1_000)
    plugins = Keyword.get(opts, :plugins, %{})
    runners = normalize_runners(plugins)
    now = Keyword.get(opts, :now, &__MODULE__.current_time/0)
    incidents = restored_incidents(Keyword.get(opts, :state_store), now.())

    policy =
      opts
      |> Keyword.get(
        :policy_state,
        Policy.new(
          incidents: incidents,
          cooldown_ms: cooldown,
          plugins: Map.keys(runners),
          now_ms: now.()
        )
      )
      |> Map.put(:cooldown_ms, cooldown)
      |> Map.put(:plugins, Map.keys(runners))
      |> Map.put(:render, render_opts(opts))

    {:ok,
     %__MODULE__{
       policy: policy,
       runners: runners,
       state_store: Keyword.get(opts, :state_store),
       now: now
     }}
  end

  @impl true
  def handle_call(:state, _from, state), do: {:reply, state.policy, state}

  def handle_call({:event, event, now_ms}, _from, state) do
    {policy, actions} = Policy.on_event(state.policy, event, now_ms)
    dispatch(actions, state)
    {:reply, :ok, %{state | policy: policy}}
  end

  @impl true
  def handle_cast({:event, event}, state) do
    {policy, actions} = Policy.on_event(state.policy, event, state.now.())
    dispatch(actions, state)
    {:noreply, %{state | policy: policy}}
  end

  # Alert content options (Requirements 26.2, 26.3, 26.5): the Console link is
  # added only when a base URL is configured; the number of Candidates per alert
  # is configurable.
  defp render_opts(opts) do
    [console_base_url: Keyword.get(opts, :console_base_url)]
    |> Keyword.merge(Keyword.take(opts, [:max_candidates]))
  end

  defp restored_incidents(nil, _now), do: %{}

  defp restored_incidents(store, _now) do
    case Autoangel.StateStore.records(store, :incident) do
      records when is_list(records) ->
        records

      {:error, reason} ->
        Logger.error("alert incident restore failed: #{inspect(reason)}")
        %{}
    end
  catch
    :exit, reason ->
      Logger.error("alert incident restore failed: #{inspect(reason)}")
      %{}
  end

  defp dispatch(actions, state) do
    Enum.each(actions, fn
      {:persist, {:open, incident}} ->
        persist(state, :put, incident)

      {:persist, {:close, incident}} ->
        persist(state, :put, incident)

      {:deliver, plugin, alert} ->
        deliver_one(state.runners, plugin, alert)

      {:deliver, alert} ->
        deliver(state.runners, alert)

      _ ->
        :ok
    end)
  end

  defp persist(%{state_store: nil}, _operation, _incident), do: :ok

  defp persist(%{state_store: store}, _operation, incident) do
    case Autoangel.StateStore.put_incident(store, incident.id, incident) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error(
          "alert incident persistence failed for #{inspect(incident.id)}: #{inspect(reason)}"
        )
    end
  catch
    :exit, reason ->
      Logger.error(
        "alert incident persistence failed for #{inspect(incident.id)}: #{inspect(reason)}"
      )
  end

  defp deliver(runners, alert) do
    Enum.each(runners, fn {name, _runner} -> deliver_one(runners, name, alert) end)
  end

  defp deliver_one(runners, name, alert) do
    case Map.get(runners, name) do
      nil ->
        :ok

      runner ->
        dispatch_to_runner(runner, alert)
    end
  end

  # A runner may be addressed either by pid or by a registered name (atom). Both
  # are valid `GenServer.server()` references for `PluginRunner.enqueue/2`, which
  # is a GenServer.call. When the target is a registered name that is not (yet)
  # alive, the call exits with `:noproc`; we catch that so a transient
  # unavailability never crashes the Dispatcher.
  defp dispatch_to_runner(runner, alert) when is_pid(runner) or is_atom(runner) do
    Task.start(fn ->
      try do
        _ = PluginRunner.enqueue(runner, alert)
      catch
        :exit, reason -> Logger.warning("alert plugin dispatch skipped: #{inspect(reason)}")
      end
    end)
  end

  defp dispatch_to_runner(_runner, _alert), do: :ok

  defp normalize_runners(runners) when is_map(runners), do: runners

  defp normalize_runners(runners) when is_list(runners) do
    Map.new(runners, &normalize_runner/1)
  end

  defp normalize_runners(_), do: %{}

  # Accepts the shapes a runners list may take: an explicit `{key, runner}`
  # pair (runner is a pid or registered name), a bare pid (keyed by itself), or
  # an enabled-plugin spec `%{name|module|config}` from which the policy key and
  # the registered PluginRunner name are derived identically to
  # `Autoangel.Alert.PluginSupervisor`.
  defp normalize_runner({key, runner}), do: {key, runner}
  defp normalize_runner(runner) when is_pid(runner), do: {runner, runner}

  defp normalize_runner(%{module: module} = _spec) when is_atom(module) do
    key = plugin_key(module)
    {key, Module.concat(Autoangel.Alert.PluginRunner, key)}
  end

  # The policy key for a plugin: the module's last segment, underscored and
  # atomized (e.g. `Autoangel.Plugin.Email` -> `:email`). This MUST match
  # `Autoangel.Alert.PluginSupervisor.plugin_name/1` so the registered runner
  # name the Dispatcher resolves is the one the supervisor started.
  defp plugin_key(module) do
    module |> Module.split() |> List.last() |> Macro.underscore() |> String.to_atom()
  end
end

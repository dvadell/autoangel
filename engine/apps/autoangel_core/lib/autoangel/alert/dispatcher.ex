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
      pid when is_pid(pid) ->
        Task.start(fn ->
          try do
            _ = PluginRunner.enqueue(pid, alert)
          catch
            :exit, reason -> Logger.warning("alert plugin dispatch skipped: #{inspect(reason)}")
          end
        end)

      _ ->
        :ok
    end
  end

  defp normalize_runners(runners) when is_map(runners), do: runners

  defp normalize_runners(runners) when is_list(runners) do
    Map.new(runners, fn
      {name, runner} -> {name, runner}
      runner when is_pid(runner) -> {runner, runner}
    end)
  end

  defp normalize_runners(_), do: %{}
end

defmodule Autoangel.Feedback do
  @moduledoc """
  Applies Feedback_Actions (Requirements 30.1, 30.2, 30.3, 30.6, 30.8, 43.3).

  `apply/2` takes an action (`"mute"` or `"this was normal"`) and a target
  (`%{series: %{name: ..., labels: %{...}}}` or `%{incident_id: ...}`) and:

    1. validates both and that the Engine holds the target, changing nothing on
       an invalid request;
    2. writes the change to the State_Store first;
    3. only then updates in-memory state (Dispatcher mutes, Worker overrides);
    4. replies.

  The State_Store write for one request is atomic (all affected Series or
  none), so on failure both the State_Store and memory are left unchanged.

  `mute` marks the Series or Incident muted. `this was normal` sets, for each
  affected Series, `override = min(current_or_base, max(base * 0.5, 0.25))`,
  where `base` is the global sensitivity (1.0 when unset). The target value is
  floored at 0.25, and the outer `min` makes the action idempotent and ensures
  it never raises sensitivity, even when `base` is already below the floor, so
  no Sample that did not alert before can alert after (Requirement 30.2). Requests are serialized by
  this process.
  """

  use GenServer

  require Logger

  alias Autoangel.Alert.Dispatcher
  alias Autoangel.{SeriesRegistry, StateStore, Workers}
  alias Autoangel.Workers.Worker

  @actions %{"mute" => :mute, "this was normal" => :normal}
  @floor 0.25

  @type target :: %{optional(atom() | String.t()) => term()}
  @type change :: %{action: :mute | :normal, target: term(), series: [term()]}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @doc "Applies a Feedback_Action; see the module documentation."
  @spec apply(GenServer.server(), term(), term()) ::
          {:ok, change()} | {:error, :invalid, atom()} | {:error, :store_failed}
  def apply(server \\ __MODULE__, action, target),
    do: GenServer.call(server, {:apply, action, target})

  @doc "Current in-memory per-Series overrides (series_id => sensitivity)."
  @spec overrides(GenServer.server()) :: %{binary() => float()}
  def overrides(server \\ __MODULE__), do: GenServer.call(server, :overrides)

  @impl true
  def init(opts) do
    store = Keyword.get(opts, :state_store, StateStore)

    state = %{
      store: store,
      base: number(Keyword.get(opts, :base_sensitivity), 1.0),
      lookup_series: Keyword.get(opts, :lookup_series, &SeriesRegistry.lookup/1),
      incident_series: Keyword.get(opts, :incident_series, &incident_series/1),
      apply_mute: Keyword.get(opts, :apply_mute, &Dispatcher.put_mute/1),
      apply_sensitivity: Keyword.get(opts, :apply_sensitivity, &apply_to_worker/2),
      overrides: restore_overrides(store)
    }

    {:ok, state}
  end

  @impl true
  def handle_call(:overrides, _from, state), do: {:reply, state.overrides, state}

  def handle_call({:apply, action, target}, _from, state) do
    with {:ok, action} <- validate_action(action),
         {:ok, resolved} <- safe_resolve(target, state) do
      commit(action, resolved, state)
    else
      {:error, :invalid, field} -> {:reply, {:error, :invalid, field}, state}
    end
  end

  # --- validation -----------------------------------------------------------

  defp validate_action(action) when is_binary(action) do
    case Map.fetch(@actions, action) do
      {:ok, atom} -> {:ok, atom}
      :error -> {:error, :invalid, :action}
    end
  end

  defp validate_action(_), do: {:error, :invalid, :action}

  # A target resolves to `{mute_target, [series_refs]}`.
  defp resolve_target(target, state) when is_map(target) do
    cond do
      series = field(target, :series) -> resolve_series(series, state)
      has_field?(target, :incident_id) -> resolve_incident(field(target, :incident_id), state)
      true -> {:error, :invalid, :target}
    end
  end

  defp resolve_target(_target, _state), do: {:error, :invalid, :target}

  defp safe_resolve(target, state) do
    resolve_target(target, state)
  rescue
    exception ->
      Logger.warning("invalid feedback target: #{Exception.message(exception)}")
      {:error, :invalid, :target}
  end

  defp resolve_series(series, state) when is_map(series) do
    with name when is_binary(name) and name != "" <- field(series, :name),
         labels when is_map(labels) <- field(series, :labels) || %{},
         true <- string_labels?(labels),
         key = SeriesRegistry.canonical_key(name, labels),
         {:ok, ref} <- safe_lookup(state, key) do
      {:ok, {{:series, key}, [ref]}}
    else
      _ -> {:error, :invalid, :target}
    end
  end

  defp resolve_series(_series, _state), do: {:error, :invalid, :target}

  # Label names and values must be strings; anything else (numbers, nested
  # maps) is an invalid target rather than a crash (Requirement 30.3).
  defp string_labels?(labels),
    do: Enum.all?(labels, fn {k, v} -> is_binary(k) and is_binary(v) end)

  defp resolve_incident(id, state) do
    with {:ok, incident_id} <- parse_incident_id(id),
         {:ok, keys} when keys != [] <- safe_call(fn -> state.incident_series.(incident_id) end),
         refs = Enum.flat_map(keys, &series_ref(state, &1)),
         false <- refs == [] do
      {:ok, {incident_id, refs}}
    else
      _ -> {:error, :invalid, :target}
    end
  end

  defp series_ref(state, key) do
    case safe_lookup(state, key) do
      {:ok, ref} -> [ref]
      _ -> []
    end
  end

  defp parse_incident_id({:incident, n} = id) when is_integer(n), do: {:ok, id}
  defp parse_incident_id(n) when is_integer(n) and n > 0, do: {:ok, {:incident, n}}

  defp parse_incident_id(s) when is_binary(s) do
    case Integer.parse(s) do
      {n, ""} when n > 0 -> {:ok, {:incident, n}}
      _ -> :error
    end
  end

  defp parse_incident_id(_), do: :error

  # --- commit: State_Store first, then memory -------------------------------

  defp commit(:mute, {mute_target, refs}, state) do
    case store(fn -> StateStore.put_mute(state.store, mute_target, true) end) do
      :ok ->
        :ok = safe_apply(fn -> state.apply_mute.(mute_target) end)
        reply(:mute, mute_target, refs, state)

      :error ->
        {:reply, {:error, :store_failed}, state}
    end
  end

  defp commit(:normal, {target, refs}, state) do
    new_values =
      Map.new(refs, fn ref ->
        current = Map.get(state.overrides, ref.id, state.base)
        {ref.id, min(current, max(state.base * 0.5, @floor)) * 1.0}
      end)

    case write_overrides(state, new_values) do
      :ok ->
        Enum.each(refs, &apply_override(state, &1, Map.fetch!(new_values, &1.id)))
        reply(:normal, target, refs, %{state | overrides: Map.merge(state.overrides, new_values)})

      :error ->
        {:reply, {:error, :store_failed}, state}
    end
  end

  defp apply_override(state, ref, value),
    do: safe_apply(fn -> state.apply_sensitivity.(ref, value) end)

  defp reply(action, target, refs, state) do
    {:reply, {:ok, %{action: action, target: target, series: Enum.map(refs, & &1.key)}}, state}
  end

  # All overrides of one request are written in a single atomic State_Store
  # operation, so a failure leaves the store exactly as it was (Req 30.8).
  defp write_overrides(state, new_values),
    do: store(fn -> StateStore.put_sensitivities(state.store, Map.to_list(new_values)) end)

  # A State_Store write counts as successful only on `:ok`; an error reply or
  # an unavailable store is a failure (Requirement 30.8).
  defp store(fun) do
    case fun.() do
      :ok -> :ok
      other -> store_failed(other)
    end
  catch
    :exit, reason -> store_failed({:exit, reason})
  end

  defp store_failed(reason) do
    Logger.error("feedback State_Store write failed: #{inspect(reason)}")
    :error
  end

  # --- defaults -------------------------------------------------------------

  defp incident_series(incident_id) do
    # Only open Incidents are targets; closed ones are not held after a restart.
    case Map.fetch(Dispatcher.state().incidents, incident_id) do
      {:ok, %{open?: true} = incident} ->
        {:ok, Map.get(incident, :series_keys, [incident.series_key])}

      _ ->
        :error
    end
  end

  # When the Worker does not hold the Series yet, it picks the persisted override
  # up when it creates the Series, so the change still takes effect.
  defp apply_to_worker(%{key: key, worker: index}, value) do
    case Workers.worker(index) do
      pid when is_pid(pid) -> Worker.set_sensitivity(pid, key, value)
      _ -> :error
    end
  end

  defp restore_overrides(store) do
    case StateStore.records(store, :sensitivity) do
      records when is_list(records) -> Map.new(records)
      {:error, reason} -> restore_failed(reason)
    end
  catch
    :exit, reason -> restore_failed(reason)
  end

  defp restore_failed(reason) do
    Logger.error("feedback override restore failed: #{inspect(reason)}")
    %{}
  end

  # --- helpers --------------------------------------------------------------

  defp safe_lookup(state, key), do: safe_call(fn -> state.lookup_series.(key) end)

  defp safe_call(fun) do
    fun.()
  catch
    :exit, _ -> :error
  end

  defp safe_apply(fun) do
    _ = fun.()
    :ok
  catch
    :exit, reason ->
      Logger.error("feedback in-memory update failed: #{inspect(reason)}")
      :ok
  end

  defp field(map, key), do: Map.get(map, key) || Map.get(map, Atom.to_string(key))
  defp has_field?(map, key), do: Map.has_key?(map, key) or Map.has_key?(map, Atom.to_string(key))

  defp number(value, _default) when is_number(value) and value > 0, do: value * 1.0
  defp number(_value, default), do: default
end

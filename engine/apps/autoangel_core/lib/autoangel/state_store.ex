defmodule Autoangel.StateStore do
  @moduledoc """
  Durable local state for checkpoints and operator feedback.

  Checkpoints use a two-phase protocol: all records are written under a new
  checkpoint id, then `:current` is switched in one transaction. The previous
  checkpoint is removed only after the switch succeeds.
  """

  require Logger

  use GenServer

  @type checkpoint_entry :: {term(), term()}
  @type checkpoint :: %{required(:id) => term(), required(:entries) => [checkpoint_entry()]}

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @spec checkpoint(GenServer.server(), [checkpoint_entry()]) :: :ok | {:error, term()}
  def checkpoint(server \\ __MODULE__, entries),
    do: GenServer.call(server, {:checkpoint, entries}, :infinity)

  @spec restore(GenServer.server()) :: {:ok, checkpoint()} | :none | {:error, term()}
  def restore(server \\ __MODULE__), do: GenServer.call(server, :restore, :infinity)

  @spec put_incident(GenServer.server(), term(), term()) :: :ok | {:error, term()}
  def put_incident(server \\ __MODULE__, id, incident),
    do: put_record(server, {:incident, id}, incident)

  @spec delete_incident(GenServer.server(), term()) :: :ok | {:error, term()}
  def delete_incident(server \\ __MODULE__, id), do: delete_record(server, {:incident, id})

  @spec put_mute(GenServer.server(), term(), term()) :: :ok | {:error, term()}
  def put_mute(server \\ __MODULE__, id, mute), do: put_record(server, {:mute, id}, mute)

  @spec delete_mute(GenServer.server(), term()) :: :ok | {:error, term()}
  def delete_mute(server \\ __MODULE__, id), do: delete_record(server, {:mute, id})

  @spec put_sensitivity(GenServer.server(), term(), term()) :: :ok | {:error, term()}
  def put_sensitivity(server \\ __MODULE__, series_id, sensitivity),
    do: put_record(server, {:sensitivity, series_id}, sensitivity)

  @doc "Writes several sensitivity overrides atomically: all are stored or none."
  @spec put_sensitivities(GenServer.server(), [{term(), term()}]) :: :ok | {:error, term()}
  def put_sensitivities(server \\ __MODULE__, entries) when is_list(entries),
    do:
      GenServer.call(
        server,
        {:put_many, Enum.map(entries, fn {id, v} -> {{:sensitivity, id}, v} end)},
        :infinity
      )

  @spec delete_sensitivity(GenServer.server(), term()) :: :ok | {:error, term()}
  def delete_sensitivity(server \\ __MODULE__, series_id),
    do: delete_record(server, {:sensitivity, series_id})

  @spec get_record(GenServer.server(), term()) :: {:ok, term()} | :not_found | {:error, term()}
  def get_record(server \\ __MODULE__, key), do: GenServer.call(server, {:get, key})

  @spec records(GenServer.server(), atom()) :: [{term(), term()}] | {:error, term()}
  def records(server \\ __MODULE__, kind) when kind in [:incident, :mute, :sensitivity],
    do: GenServer.call(server, {:records, kind})

  @spec feedback(GenServer.server()) ::
          {:ok,
           %{
             incidents: [{term(), term()}],
             mutes: [{term(), term()}],
             sensitivities: [{term(), term()}]
           }}
          | {:error, term()}
  def feedback(server \\ __MODULE__) do
    with incidents when is_list(incidents) <- records(server, :incident),
         mutes when is_list(mutes) <- records(server, :mute),
         sensitivities when is_list(sensitivities) <- records(server, :sensitivity) do
      {:ok, %{incidents: incidents, mutes: mutes, sensitivities: sensitivities}}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @spec restore_worker(GenServer.server(), non_neg_integer()) ::
          {:ok, [checkpoint_entry()]} | :none | {:error, term()}
  def restore_worker(server \\ __MODULE__, worker_index),
    do: GenServer.call(server, {:restore_worker, worker_index}, :infinity)

  @impl true
  def init(opts) do
    path = Keyword.get(opts, :path)

    with true <- is_binary(path),
         :ok <- File.mkdir_p(path),
         {:ok, db} <- CubDB.start_link(data_dir: path, auto_file_sync: true) do
      {:ok, %{db: db}}
    else
      false -> {:stop, :path_required}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def terminate(_reason, %{db: db}) do
    CubDB.stop(db)
    :ok
  end

  @impl true
  def handle_call({:checkpoint, entries}, _from, state) do
    id = {:checkpoint, System.system_time(:microsecond), :crypto.strong_rand_bytes(16)}
    records = Enum.map(entries, fn {key, value} -> {{:checkpoint, id, key}, value} end)

    result =
      CubDB.transaction(state.db, fn tx ->
        tx = Enum.reduce(records, tx, fn {key, value}, tx -> CubDB.Tx.put(tx, key, value) end)
        {:commit, CubDB.Tx.put(tx, {:checkpoint, :current}, id), :ok}
      end)

    case result do
      :ok ->
        try do
          cleanup_checkpoint(state.db, id)
        rescue
          exception -> Logger.error("checkpoint cleanup failed: #{Exception.message(exception)}")
        end

        {:reply, :ok, state}

      {:error, reason} ->
        Logger.error("checkpoint write failed: #{inspect(reason)}")
        {:reply, {:error, reason}, state}
    end
  rescue
    exception ->
      Logger.error("checkpoint write failed: #{Exception.message(exception)}")
      {:reply, {:error, exception}, state}
  end

  def handle_call(:restore, _from, state) do
    case CubDB.get(state.db, {:checkpoint, :current}) do
      nil -> {:reply, :none, state}
      id -> {:reply, {:ok, %{id: id, entries: read_checkpoint(state.db, id)}}, state}
    end
  rescue
    exception -> {:reply, {:error, exception}, state}
  end

  def handle_call({:put, key, value}, _from, state) do
    {:reply, sync(fn -> CubDB.put(state.db, key, value) end), state}
  end

  def handle_call({:put_many, entries}, _from, state) do
    {:reply, sync(fn -> CubDB.put_multi(state.db, entries) end), state}
  end

  def handle_call({:delete, key}, _from, state) do
    {:reply, sync(fn -> CubDB.delete(state.db, key) end), state}
  end

  def handle_call({:get, key}, _from, state) do
    case CubDB.fetch(state.db, key) do
      :error -> {:reply, :not_found, state}
      {:ok, value} -> {:reply, {:ok, value}, state}
    end
  end

  def handle_call({:records, kind}, _from, state) do
    records =
      CubDB.select(state.db)
      |> Stream.filter(fn
        {{record_kind, _id}, _value} -> record_kind == kind
        _ -> false
      end)
      |> Enum.map(fn {{_record_kind, id}, value} -> {id, value} end)
      |> Enum.sort_by(fn {id, _value} -> :erlang.term_to_binary(id) end)

    {:reply, records, state}
  rescue
    exception -> {:reply, {:error, exception}, state}
  end

  def handle_call({:restore_worker, worker_index}, _from, state) do
    reply =
      case CubDB.get(state.db, {:checkpoint, :current}) do
        nil -> :none
        id -> {:ok, read_checkpoint_for_worker(state.db, id, worker_index)}
      end

    {:reply, reply, state}
  rescue
    exception -> {:reply, {:error, exception}, state}
  end

  defp put_record(server, key, value), do: GenServer.call(server, {:put, key, value}, :infinity)
  defp delete_record(server, key), do: GenServer.call(server, {:delete, key}, :infinity)

  defp sync(fun) do
    fun.()
  rescue
    exception -> {:error, exception}
  end

  defp read_checkpoint(db, id) do
    CubDB.select(db)
    |> Stream.filter(fn
      {{:checkpoint, checkpoint_id, _key}, _value} -> checkpoint_id == id
      _ -> false
    end)
    |> Enum.map(fn {{:checkpoint, _checkpoint_id, key}, value} -> {key, value} end)
    |> Enum.sort_by(fn {key, _value} -> :erlang.term_to_binary(key) end)
  end

  defp read_checkpoint_for_worker(db, id, worker_index) do
    read_checkpoint(db, id)
    |> Enum.filter(fn {_key, value} -> checkpoint_worker?(value, worker_index) end)
  end

  defp checkpoint_worker?({_version, _state_bin, meta_bin}, worker_index)
       when is_binary(meta_bin) do
    case safe_binary_to_term(meta_bin) do
      {:ok, %{worker_index: ^worker_index}} -> true
      {:ok, %{"worker_index" => ^worker_index}} -> true
      {:ok, _meta} -> false
      :error -> true
    end
  end

  defp checkpoint_worker?(_value, _worker_index), do: true

  defp safe_binary_to_term(binary) do
    {:ok, :erlang.binary_to_term(binary, [:safe])}
  rescue
    _ -> :error
  end

  defp cleanup_checkpoint(db, current_id) do
    CubDB.transaction(db, fn tx ->
      tx =
        CubDB.Tx.select(tx)
        |> Stream.filter(fn
          {{:checkpoint, checkpoint_id, _key}, _value} when checkpoint_id != current_id -> true
          _ -> false
        end)
        |> Enum.reduce(tx, fn {{:checkpoint, checkpoint_id, key}, _value}, tx ->
          CubDB.Tx.delete(tx, {:checkpoint, checkpoint_id, key})
        end)

      {:commit, tx, :ok}
    end)
  end
end

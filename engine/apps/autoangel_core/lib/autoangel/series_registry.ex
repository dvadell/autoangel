defmodule Autoangel.SeriesRegistry do
  @moduledoc "Canonical series identity, worker assignment, and cardinality admission."

  use GenServer
  import Bitwise
  require Logger

  @type series_key :: {String.t(), [{String.t(), String.t()}]}
  @type series_ref :: %{
          id: binary(),
          key: series_key(),
          worker: non_neg_integer(),
          kind: :counter | :gauge
        }
  @default_namespace <<0x6B, 0xA7, 0xB8, 0x10, 0x9D, 0xAD, 0x11, 0xD1, 0x80, 0xB4, 0x00, 0xC0, 0x4F,
                       0xD4, 0x30, 0xC8>>

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @spec canonical_key(String.t(), map() | [{String.t(), String.t()}]) :: series_key()
  def canonical_key(name, labels) when is_binary(name), do: {name, normalize_labels(labels)}

  @spec canonical_key(%{name: String.t(), labels: map() | [{String.t(), String.t()}]}) ::
          series_key()
  def canonical_key(%{name: name, labels: labels}), do: canonical_key(name, labels)

  @doc "Encodes a canonical key with an explicit, version-independent length-prefixed format."
  @spec canonical_binary(series_key()) :: binary()
  def canonical_binary({name, labels}) when is_binary(name) and is_list(labels) do
    label_bytes =
      Enum.map(labels, fn {label_name, label_value} ->
        <<byte_size(label_name)::32, label_name::binary, byte_size(label_value)::32,
          label_value::binary>>
      end)

    [<<1, byte_size(name)::32, name::binary, length(labels)::32>>, label_bytes]
    |> IO.iodata_to_binary()
  end

  @spec series_id(series_key(), binary()) :: <<_::128>>
  def series_id(key, namespace \\ @default_namespace)
      when is_binary(namespace) and byte_size(namespace) == 16 do
    <<a::32, b::16, c::16, d::16, e::48, _::binary>> =
      :crypto.hash(:sha, namespace <> canonical_binary(key))

    <<a::32, b::16, (c &&& 0x0FFF) ||| 0x5000::16, (d &&& 0x3FFF) ||| 0x8000::16, e::48>>
  end

  @spec worker_index(series_key(), pos_integer()) :: non_neg_integer()
  def worker_index(key, pool_size) when is_integer(pool_size) and pool_size > 0,
    do: :erlang.phash2(canonical_binary(key), pool_size)

  @spec resolve_or_create(series_key() | map(), Autoangel.SeriesKind.type_hint()) ::
          {:existing, series_ref()} | {:created, series_ref()} | {:rejected, :per_metric | :total}
  def resolve_or_create(key_or_sample, type_hint \\ nil) do
    GenServer.call(__MODULE__, {:resolve, key_from(key_or_sample), type_hint})
  end

  @spec count() :: non_neg_integer()
  def count, do: GenServer.call(__MODULE__, :count)

  @spec rejection_counts() :: %{per_metric: non_neg_integer(), total: non_neg_integer()}
  def rejection_counts, do: GenServer.call(__MODULE__, :rejection_counts)

  @spec metrics() :: %{
          series: non_neg_integer(),
          rejected_series: %{per_metric: non_neg_integer(), total: non_neg_integer()}
        }
  def metrics do
    %{series: count(), rejected_series: rejection_counts()}
  end

  @impl GenServer
  def init(opts) do
    per_metric = Keyword.get(opts, :per_metric_limit, 2_000)
    total = Keyword.get(opts, :total_limit, 50_000)
    pool_size = Keyword.get(opts, :pool_size, System.schedulers_online())
    namespace = Keyword.get(opts, :namespace, @default_namespace)

    if valid_options?(per_metric, total, pool_size, namespace) do
      keys = :ets.new(:autoangel_series_keys, [:set, :protected])
      metric_counts = :ets.new(:autoangel_series_metric_counts, [:set, :protected])
      warnings = :ets.new(:autoangel_series_warning_rate, [:set, :protected])
      Autoangel.Metrics.set(:series_count, 0)

      {:ok,
       %{
         keys: keys,
         metric_counts: metric_counts,
         warnings: warnings,
         total: :atomics.new(1, signed: true),
         rejected: :atomics.new(2, signed: true),
         per_metric: per_metric,
         total_limit: total,
         pool_size: pool_size,
         namespace: namespace
       }}
    else
      {:stop, {:invalid_limits, :cardinality}}
    end
  end

  @impl GenServer
  def handle_call({:resolve, key, type_hint}, _from, state) do
    case :ets.lookup(state.keys, key) do
      [{^key, reference}] ->
        {:reply, {:existing, update_kind(state.keys, key, reference, type_hint)}, state}

      [] ->
        admit(key, type_hint, state)
    end
  end

  def handle_call(:count, _from, state), do: {:reply, :atomics.get(state.total, 1), state}

  def handle_call(:rejection_counts, _from, state) do
    {:reply, %{per_metric: :atomics.get(state.rejected, 1), total: :atomics.get(state.rejected, 2)},
     state}
  end

  defp admit({metric, _labels} = key, type_hint, state) do
    total_value = :atomics.get(state.total, 1)
    metric_counter = lookup_metric_counter(state, metric)
    metric_value = if metric_counter, do: :atomics.get(metric_counter, 1), else: 0

    cond do
      total_value >= state.total_limit ->
        reject(metric, :total, state)

      metric_value >= state.per_metric ->
        reject(metric, :per_metric, state)

      true ->
        metric_counter = metric_counter || new_metric_counter(state, metric)
        :atomics.add(metric_counter, 1, 1)
        :atomics.add(state.total, 1, 1)

        if :ets.insert_new(state.keys, {key, reference(key, type_hint, state)}) do
          [{^key, reference}] = :ets.lookup(state.keys, key)
          Autoangel.Metrics.set(:series_count, :atomics.get(state.total, 1))
          {:reply, {:created, reference}, state}
        else
          :atomics.add(metric_counter, 1, -1)
          :atomics.add(state.total, 1, -1)
          [{^key, reference}] = :ets.lookup(state.keys, key)
          {:reply, {:existing, reference}, state}
        end
    end
  end

  defp reference({name, labels} = key, type_hint, state) do
    %{
      id: series_id(key, state.namespace),
      key: key,
      worker: worker_index(key, state.pool_size),
      kind: Autoangel.SeriesKind.infer(name, type_hint, Map.new(labels))
    }
  end

  defp lookup_metric_counter(state, metric) do
    case :ets.lookup(state.metric_counts, metric) do
      [{^metric, counter}] -> counter
      [] -> nil
    end
  end

  defp new_metric_counter(state, metric) do
    counter = :atomics.new(1, signed: true)
    :ets.insert(state.metric_counts, {metric, counter})
    counter
  end

  defp update_kind(table, key, reference, type_hint) do
    if type_hint in [:counter, :gauge, :histogram, :summary] do
      updated = update_kind_for_hint(reference, type_hint)
      :ets.insert(table, {key, updated})
      updated
    else
      reference
    end
  end

  defp update_kind_for_hint(reference, type_hint) do
    {name, labels} = reference.key
    %{reference | kind: Autoangel.SeriesKind.infer(name, type_hint, Map.new(labels))}
  end

  defp reject(metric, limit, state) do
    position = if limit == :per_metric, do: 1, else: 2
    :atomics.add(state.rejected, position, 1)
    Autoangel.Metrics.increment({:rejected_series, limit})
    warn_rejection(metric, limit, state.warnings)
    {:reply, {:rejected, limit}, state}
  end

  defp warn_rejection(metric, limit, warnings) do
    now = System.monotonic_time(:millisecond)
    key = limit

    previous =
      case :ets.lookup(warnings, key) do
        [{^key, timestamp}] -> timestamp
        [] -> nil
      end

    if is_nil(previous) or now - previous >= 60_000 do
      :ets.insert(warnings, {key, now})
      Logger.warning("series cardinality limit exceeded for metric #{inspect(metric)}: #{limit}")
    end
  end

  defp key_from(%{name: name, labels: labels}), do: canonical_key(name, labels)
  defp key_from({name, labels}), do: canonical_key(name, labels)

  defp normalize_labels(labels) when is_map(labels),
    do: labels |> Map.to_list() |> normalize_labels()

  defp normalize_labels(labels) when is_list(labels),
    do:
      labels |> Enum.map(fn {name, value} -> {to_string(name), to_string(value)} end) |> Enum.sort()

  defp valid_options?(per_metric, total, pool_size, namespace),
    do:
      Enum.all?([per_metric, total, pool_size], &(is_integer(&1) and &1 > 0)) and
        is_binary(namespace) and byte_size(namespace) == 16
end

defmodule Autoangel.Metrics do
  @moduledoc "Process-wide counters and histograms exported by the Control API."

  @table :autoangel_metrics
  @histograms %{
    detection_latency_seconds: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
    nif_call_duration_seconds: [0.0001, 0.001, 0.005, 0.01, 0.1, 1.0]
  }

  @started_at {__MODULE__, :started_at}

  @spec ensure_started() :: :ok
  def ensure_started do
    ensure_started_at()

    if :ets.whereis(@table) == :undefined do
      try do
        :ets.new(@table, [
          :named_table,
          :public,
          :set,
          {:heir, Process.whereis(:init), :none},
          read_concurrency: true
        ])
      rescue
        ArgumentError -> :ok
      end
    end

    :ok
  end

  @spec increment(term(), non_neg_integer()) :: :ok
  def increment(name, amount \\ 1) when is_integer(amount) do
    ensure_started()
    :ets.update_counter(@table, {:counter, name}, {2, amount}, {{:counter, name}, 0})
    :ok
  end

  @spec observe(atom(), number()) :: :ok
  def observe(name, value) when is_atom(name) and is_number(value) and value >= 0 do
    ensure_started()
    micros = round(value * 1_000_000)
    :ets.update_counter(@table, {:sum, name}, {2, micros}, {{:sum, name}, 0})
    :ets.update_counter(@table, {:count, name}, {2, 1}, {{:count, name}, 0})

    Enum.each(Map.get(@histograms, name, []), fn bucket ->
      if value <= bucket,
        do:
          :ets.update_counter(@table, {:bucket, name, bucket}, {2, 1}, {{:bucket, name, bucket}, 0})
    end)

    :ok
  end

  @spec plugin_delivery(term(), atom()) :: :ok
  def plugin_delivery(plugin, outcome) do
    increment({:plugin, plugin, outcome})
  end

  @spec set(term(), number()) :: :ok
  def set(name, value) when is_number(value) do
    ensure_started()
    :ets.insert(@table, {{:gauge, name}, value})
    :ok
  end

  @spec gauge(term()) :: number()
  def gauge(name) do
    ensure_started()

    case :ets.lookup(@table, {:gauge, name}) do
      [{{:gauge, ^name}, value}] -> value
      _ -> 0
    end
  end

  @spec counter(term()) :: non_neg_integer()
  def counter(name) do
    ensure_started()

    case :ets.lookup(@table, {:counter, name}) do
      [{{:counter, ^name}, value}] -> value
      _ -> 0
    end
  end

  @spec rate(term()) :: float()
  def rate(name) do
    ensure_started()
    elapsed = max(System.monotonic_time(:millisecond) - started_at(), 1) / 1_000
    counter(name) / elapsed
  end

  @spec uptime_seconds() :: float()
  def uptime_seconds do
    ensure_started()
    max(System.monotonic_time(:millisecond) - started_at(), 0) / 1_000
  end

  @spec snapshot() :: map()
  def snapshot do
    ensure_started()

    @table
    |> :ets.tab2list()
    |> Map.new(fn {key, value} -> {key, value} end)
  end

  @spec families() :: [Autoangel.Exposition.Family.t()]
  def families do
    entries = snapshot()

    entries
    |> counters()
    |> Enum.map(fn {name, value} -> family(name, value) end)
    |> Kernel.++(gauges(entries))
    |> Kernel.++(histogram_families())
    |> Enum.reduce(%{}, fn family, acc ->
      Map.update(acc, family.name, family, fn existing ->
        %{existing | samples: existing.samples ++ family.samples}
      end)
    end)
    |> Map.values()
    |> Enum.sort_by(& &1.name)
  end

  defp counters(entries) do
    for {{:counter, name}, value} <- entries,
        not (is_tuple(name) and tuple_size(name) > 0 and elem(name, 0) == :storage),
        do: {name, value}
  end

  defp gauges(entries) do
    for {{:gauge, name}, value} <- entries, do: gauge_family(name, value)
  end

  defp gauge_family(name, value),
    do: %Autoangel.Exposition.Family{
      name: "autoangel_" <> Atom.to_string(name),
      type: :gauge,
      samples: [
        %Autoangel.Exposition.Sample{name: "autoangel_" <> Atom.to_string(name), value: value * 1.0}
      ]
    }

  defp histogram_families do
    Enum.flat_map(@histograms, fn {name, buckets} ->
      count = lookup({:count, name})
      sum = lookup({:sum, name}) / 1_000_000
      bucket_samples = Enum.map(buckets, &lookup({:bucket, name, &1}))
      values = Enum.zip(buckets, bucket_samples)

      samples =
        Enum.map(values, fn {bucket, value} ->
          %Autoangel.Exposition.Sample{
            name: "autoangel_" <> Atom.to_string(name) <> "_bucket",
            labels: %{"le" => format_bucket(bucket)},
            value: value * 1.0
          }
        end)

      samples =
        samples ++
          [
            %Autoangel.Exposition.Sample{
              name: "autoangel_" <> Atom.to_string(name) <> "_bucket",
              labels: %{"le" => "+Inf"},
              value: count * 1.0
            },
            %Autoangel.Exposition.Sample{
              name: "autoangel_" <> Atom.to_string(name) <> "_count",
              value: count * 1.0
            },
            %Autoangel.Exposition.Sample{
              name: "autoangel_" <> Atom.to_string(name) <> "_sum",
              value: sum * 1.0
            }
          ]

      [
        %Autoangel.Exposition.Family{
          name: "autoangel_" <> Atom.to_string(name),
          type: :histogram,
          samples: samples
        }
      ]
    end)
  end

  defp family({:plugin, plugin, outcome}, value),
    do: %Autoangel.Exposition.Family{
      name: "autoangel_plugin_deliveries_total",
      type: :counter,
      samples: [
        %Autoangel.Exposition.Sample{
          name: "autoangel_plugin_deliveries_total",
          labels: %{"plugin" => plugin_name(plugin), "outcome" => Atom.to_string(outcome)},
          value: value * 1.0
        }
      ]
    }

  defp family({:rejected_series, limit}, value),
    do: %Autoangel.Exposition.Family{
      name: "autoangel_rejected_series_total",
      type: :counter,
      samples: [
        %Autoangel.Exposition.Sample{
          name: "autoangel_rejected_series_total",
          labels: %{"limit" => Atom.to_string(limit)},
          value: value * 1.0
        }
      ]
    }

  defp family(name, value),
    do: %Autoangel.Exposition.Family{
      name: "autoangel_" <> Atom.to_string(name) <> "_total",
      type: :counter,
      samples: [
        %Autoangel.Exposition.Sample{
          name: "autoangel_" <> Atom.to_string(name) <> "_total",
          value: value * 1.0
        }
      ]
    }

  defp lookup(key) do
    case :ets.lookup(@table, key) do
      [{^key, value}] -> value
      _ -> 0
    end
  end

  defp plugin_name(plugin) when is_atom(plugin) do
    name = Atom.to_string(plugin)

    if String.starts_with?(name, "Elixir."),
      do: plugin |> Module.split() |> List.last() |> Macro.underscore(),
      else: name
  end

  defp plugin_name(plugin), do: to_string(plugin)
  defp format_bucket(value), do: :erlang.float_to_binary(value * 1.0, [:short])

  defp ensure_started_at do
    case :persistent_term.get(@started_at, nil) do
      nil -> :persistent_term.put(@started_at, System.monotonic_time(:millisecond))
      _value -> :ok
    end
  end

  defp started_at, do: :persistent_term.get(@started_at, System.monotonic_time(:millisecond))
end

defmodule Autoangel.Boot do
  @moduledoc "Shared boot phase used by readiness checks."
  @key {__MODULE__, :phase}

  def set_phase(phase) when phase in [:starting, :ready], do: :persistent_term.put(@key, phase)
  def phase, do: :persistent_term.get(@key, :starting)
  def ready?, do: phase() == :ready
end

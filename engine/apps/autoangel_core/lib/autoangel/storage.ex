defmodule Autoangel.Storage do
  @moduledoc """
  Behaviour and configuration boundary for optional storage backends.

  The core application refers to a backend only through this module. Backend
  modules are discovered and checked at runtime, which keeps detection,
  correlation, and alerting compilable when no storage application is present.
  """

  @type outcome :: :ok | {:retryable, term()} | {:permanent, term()}
  @type handle :: term()

  @callback init(config :: map()) :: {:ok, handle()} | {:error, term()}
  @callback append_samples(handle(), [term()]) :: outcome()
  @callback write_anomalies(handle(), [term()]) :: outcome()
  @callback write_series(handle(), [term()]) :: outcome()
  @callback write_bands(handle(), [term()]) :: outcome()
  @callback read_range(handle(), [term()], term(), term()) ::
              {:ok, [term()]} | {:retryable, term()} | {:permanent, term()}
  @callback read_aggregates(handle(), [term()], term(), term(), :minute | :hour) ::
              {:ok, [term()]} | {:retryable, term()} | {:permanent, term()}

  @callbacks [
    {:init, 1},
    {:append_samples, 2},
    {:write_anomalies, 2},
    {:write_series, 2},
    {:write_bands, 2},
    {:read_range, 4},
    {:read_aggregates, 5}
  ]

  @doc "Selects the only configured backend, or returns `:none` for none."
  @spec select_backend(map() | keyword()) :: {:ok, :none | module()} | {:error, term()}
  def select_backend(config) when is_list(config) do
    if Keyword.keyword?(config),
      do: config |> Map.new() |> select_backend(),
      else: {:error, :invalid_storage_configuration}
  end

  def select_backend(config) when is_map(config) do
    with {:ok, storage} <- storage_config(config),
         backends = Map.get(storage, :backends, :not_configured),
         backend = Map.get(storage, :backend, :none),
         {:ok, configured} <- configured_backends(backend, backends),
         :ok <- validate_backend_count(configured),
         {:ok, selected} <- resolve_backend(List.first(configured)),
         :ok <- validate_callbacks(selected) do
      {:ok, selected}
    end
  end

  def select_backend(_config), do: {:error, :invalid_storage_configuration}

  @doc "Returns an error for an invalid backend without starting it."
  @spec validate_backend(module()) :: :ok | {:error, term()}
  def validate_backend(module), do: validate_callbacks(module)

  defp storage_config(%{storage: storage}) when is_map(storage), do: {:ok, storage}

  defp storage_config(%{storage: storage}) when is_list(storage) do
    if Keyword.keyword?(storage),
      do: {:ok, Map.new(storage)},
      else: {:error, :invalid_storage_configuration}
  end

  defp storage_config(%{storage: _storage}), do: {:error, :invalid_storage_configuration}
  defp storage_config(config), do: {:ok, config}

  defp configured_backends(backend, :not_configured), do: configured_backends(backend)

  defp configured_backends(backend, backends) when is_list(backends) do
    if backend in [:none, nil, "none"] do
      {:ok, Enum.reject(backends, &(&1 in [:none, nil, "none"]))}
    else
      {:error, :multiple_backends}
    end
  end

  defp configured_backends(_backend, _backends), do: {:error, :invalid_backends}

  defp configured_backends(backend) when backend in [:none, nil, "none"], do: {:ok, []}
  defp configured_backends(backend) when is_atom(backend), do: {:ok, [backend]}
  defp configured_backends(backend) when is_binary(backend), do: {:ok, [backend]}
  defp configured_backends(_backend), do: {:error, :invalid_backend}

  defp validate_backend_count(backends) when length(backends) <= 1, do: :ok
  defp validate_backend_count(_backends), do: {:error, :multiple_backends}

  defp resolve_backend(nil), do: {:ok, :none}
  defp resolve_backend(:timescaledb), do: {:ok, Module.concat(["AutoangelTsdb"])}
  defp resolve_backend("none"), do: {:ok, :none}
  defp resolve_backend("timescaledb"), do: {:ok, Module.concat(["AutoangelTsdb"])}
  defp resolve_backend(module) when is_atom(module), do: {:ok, module}

  defp resolve_backend(name) when is_binary(name), do: {:error, {:unknown_backend, name}}
  defp resolve_backend(_backend), do: {:error, :invalid_backend}

  defp validate_callbacks(:none), do: :ok

  defp validate_callbacks(module) when is_atom(module) do
    case Code.ensure_loaded(module) do
      {:module, ^module} ->
        missing = Enum.reject(@callbacks, &function_exported?(module, elem(&1, 0), elem(&1, 1)))

        if missing == [], do: :ok, else: {:error, {:missing_callbacks, module, missing}}

      {:error, reason} ->
        {:error, {:backend_unavailable, module, reason}}
    end
  end

  defp validate_callbacks(_module), do: {:error, :invalid_backend}
end

defmodule Autoangel.Plugin.Registry do
  @moduledoc """
  Resolves and validates the compile-time plugin set.

  The registry is compiled from `:autoangel_core, :plugins` and never loads a
  module from configuration. Configuration can only enable a module already
  present in this fixed registry.
  """

  @compiled_plugins Application.compile_env(:autoangel_core, :plugins, [])

  @type plugin :: %{name: String.t(), module: module(), config: map()}

  @doc "Returns the plugin modules compiled into this release."
  @spec compiled_plugins() :: [{String.t(), module()}]
  def compiled_plugins do
    Enum.map(@compiled_plugins, &normalize_compiled/1)
  end

  @doc """
  Validates and returns the enabled plugins.

  Each enabled plugin's `validate_config/1` callback is called exactly once by
  this function. Errors contain only plugin and field names.
  """
  @spec validate(map()) :: {:ok, [plugin()]} | {:error, [String.t()]}
  def validate(config) when is_map(config) do
    configured = Map.get(config, :plugins, Map.get(config, "plugins", %{}))

    case enabled_plugins(configured) do
      {:error, errors} -> {:error, errors}
      {:ok, enabled} -> validate_available(enabled)
    end
  end

  def validate(_config), do: {:error, ["plugins: invalid configuration"]}

  defp validate_available(enabled) do
    available = Map.new(compiled_plugins())

    case unknown_plugins(enabled, available) do
      [] -> validate_enabled(enabled, available)
      errors -> {:error, errors}
    end
  end

  defp unknown_plugins(enabled, available) do
    enabled
    |> Enum.reject(fn {name, _plugin_config} -> Map.has_key?(available, name) end)
    |> Enum.map(fn {name, _} -> "plugins.#{name}: unknown plugin" end)
  end

  defp validate_enabled(enabled, available) do
    enabled
    |> Enum.map(fn {name, plugin_config} ->
      module = Map.fetch!(available, name)

      case safe_validate(module, plugin_config) do
        :ok -> {:ok, %{name: name, module: module, config: plugin_config}}
        {:error, fields} -> {:error, validation_errors(name, fields)}
      end
    end)
    |> collect_results()
  end

  defp collect_results(results) do
    {plugins, errors} =
      Enum.reduce(results, {[], []}, fn
        {:ok, plugin}, {plugins, errors} -> {[plugin | plugins], errors}
        {:error, plugin_errors}, {plugins, errors} -> {plugins, errors ++ plugin_errors}
      end)

    if errors == [], do: {:ok, Enum.reverse(plugins)}, else: {:error, errors}
  end

  defp safe_validate(module, config) do
    case module.validate_config(config) do
      :ok -> :ok
      {:error, fields} when is_list(fields) -> {:error, fields}
      {:error, _reason} -> {:error, [:configuration]}
      _other -> {:error, [:configuration]}
    end
  rescue
    _error -> {:error, [:configuration]}
  catch
    _kind, _reason -> {:error, [:configuration]}
  end

  defp validation_errors(name, fields) do
    fields
    |> Enum.map(&field_name/1)
    |> Enum.uniq()
    |> Enum.map(&"plugins.#{name}.#{&1}")
  end

  defp field_name({field, _reason}), do: field_name(field)
  defp field_name(field) when is_atom(field), do: Atom.to_string(field)
  defp field_name(field) when is_binary(field), do: field
  defp field_name(_field), do: "configuration"

  defp enabled_plugins(config) when is_map(config) do
    normalized_config = Map.new(config, fn {key, value} -> {normalize_name(key), value} end)
    explicit = Map.get(normalized_config, "enabled", :not_configured)
    explicit_names = explicit_names(explicit)
    names = Enum.uniq(explicit_names ++ section_names(normalized_config, explicit))

    case map_config_error(normalized_config, names, explicit) do
      nil -> {:ok, build_enabled(normalized_config, names, explicit_names)}
      error -> {:error, [error]}
    end
  end

  defp enabled_plugins(config) when is_list(config) do
    entries =
      Enum.map(config, fn
        name when is_atom(name) or is_binary(name) ->
          {:ok, {normalize_name(name), %{}}}

        %{name: name} = entry when is_atom(name) or is_binary(name) ->
          plugin_config = Map.get(entry, :config, Map.delete(entry, :name))

          if is_map(plugin_config),
            do: {:ok, {normalize_name(name), plugin_config}},
            else: {:error, "plugins: invalid configuration"}

        _ ->
          {:error, "plugins: invalid configuration"}
      end)

    with :ok <- validate_plugin_entries(entries),
         values <- Enum.map(entries, &elem(&1, 1)),
         true <- length(values) == length(Enum.uniq_by(values, &elem(&1, 0))) do
      {:ok, values}
    else
      false -> {:error, ["plugins: duplicate plugin"]}
      {:error, error} -> {:error, [error]}
    end
  end

  defp enabled_plugins(_config), do: {:error, ["plugins: invalid configuration"]}

  defp explicit_names(:not_configured), do: []
  defp explicit_names(explicit), do: Enum.map(List.wrap(explicit), &normalize_name/1)

  defp section_names(_normalized_config, explicit) when explicit != :not_configured, do: []

  defp section_names(normalized_config, _explicit),
    do: normalized_config |> Map.delete("enabled") |> Map.keys()

  defp map_config_error(normalized_config, names, explicit) do
    invalid_section =
      normalized_config
      |> Map.delete("enabled")
      |> Enum.find(fn {_name, value} -> not (is_map(value) or is_boolean(value)) end)

    cond do
      invalid_section != nil ->
        "plugins.#{elem(invalid_section, 0)}: invalid configuration"

      Enum.any?(names, &(&1 == "")) ->
        "plugins.enabled: invalid plugin name"

      not (explicit == :not_configured or is_list(explicit)) ->
        "plugins.enabled: invalid configuration"

      true ->
        nil
    end
  end

  defp build_enabled(normalized_config, names, explicit_names) do
    names
    |> Enum.map(fn name ->
      value = Map.get(normalized_config, name, false)
      {name, plugin_config_for(name, value, explicit_names)}
    end)
    |> Enum.reject(fn {_name, value} -> value == :disabled end)
  end

  defp plugin_config_for(name, value, explicit_names) do
    cond do
      name in explicit_names -> normalize_plugin_config(value)
      is_map(value) and Map.get(value, :enabled, true) == true -> Map.delete(value, :enabled)
      value == true -> %{}
      true -> :disabled
    end
  end

  defp validate_plugin_entries(entries),
    do:
      if(Enum.all?(entries, &match?({:ok, _}, &1)),
        do: :ok,
        else: {:error, "plugins: invalid configuration"}
      )

  defp normalize_plugin_config(config) when is_map(config), do: Map.delete(config, :enabled)
  defp normalize_plugin_config(_config), do: %{}

  defp normalize_compiled({name, module}) when is_atom(module),
    do: {normalize_name(name), module}

  defp normalize_compiled(module) when is_atom(module),
    do: {module_name(module), module}

  defp normalize_compiled(_invalid), do: {"", Autoangel.Plugin}

  defp normalize_name(name) when is_atom(name), do: Atom.to_string(name)
  defp normalize_name(name) when is_binary(name), do: name
  defp normalize_name(_name), do: ""

  defp module_name(module) do
    module
    |> Module.split()
    |> List.last()
    |> Macro.underscore()
  end
end

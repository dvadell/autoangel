defmodule Autoangel.Alert.PluginSupervisor do
  @moduledoc "Supervises isolated plugin runners and their task supervisor."

  use Supervisor

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []),
    do: Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))

  @impl true
  def init(opts) do
    task_sup = {Task.Supervisor, name: Autoangel.Alert.PluginTasks}
    plugins = Keyword.get(opts, :plugins, [])
    runner_opts = Keyword.drop(opts, [:plugins, :name])

    children = [
      task_sup
      | Enum.map(plugins, fn plugin_spec ->
          {plugin, config} = normalize(plugin_spec)

          name =
            Keyword.get(runner_opts, :name_prefix, Autoangel.Alert.PluginRunner)
            |> then(&Module.concat(&1, plugin_name(plugin)))

          opts_for_runner =
            Keyword.merge(runner_opts,
              plugin: plugin,
              config: config,
              task_supervisor: Autoangel.Alert.PluginTasks,
              name: name
            )

          Supervisor.child_spec({Autoangel.Alert.PluginRunner, opts_for_runner}, id: name)
        end)
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp normalize({plugin, config}) when is_atom(plugin) and is_map(config), do: {plugin, config}
  defp normalize(plugin) when is_atom(plugin), do: {plugin, %{}}
  defp normalize(%{module: plugin, config: config}), do: {plugin, config}

  defp plugin_name(plugin) do
    plugin |> Module.split() |> List.last() |> Macro.underscore() |> String.to_atom()
  end
end

defmodule EnginePluginEmail.MixProject do
  use Mix.Project

  def project do
    [
      app: :engine_plugin_email,
      version: "0.1.0",
      elixir: "~> 1.15",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application, do: [extra_applications: [:logger, :ssl]]

  defp deps do
    [{:autoangel_core, in_umbrella: true}]
  end
end

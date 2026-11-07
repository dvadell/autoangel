defmodule AutoangelCore.MixProject do
  use Mix.Project

  def project do
    [
      app: :autoangel_core,
      version: "0.1.0",
      elixir: "~> 1.15",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      dialyzer: [plt_add_apps: [:mix], flags: [:error_handling, :underspecs]],
      test_coverage: [tool: ExUnit]
    ]
  end

  def application do
    [mod: {Autoangel.Application, []}, extra_applications: [:logger, :inets]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false},
      {:stream_data, "~> 1.1", only: [:dev, :test]},
      {:cubdb, "~> 2.0"},
      {:plug, "~> 1.16"},
      {:bandit, "~> 1.6"}
    ]
  end
end

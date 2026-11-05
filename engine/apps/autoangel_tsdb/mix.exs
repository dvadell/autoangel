defmodule AutoangelTsdb.MixProject do
  use Mix.Project

  def project do
    [
      app: :autoangel_tsdb,
      version: "0.1.0",
      elixir: "~> 1.15",
      build_path: "../../_build",
      config_path: "../../config/config.exs",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      dialyzer: [plt_add_apps: [:mix], flags: [:error_handling, :underspecs]]
    ]
  end

  def application, do: [extra_applications: [:logger]]

  defp deps do
    [
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false},
      {:stream_data, "~> 1.1", only: [:dev, :test]},
      {:postgrex, "~> 0.19"},
      {:jason, "~> 1.4"},
      {:autoangel_core, in_umbrella: true}
    ]
  end
end

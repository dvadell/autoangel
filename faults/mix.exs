defmodule AutoangelFaults.MixProject do
  use Mix.Project

  def project do
    [
      app: :autoangel_faults,
      version: "0.1.0",
      elixir: "~> 1.18",
      deps: deps(),
      elixirc_paths: ["lib"],
      dialyzer: [
        plt_add_apps: [:mix, :autoangel_core, :autoangel_replay, :autoangel_tsdb],
        flags: [:error_handling, :underspecs]
      ]
    ]
  end

  def application, do: [extra_applications: [:logger, :inets]]

  defp deps do
    [
      # The Engine under test runs in-process, started by the replay harness.
      {:autoangel_replay, path: "../replay", runtime: false},
      {:autoangel_core, path: "../engine/apps/autoangel_core", runtime: false},
      {:autoangel_tsdb, path: "../engine/apps/autoangel_tsdb", runtime: false},
      {:postgrex, "~> 0.19"},
      {:jason, "~> 1.4"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false}
    ]
  end
end

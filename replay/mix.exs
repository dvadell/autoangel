defmodule AutoangelReplay.MixProject do
  use Mix.Project

  def project do
    [
      app: :autoangel_replay,
      version: "0.1.0",
      elixir: "~> 1.18",
      deps: deps(),
      elixirc_paths: ["lib"],
      dialyzer: [plt_add_apps: [:mix, :autoangel_core], flags: [:error_handling, :underspecs]]
    ]
  end

  def application, do: [extra_applications: [:logger, :inets]]

  defp deps do
    [
      # The Engine under test runs in-process. `runtime: false` keeps it from
      # starting with the harness; each replay starts it with its own config.
      {:autoangel_core, path: "../engine/apps/autoangel_core", runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false},
      {:stream_data, "~> 1.1", only: [:dev, :test]}
    ]
  end
end

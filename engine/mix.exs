defmodule Autoangel.Engine.MixProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      version: "0.1.0",
      start_permanent: Mix.env() == :prod,
      deps: [
        {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
        {:dialyxir, "~> 1.4", only: [:dev], runtime: false}
      ],
      dialyzer: [plt_add_apps: [:mix], flags: [:error_handling, :underspecs]]
    ]
  end
end

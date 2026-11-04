defmodule AutoangelConsole.MixProject do
  use Mix.Project

  def project do
    [
      app: :autoangel_console,
      version: "0.1.0",
      elixir: "~> 1.15",
      deps: deps(),
      dialyzer: [plt_add_apps: [:mix], flags: [:error_handling, :underspecs]]
    ]
  end

  defp deps do
    [
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false},
      {:stream_data, "~> 1.1", only: [:dev, :test]}
    ]
  end
end

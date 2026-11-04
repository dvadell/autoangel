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
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: [tool: ExUnit]
    ]
  end

  def application, do: [extra_applications: [:logger]]

  defp deps do
    [{:stream_data, "~> 1.1", only: [:dev, :test]}]
  end
end

defmodule AutoangelReplay.MixProject do
  use Mix.Project

  def project do
    [
      app: :autoangel_replay,
      version: "0.1.0",
      elixir: "~> 1.15",
      deps: deps()
    ]
  end

  defp deps do
    [{:stream_data, "~> 1.1", only: [:dev, :test]}]
  end
end

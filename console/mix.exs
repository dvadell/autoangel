defmodule AutoangelConsole.MixProject do
  use Mix.Project

  def project do
    [
      app: :autoangel_console,
      version: "0.1.0",
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      deps: deps(),
      releases: [
        autoangel_console: [
          include_executables_for: [:unix],
          applications: [runtime_tools: :permanent]
        ]
      ],
      dialyzer: [plt_add_apps: [:mix], flags: [:error_handling, :underspecs]]
    ]
  end

  def application do
    [mod: {Console.Application, []}, extra_applications: [:logger, :runtime_tools]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_env), do: ["lib"]

  # The Console is a separate release with no Rust, Rustler or NIF. The CI check
  # `scripts/console_no_rust_check.sh` keeps it that way.
  defp deps do
    [
      {:autoangel_schema, path: "../shared/autoangel_schema"},
      {:phoenix, "~> 1.8"},
      {:phoenix_live_view, "~> 1.1"},
      {:phoenix_html, "~> 4.2"},
      {:bandit, "~> 1.8"},
      {:ecto_sql, "~> 3.12"},
      {:postgrex, "~> 0.19"},
      {:jason, "~> 1.4"},
      {:plug_crypto, "~> 2.1"},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false},
      {:stream_data, "~> 1.1", only: [:dev, :test]}
    ]
  end
end

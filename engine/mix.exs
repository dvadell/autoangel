defmodule Autoangel.Engine.MixProject do
  use Mix.Project

  def project do
    [
      apps_path: "apps",
      version: "0.1.0",
      start_permanent: Mix.env() == :prod,
      releases: releases(),
      deps: [
        {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
        {:dialyxir, "~> 1.4", only: [:dev], runtime: false}
      ],
      dialyzer: [plt_add_apps: [:mix], flags: [:error_handling, :underspecs]]
    ]
  end

  # The Engine release. The plugin set is fixed in `config/prod.exs`; the
  # configuration file is read at start from AUTOANGEL_CONFIG (`config/runtime.exs`).
  # No Rust kernel is promoted (docs/decisions.md), so there is no NIF to package;
  # the application list below is the whole release.
  defp releases do
    [
      autoangel: [
        applications: [
          autoangel_core: :permanent,
          autoangel_tsdb: :permanent,
          engine_plugin_email: :permanent,
          engine_plugin_telegram: :permanent,
          engine_plugin_webhook: :permanent,
          engine_plugin_ntfy: :permanent,
          engine_plugin_slack: :permanent,
          engine_plugin_discord: :permanent,
          runtime_tools: :permanent
        ],
        include_executables_for: [:unix],
        strip_beams: true
      ]
    ]
  end
end

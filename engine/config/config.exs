import Config

config :stream_data, max_runs: 100

# Environment-specific configuration. Required so that the :test build can
# register a test-only plugin into the compile-time plugin set consumed by
# Autoangel.Plugin.Registry (`Application.compile_env(:autoangel_core, :plugins)`).
if config_env() == :test do
  import_config "test.exs"
end

# A release fixes its plugin set at build time (Requirement 22.2).
if config_env() == :prod do
  import_config "prod.exs"
end

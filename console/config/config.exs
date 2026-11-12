import Config

config :stream_data, max_runs: 100

# Settings come from the environment at start-up (see Console.Config).
config :autoangel_console, ecto_repos: []

config :autoangel_console, Console.Endpoint,
  render_errors: [formats: [html: Console.ErrorView, json: Console.ErrorView], layout: false]

config :phoenix, :json_library, Jason

import_config "#{config_env()}.exs"

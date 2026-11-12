import Config

# Tests start the Console pieces they need; the application does not boot a server.
config :autoangel_console, autostart: false

config :logger, level: :warning

config :autoangel_console,
  data: Console.Test.FakeData,
  engine_client: Console.Test.FakeEngine

config :autoangel_console, Console.Endpoint,
  secret_key_base: String.duplicate("t", 64),
  live_view: [signing_salt: "console-test-salt"],
  server: false

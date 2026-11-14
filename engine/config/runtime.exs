import Config

# The Engine reads its TOML configuration from the file named by AUTOANGEL_CONFIG.
# Secrets come from AUTOANGEL_* environment variables (`Autoangel.Config`).
# With no file, the defaults apply and the Engine refuses to start for lack of
# credentials, with a message naming each missing setting.
if config_env() == :prod do
  case System.get_env("AUTOANGEL_CONFIG") do
    nil -> :ok
    "" -> :ok
    path -> config :autoangel_core, :config, path
  end
end

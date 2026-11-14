import Config

# The plugin set of a release is fixed here, at build time. Configuration can
# only enable a plugin from this list; it never loads a module by name
# (`Autoangel.Plugin.Registry`).
config :autoangel_core, :plugins, [
  {"email", Autoangel.Plugin.Email},
  {"telegram", EnginePluginTelegram},
  {"webhook", EnginePluginWebhook},
  {"ntfy", EnginePluginNtfy},
  {"slack", EnginePluginSlack},
  {"discord", EnginePluginDiscord}
]

config :logger, level: :info

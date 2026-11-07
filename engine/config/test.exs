import Config

# Register a test-only counting plugin into the compile-time plugin set so that
# Autoangel.Plugin.Registry.validate/1 can actually invoke validate_config/1.
# The shape is {name, module}, matching normalize_compiled/1 in the registry.
config :autoangel_core, :plugins, [{"counting", Autoangel.Test.CountingPlugin}]

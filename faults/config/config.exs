import Config

# Compile-time plugin set of the Engine under test: the replay capture plugin
# plus a plugin that always fails (plugin fault test).
config :autoangel_core, :plugins, [
  {"capture", AutoangelReplay.CapturePlugin},
  {"failing", AutoangelFaults.FailingPlugin}
]

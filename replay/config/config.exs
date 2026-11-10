import Config

config :stream_data, max_runs: 100

# The replay-only capture plugin is registered in the Engine's compile-time
# plugin set; it exists only in the replay build.
config :autoangel_core, :plugins, [{"capture", AutoangelReplay.CapturePlugin}]

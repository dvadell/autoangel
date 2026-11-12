ExUnit.start()

# LiveView tests drive the Endpoint in-process (no listener: `server: false`).
{:ok, _} = Application.ensure_all_started(:phoenix)
{:ok, _} = Supervisor.start_link([Console.Endpoint], strategy: :one_for_one)

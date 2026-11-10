defmodule AutoangelTsdb.HeartbeatIntegrationTest do
  @moduledoc """
  Task 22.2 against real TimescaleDB (127.0.0.1:5433), skipping gracefully when
  unreachable. Each test uses its own schema through the connection's
  `search_path`, so the backend's own connections land there too.

  Covers Requirements 41.1 to 41.3: Heartbeat cadence, no Heartbeat before
  migrations succeed, and the Schema_Version after a no-op start.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Autoangel.Storage.Heartbeat
  alias AutoangelTsdb.Migrations

  @moduletag :integration

  @base [
    hostname: "127.0.0.1",
    port: 5433,
    username: "autoangel",
    password: "autoangel",
    database: "autoangel",
    connect_timeout: 2_000,
    timeout: 10_000
  ]

  setup do
    with true <- reachable?(),
         schema = "aa_hb_#{System.system_time(:microsecond)}_#{:erlang.phash2(make_ref())}",
         opts = @base ++ [parameters: [search_path: "#{schema},public"]],
         {:ok, admin} <- Postgrex.start_link(@base) do
      {:ok, _} = Postgrex.query(admin, "CREATE SCHEMA #{schema}", [])
      {:ok, conn} = Postgrex.start_link(opts)

      on_exit(fn ->
        with {:ok, c} <- Postgrex.start_link(@base) do
          _ = Postgrex.query(c, "DROP SCHEMA IF EXISTS #{schema} CASCADE", [])
          GenServer.stop(c)
        end
      end)

      {:ok, conn: conn, conn_opts: opts}
    else
      _ -> {:ok, skip: true}
    end
  end

  defp reachable? do
    case :gen_tcp.connect(~c"127.0.0.1", 5433, [], 1_000) do
      {:ok, socket} -> :gen_tcp.close(socket) == :ok
      _ -> false
    end
  end

  defp run(%{skip: true}, _fun), do: IO.puts("[integration] skipped: TimescaleDB unreachable")
  defp run(ctx, fun), do: fun.(ctx)

  defp eventually(fun, deadline_ms \\ 5_000) do
    cond do
      fun.() -> true
      deadline_ms <= 0 -> false
      true -> Process.sleep(25) && eventually(fun, deadline_ms - 25)
    end
  end

  defp connection_count(conn) do
    {:ok, %{rows: [[n]]}} =
      Postgrex.query(conn, "SELECT count(*) FROM pg_stat_activity WHERE usename = 'autoangel'", [])

    n
  end

  defp heartbeats(conn) do
    {:ok, %{rows: rows}} =
      Postgrex.query(conn, "SELECT ts, version FROM engine_status ORDER BY ts", [])

    rows
  end

  defp stored_version(conn) do
    {:ok, %{rows: [[v]]}} = Postgrex.query(conn, "SELECT version FROM schema_version", [])
    v
  end

  defp start_heartbeat(opts, interval_ms) do
    {:ok, pid} =
      Heartbeat.start_link(
        name: nil,
        backend: AutoangelTsdb,
        backend_config: %{connection: opts},
        interval_ms: interval_ms,
        version: "0.1.0-test"
      )

    pid
  end

  test "Heartbeats are written at the configured cadence with the Engine version", ctx do
    run(ctx, fn %{conn: conn, conn_opts: opts} ->
      # Migrate first so the cadence window measures only Heartbeat scheduling.
      {:ok, _} = AutoangelTsdb.init(%{connection: opts})
      pid = start_heartbeat(opts, 200)
      Process.sleep(1_100)
      GenServer.stop(pid)

      rows = heartbeats(conn)
      assert length(rows) in 4..7
      assert Enum.all?(rows, fn [_ts, v] -> v == "0.1.0-test" end)

      gaps =
        rows
        |> Enum.map(fn [ts, _] -> DateTime.to_unix(ts, :millisecond) end)
        |> Enum.chunk_every(2, 1, :discard)
        |> Enum.map(fn [a, b] -> b - a end)

      # Each Heartbeat follows the previous one within one interval plus slack.
      assert Enum.all?(gaps, &(&1 <= 200 + 200)), inspect(gaps)
    end)
  end

  test "no Heartbeat is written while migrations fail, then they start", ctx do
    run(ctx, fn %{conn: conn, conn_opts: opts} ->
      # A conflicting table makes migration version 1 fail deterministically.
      {:ok, _} = Postgrex.query(conn, "CREATE TABLE samples (x int)", [])

      log =
        capture_log(fn ->
          pid = start_heartbeat(opts, 100)
          Process.sleep(350)

          {:ok, %{rows: [[exists]]}} =
            Postgrex.query(
              conn,
              "SELECT to_regclass(current_schema() || '.engine_status') IS NOT NULL",
              []
            )

          refute exists, "no engine_status table, hence no Heartbeat, before migrations"
          assert Heartbeat.state(pid).written == 0

          # Remove the obstacle: the next interval migrates and writes.
          {:ok, _} = Postgrex.query(conn, "DROP TABLE samples", [])
          assert eventually(fn -> Heartbeat.state(pid).written >= 1 end)
          GenServer.stop(pid)
        end)

      assert log =~ "heartbeat not written"
      assert heartbeats(conn) != []
      assert stored_version(conn) == Migrations.version()
    end)
  end

  test "a failed init closes the connection it opened", ctx do
    run(ctx, fn %{conn: conn, conn_opts: opts} ->
      {:ok, _} = Postgrex.query(conn, "CREATE TABLE samples (x int)", [])
      before = connection_count(conn)

      for _ <- 1..5, do: assert({:error, _} = AutoangelTsdb.init(%{connection: opts}))

      assert eventually(fn -> connection_count(conn) <= before end)
    end)
  end

  test "a no-op start keeps the Schema_Version and the stored rows", ctx do
    run(ctx, fn %{conn: conn, conn_opts: opts} ->
      assert {:ok, handle} = AutoangelTsdb.init(%{connection: opts})
      assert stored_version(conn) == Migrations.version()
      assert :ok = AutoangelTsdb.write_heartbeat(handle, 1_700_000_000_000, "first")

      # Second start: nothing to migrate.
      assert {:ok, _handle} = AutoangelTsdb.init(%{connection: opts})
      assert stored_version(conn) == Migrations.version()
      assert [[_, "first"]] = heartbeats(conn)
    end)
  end

  test "Heartbeats older than one day are pruned", ctx do
    run(ctx, fn %{conn: conn, conn_opts: opts} ->
      {:ok, handle} = AutoangelTsdb.init(%{connection: opts})
      now = System.system_time(:millisecond)
      :ok = AutoangelTsdb.write_heartbeat(handle, now - 2 * 86_400_000, "old")
      :ok = AutoangelTsdb.write_heartbeat(handle, now - 23 * 3_600_000, "recent")

      pid = start_heartbeat(opts, 60_000)
      assert eventually(fn -> Heartbeat.state(pid).written >= 1 end)
      GenServer.stop(pid)

      versions = for [_, v] <- heartbeats(conn), do: v
      assert versions == ["recent", "0.1.0-test"]
    end)
  end
end

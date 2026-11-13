defmodule AutoangelFaults.Database do
  @moduledoc """
  A fresh TimescaleDB database per run, so a reference run and a fault run never
  share rows. The Engine reaches it through the Toxiproxy port; the harness
  inspects it directly on `AUTOANGEL_TEST_DB_PORT`.

  Databases are not removed one by one: the whole throw-away container goes away
  with `scripts/fault_env.sh down`.
  """

  @spec direct_opts(String.t()) :: keyword()
  def direct_opts(database) do
    [
      hostname: "127.0.0.1",
      port: String.to_integer(System.get_env("AUTOANGEL_TEST_DB_PORT", "5433")),
      username: "autoangel",
      password: "autoangel",
      database: database,
      timeout: 15_000
    ]
  end

  @spec create() :: String.t()
  def create do
    name = "aa_fault_#{System.unique_integer([:positive])}_#{System.os_time(:second)}"
    {:ok, admin} = Postgrex.start_link(direct_opts("autoangel"))
    Postgrex.query!(admin, "CREATE DATABASE #{name}", [])
    GenServer.stop(admin)
    name
  end

  @doc "Connection string the Engine uses (through the proxy)."
  @spec engine_url(String.t(), pos_integer()) :: String.t()
  def engine_url(name, port), do: "ecto://autoangel:autoangel@127.0.0.1:#{port}/#{name}"

  @doc "Row counts read directly from the database."
  @spec counts(String.t()) :: %{
          samples: integer(),
          distinct_samples: integer(),
          heartbeats: integer()
        }
  def counts(name) do
    {:ok, conn} = Postgrex.start_link(direct_opts(name))

    one = fn sql ->
      %{rows: [[n]]} = Postgrex.query!(conn, sql, [])
      n
    end

    result = %{
      samples: one.("SELECT count(*) FROM samples"),
      distinct_samples: one.("SELECT count(*) FROM (SELECT DISTINCT series_id, ts FROM samples) d"),
      heartbeats: one.("SELECT count(*) FROM engine_status")
    }

    GenServer.stop(conn)
    result
  end
end

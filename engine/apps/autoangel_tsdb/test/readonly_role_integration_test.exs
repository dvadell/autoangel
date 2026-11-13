defmodule AutoangelTsdb.ReadonlyRoleIntegrationTest do
  @moduledoc """
  Task 28.1 against real TimescaleDB (127.0.0.1, port from AUTOANGEL_TEST_DB_PORT, default 5433).
  Applies `docs/readonly-role.sql`, then checks the role reads every object and is
  refused every write and DDL statement (Requirements 42.1 to 42.3).
  """
  use ExUnit.Case, async: false

  alias AutoangelTsdb.Migrations

  @moduletag :integration

  @port String.to_integer(System.get_env("AUTOANGEL_TEST_DB_PORT", "5433"))
  @base [
    hostname: "127.0.0.1",
    port: @port,
    username: "autoangel",
    password: "autoangel",
    database: "autoangel",
    connect_timeout: 2_000,
    timeout: 15_000
  ]
  @sql_path Path.expand("../../../../docs/readonly-role.sql", __DIR__)
  @login "aa_ro_test_login"

  setup_all do
    case :gen_tcp.connect(~c"127.0.0.1", @port, [], 1_000) do
      {:ok, s} ->
        :gen_tcp.close(s)
        {:ok, admin} = Postgrex.start_link(@base)
        :ok = Migrations.run(admin)
        apply_sql(admin)
        Postgrex.query!(admin, "DROP ROLE IF EXISTS #{@login}", [])

        Postgrex.query!(
          admin,
          "CREATE ROLE #{@login} LOGIN PASSWORD 'ro' IN ROLE autoangel_readonly",
          []
        )

        {:ok, ro} = Postgrex.start_link(Keyword.merge(@base, username: @login, password: "ro"))

        on_exit(fn ->
          {:ok, c} = Postgrex.start_link(@base)
          Postgrex.query(c, "DROP OWNED BY #{@login}", [])
          Postgrex.query(c, "DROP ROLE IF EXISTS #{@login}", [])
        end)

        {:ok, admin: admin, ro: ro}

      _ ->
        IO.puts("[integration] skipped: TimescaleDB unreachable")
        {:ok, skip: true}
    end
  end

  defp apply_sql(conn) do
    @sql_path
    |> File.read!()
    |> String.split(~r/\n\s*\n/, trim: true)
    |> Enum.map(fn block ->
      block
      |> String.split("\n")
      |> Enum.reject(&String.starts_with?(&1, "--"))
      |> Enum.join("\n")
      |> String.trim()
    end)
    |> Enum.reject(&(&1 == ""))
    |> Enum.each(&Postgrex.query!(conn, &1, []))
  end

  defp objects(admin) do
    %{rows: rows} =
      Postgrex.query!(
        admin,
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type IN ('BASE TABLE', 'VIEW')
        ORDER BY 1
        """,
        []
      )

    Enum.map(rows, fn [n] -> n end)
  end

  test "the role reads every table and view", ctx do
    if ctx[:skip], do: :ok, else: read_all(ctx)
  end

  defp read_all(%{admin: admin, ro: ro}) do
    names = objects(admin)
    assert "samples" in names and "anomalies" in names and "samples_1m" in names

    for n <- names do
      assert {:ok, _} = Postgrex.query(ro, ~s(SELECT * FROM "#{n}" LIMIT 1), []),
             "cannot read #{n}"
    end
  end

  test "the role is refused every write and DDL statement", ctx do
    if ctx[:skip], do: :ok, else: refuse_all(ctx)
  end

  defp refuse_all(%{ro: ro}) do
    statements = [
      "INSERT INTO series (id, name, kind, status) VALUES (gen_random_uuid(), 'x', 'gauge', 'healthy')",
      "UPDATE series SET name = name",
      "DELETE FROM samples",
      "TRUNCATE samples",
      "CREATE TABLE aa_ro_forbidden (x int)",
      "DROP TABLE series",
      "ALTER TABLE series ADD COLUMN aa_x int",
      "CREATE INDEX aa_ro_idx ON series (id)",
      "CREATE SCHEMA aa_ro_schema",
      "CREATE ROLE aa_ro_other",
      "DROP MATERIALIZED VIEW samples_1m"
    ]

    for sql <- statements do
      assert {:error, %Postgrex.Error{postgres: %{code: code}}} = Postgrex.query(ro, sql, []),
             "was allowed: #{sql}"

      assert code in [:insufficient_privilege, :wrong_object_type], "#{sql} -> #{code}"
    end
  end

  test "tables created later are readable too", ctx do
    if ctx[:skip], do: :ok, else: future_tables(ctx)
  end

  defp future_tables(%{admin: admin, ro: ro}) do
    Postgrex.query!(admin, "CREATE TABLE aa_ro_future (x int)", [])

    try do
      assert {:ok, _} = Postgrex.query(ro, "SELECT * FROM aa_ro_future", [])
    after
      Postgrex.query!(admin, "DROP TABLE aa_ro_future", [])
    end
  end
end

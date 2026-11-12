defmodule Console.StartupTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Console.{Application, StartupChecks}

  # A fake read-only database: maps each SQL string to a result and records the calls.
  defp db(version_row, opts \\ []) do
    test = self()
    table = Keyword.get(opts, :table, [["schema_version"]])
    connect = Keyword.get(opts, :connect, {:ok, %{rows: [[1]]}})

    fn sql ->
      send(test, {:sql, sql})

      cond do
        sql == "SELECT 1" -> connect
        String.starts_with?(sql, "SELECT to_regclass") -> {:ok, %{rows: table}}
        String.starts_with?(sql, "SELECT version") -> {:ok, %{rows: version_row}}
      end
    end
  end

  test "passes on a supported version" do
    assert :ok = StartupChecks.run("ecto://db", db([[3]]))
  end

  test "database not configured" do
    assert {:error, :database_not_configured} = StartupChecks.run(nil, db([[3]]))
    refute_received {:sql, _}
  end

  test "connection failure" do
    fun = db([[3]], connect: {:error, :econnrefused})
    assert {:error, {:connection_failed, :econnrefused}} = StartupChecks.run("ecto://db", fun)
  end

  test "schema_version table missing" do
    fun = db([[3]], table: [[nil]])
    assert {:error, :schema_version_missing} = StartupChecks.run("ecto://db", fun)
  end

  test "schema_version table empty" do
    assert {:error, :schema_version_empty} = StartupChecks.run("ecto://db", db([]))
  end

  test "unsupported version reports found and supported" do
    assert {:error, {:unsupported_schema_version, 4, [3]} = reason} =
             StartupChecks.run("ecto://db", db([[4]]), [3])

    assert StartupChecks.describe(reason) =~ "found 4, supported 3"
  end

  test "only reads are issued" do
    StartupChecks.run("ecto://db", db([[3]]))

    for _ <- 1..3 do
      assert_received {:sql, sql}
      assert String.starts_with?(sql, "SELECT")
    end
  end

  describe "boot/3" do
    @env %{
      "AUTOANGEL_CONSOLE_TOKEN" => "tok",
      "AUTOANGEL_CONSOLE_SECRET_KEY_BASE" => String.duplicate("s", 64)
    }

    defp halting, do: fn code -> send(self(), {:halt, code}) && :halted end

    test "halts non-zero and logs the reason for an invalid configuration" do
      log =
        capture_log(fn ->
          assert :halted = Application.boot(%{}, halting())
        end)

      assert_received {:halt, 1}
      assert log =~ "Console cannot start"
      assert log =~ "needs credentials"
    end

    test "halts non-zero when the database is not configured" do
      log =
        capture_log(fn ->
          assert :halted = Application.boot(@env, halting(), name: :console_boot_test)
        end)

      assert_received {:halt, 1}
      assert log =~ "database is not configured"
    end
  end
end

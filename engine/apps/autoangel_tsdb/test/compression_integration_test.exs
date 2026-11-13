defmodule AutoangelTsdb.CompressionIntegrationTest do
  @moduledoc """
  Task 37.6 against real TimescaleDB (127.0.0.1, port from AUTOANGEL_TEST_DB_PORT,
  default 5433), skipping when unreachable. Each test uses its own schema.

  Covers Requirements 40.1 to 40.6: one compression policy with the configured
  age, replaced on change, a rejection that leaves the Engine running, and
  compressed reads equal to uncompressed reads.
  """
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias AutoangelTsdb.{Compression, Migrations}

  @moduletag :integration

  @port String.to_integer(System.get_env("AUTOANGEL_TEST_DB_PORT", "5433"))
  @base [
    hostname: "127.0.0.1",
    port: @port,
    username: "autoangel",
    password: "autoangel",
    database: "autoangel",
    connect_timeout: 2_000,
    timeout: 30_000
  ]
  @day 86_400_000

  setup do
    with {:ok, socket} <- :gen_tcp.connect(~c"127.0.0.1", @port, [], 1_000),
         :ok <- :gen_tcp.close(socket),
         schema = "aa_cmp_#{System.system_time(:microsecond)}_#{:erlang.phash2(make_ref())}",
         {:ok, admin} <- Postgrex.start_link(@base) do
      {:ok, _} = Postgrex.query(admin, "CREATE SCHEMA #{schema}", [])
      {:ok, conn} = Postgrex.start_link(@base ++ [parameters: [search_path: "#{schema},public"]])
      :ok = Migrations.run(conn)

      on_exit(fn ->
        with {:ok, c} <- Postgrex.start_link(@base) do
          _ = Postgrex.query(c, "DROP SCHEMA IF EXISTS #{schema} CASCADE", [])
        end
      end)

      {:ok, conn: conn, schema: schema}
    else
      _ -> {:ok, skip: true}
    end
  end

  defp run(%{skip: true}, _fun), do: IO.puts("[integration] skipped: TimescaleDB unreachable")
  defp run(ctx, fun), do: fun.(ctx)

  defp policies(conn, schema) do
    {:ok, %{rows: rows}} =
      Postgrex.query(
        conn,
        "SELECT config->>'compress_after' FROM timescaledb_information.jobs " <>
          "WHERE proc_name = 'policy_compression' AND hypertable_schema = $1 AND hypertable_name = 'samples'",
        [schema]
      )

    Enum.map(rows, fn [after_] -> after_ end)
  end

  test "the default age is one day and the policy is exactly one", ctx do
    run(ctx, fn %{conn: conn, schema: schema} ->
      assert :ok = Compression.apply_policy(conn, %{})
      assert ["1 day"] == policies(conn, schema)

      # Applying again does not add a second policy.
      assert :ok = Compression.apply_policy(conn, %{compression: %{age: 86_400}})
      assert ["1 day"] == policies(conn, schema)
    end)
  end

  test "a changed age replaces the earlier policy", ctx do
    run(ctx, fn %{conn: conn, schema: schema} ->
      assert :ok = Compression.apply_policy(conn, %{compression: %{age: 86_400}})
      assert :ok = Compression.apply_policy(conn, %{compression: %{age: 3 * 86_400}})
      assert ["3 days"] == policies(conn, schema)

      assert :ok = Compression.apply_policy(conn, %{compression: %{age: 7_200}})
      assert ["02:00:00"] == policies(conn, schema) or ["2 hours"] == policies(conn, schema)
    end)
  end

  test "initialising the backend applies the policy with the configured age", ctx do
    run(ctx, fn %{conn: conn, schema: schema} ->
      assert {:ok, %{ready: true}} =
               AutoangelTsdb.init(%{conn: conn, compression: %{age: 2 * 86_400}})

      assert ["2 days"] == policies(conn, schema)
    end)
  end

  test "a rejection is logged and the Engine continues uncompressed", ctx do
    run(ctx, fn %{conn: conn, schema: schema} ->
      rejecting = fn _conn, sql, _params ->
        if sql =~ "add_compression_policy",
          do: {:error, %Postgrex.Error{message: "rejected"}},
          else: {:ok, :done}
      end

      log =
        capture_log(fn ->
          assert :ok = Compression.apply_or_continue(conn, %{}, query: rejecting)
        end)

      assert log =~ "compression policy not applied"
      assert log =~ "add_policy"
      assert [] == policies(conn, schema)

      # A real backend init still succeeds when compression cannot be set up.
      assert {:error, {:add_policy, _}} = Compression.apply_policy(conn, %{}, query: rejecting)
    end)
  end

  test "an invalid age is an error, not a policy", ctx do
    run(ctx, fn %{conn: conn, schema: schema} ->
      for age <- [0, -5, "1d", 1.5, nil] do
        assert {:error, {:invalid_age, _}} =
                 Compression.apply_policy(conn, %{compression: %{age: age}})
      end

      assert [] == policies(conn, schema)
    end)
  end

  test "compressed reads equal uncompressed reads", ctx do
    run(ctx, fn %{conn: conn, schema: schema} ->
      id = :crypto.strong_rand_bytes(16)
      now = System.system_time(:millisecond)
      start = now - 5 * @day

      samples =
        for i <- 0..599 do
          %{
            series_id: id,
            name: "cmp_metric",
            labels: %{"host" => "a"},
            kind: :gauge,
            status: :healthy,
            ts_ms: start + i * 12 * 60_000,
            value: 50.0 + :math.sin(i / 7) * 10
          }
        end

      assert :ok = AutoangelTsdb.append_samples(conn, samples)
      assert :ok = Compression.apply_policy(conn, %{compression: %{age: 86_400}})

      read = fn -> AutoangelTsdb.read_range(conn, [id], start - 1, now + 1) end
      assert {:ok, before} = read.()
      assert length(before) == 600

      {:ok, %{rows: rows}} =
        Postgrex.query(
          conn,
          "SELECT compress_chunk(c, if_not_compressed => true) FROM show_chunks('samples', older_than => INTERVAL '1 day') c",
          []
        )

      assert rows != []

      {:ok, %{rows: [[compressed]]}} =
        Postgrex.query(
          conn,
          "SELECT count(*) FROM timescaledb_information.chunks WHERE hypertable_schema = $1 AND hypertable_name = 'samples' AND is_compressed",
          [schema]
        )

      assert compressed > 0
      assert {:ok, after_compression} = read.()
      assert after_compression == before
    end)
  end
end

defmodule AutoangelTsdb.RetentionTest do
  use ExUnit.Case, async: true

  alias AutoangelTsdb.Retention

  test "uses documented retention defaults and safe refresh offset" do
    config = %{retention: %{}}
    values = Retention.retention_config(config)

    assert values.raw == 14 * 86_400
    assert values.aggregate == 13 * 30 * 86_400
    assert values.anomaly == 90 * 86_400

    statements = Retention.policy_statements(values)
    add_refresh = Enum.find(statements, fn {name, _, _} -> name == :add_minute_refresh end)
    assert {_, sql, []} = add_refresh
    assert sql =~ "start_offset => INTERVAL '1209599 seconds'"
  end

  test "replaces every policy and reports the operation that fails" do
    test_pid = self()

    query = fn _connection, sql, _params ->
      send(test_pid, {:query, sql})
      {:ok, %{rows: []}}
    end

    assert {:ok, results} =
             Retention.apply_policies(
               :connection,
               %{retention: %{raw: 10, aggregate: 20, anomaly: 30}},
               query: query
             )

    assert length(results) == 10
    assert_receive {:query, "SELECT remove_retention_policy('samples', if_exists => TRUE)"}
    assert Enum.any?(results, fn {name, _} -> name == :add_hour_refresh end)

    failing = fn _connection, sql, _params ->
      if sql =~ "samples_1m'", do: {:error, :database_unavailable}, else: {:ok, %{}}
    end

    assert {:error, {:remove_minute, :database_unavailable}} =
             Retention.apply_policies(
               :connection,
               %{retention: %{raw: 10, aggregate: 20, anomaly: 30}},
               query: failing
             )
  end

  test "cleanup only targets ended anomalies and leaves open anomalies" do
    now = ~U[2025-01-31 00:00:00Z]

    query = fn _connection, sql, [cutoff] ->
      assert sql == "DELETE FROM anomalies WHERE end_time IS NOT NULL AND end_time < $1"
      assert cutoff == ~U[2025-01-01 00:00:00Z]
      {:ok, %{num_rows: 2}}
    end

    assert {:ok, %{num_rows: 2}} =
             Retention.cleanup_anomalies(:connection, 30 * 86_400, now: now, query: query)
  end
end

defmodule Autoangel.ConfigTest do
  use ExUnit.Case, async: false

  alias Autoangel.Config
  alias Autoangel.Secret

  test "loads defaults, environment secrets, and redacts secret values" do
    assert {:ok, config} =
             Config.load("[storage]\nbackend = \"none\"\n", %{
               "AUTOANGEL_INGEST_TOKEN" => "ingest-secret",
               "AUTOANGEL_STORAGE_PASSWORD" => "database-secret"
             })

    assert config.cardinality == %{per_metric: 2_000, total: 50_000}
    assert %Secret{} = config.ingest.token
    assert Secret.unwrap(config.ingest.token) == "ingest-secret"
    assert inspect(config.ingest.token) == "[REDACTED]"
    assert to_string(config.ingest.token) == "[REDACTED]"
    assert inspect(config.storage.password) == "[REDACTED]"
  end

  test "rejects an invalid scrape target without silently dropping it" do
    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert catch_exit(Config.load("[[scrape_targets]]\ninterval = 10\ntimeout = 20\n"))
      end)

    assert log =~ "scrape_targets"
  end

  test "logger filter redacts secrets embedded in formatted messages" do
    event = %{msg: {"token=~s", ["database-secret"]}}

    assert %{msg: {:string, "token=[REDACTED]"}} =
             Autoangel.LoggerSecretFilter.filter(event, ["database-secret"])

    escaped = %{msg: {:string, ~s(token=q\\"uote\\\\x)}}

    assert %{msg: {:string, "token=[REDACTED]"}} =
             Autoangel.LoggerSecretFilter.filter(escaped, [~s(q"uote\\x)])

    assert %{msg: {:string, "[REDACTED]=[REDACTED]"}} =
             Autoangel.LoggerSecretFilter.filter(%{msg: "secret=secret"}, ["secret"])

    _ = Autoangel.LoggerSecretFilter.filter(%{msg: {:string, ["bad ", <<255>>]}}, ["secret"])

    report = %{msg: {:report, %{status: "ok"}}}
    assert Autoangel.LoggerSecretFilter.filter(report, ["secret"]) == report
  end

  test "rejects multiple storage backends" do
    assert_invalid_setting(
      ~S"""
      [storage]
      backend = "none"
      backends = ["timescaledb", "timescaledb"]
      """,
      "storage_backend_selection"
    )
  end

  test "rejects invalid cardinality limits" do
    assert_invalid_setting("[cardinality]\nper_metric = 0\n", "cardinality_per_metric")
    assert_invalid_setting("[cardinality]\ntotal = 0\n", "cardinality_total")
  end

  test "rejects invalid learning duration and cycle count" do
    assert_invalid_setting("[learning]\nduration = 0\n", "learning_duration")
    assert_invalid_setting("[learning]\ncycles = 1\n", "learning_cycles")
  end

  test "rejects invalid missing-data settings" do
    assert_invalid_setting("[missing_data]\nmultiple = 1\n", "missing_data_multiple")
    assert_invalid_setting("[missing_data]\nsweep_interval = 0\n", "sweep_interval")
  end

  test "rejects invalid persistence settings" do
    assert_invalid_setting("[persistence]\nn = 0\n", "persistence_n")
    assert_invalid_setting("[persistence]\nm = 0\n", "persistence_m")
  end

  test "rejects invalid candidate pool settings" do
    assert_invalid_setting("[correlation]\ncandidate_pool_limit = 0\n", "candidate_pool_limit")
    assert_invalid_setting("[correlation]\nsupport_threshold = -1\n", "support_threshold")
  end

  test "rejects invalid retention windows" do
    assert_invalid_setting("[retention]\nraw = 0\n", "retention_raw")
    assert_invalid_setting("[retention]\naggregate = 0\n", "retention_aggregate")
    assert_invalid_setting("[retention]\nanomaly = 0\n", "retention_anomaly")
  end

  test "rejects an invalid heartbeat interval" do
    assert_invalid_setting("heartbeat_interval = 0\n", "heartbeat_interval")
  end

  test "rejects invalid ingest rate limits" do
    assert_invalid_setting("[ingest_rate_limit]\nrate = 0\n", "ingest_rate")
    assert_invalid_setting("[ingest_rate_limit]\nburst = 0\n", "ingest_burst")
  end

  test "rejects invalid authentication exemptions" do
    assert_invalid_setting("auth_exemptions = [\"/metrics\"]\n", "auth_exemptions")
  end

  test "rejects invalid credentials" do
    assert_invalid_setting("[ingest]\ntoken = \"\"\n", "ingest_credentials")
  end

  test "rejects credentials shared by ingest and Control API" do
    assert_invalid_setting(
      ~S"""
      [ingest]
      token = "same"
      [control_api]
      token = "same"
      """,
      "distinct_credentials"
    )
  end

  defp assert_invalid_setting(source, setting) do
    log =
      ExUnit.CaptureLog.capture_log(fn ->
        assert {:shutdown, {:invalid_configuration, errors}} = catch_exit(Config.load(source, %{}))
        assert String.to_atom(setting) in errors
      end)

    assert log =~ "invalid configuration setting: #{setting}"
  end
end

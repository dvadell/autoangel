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
end

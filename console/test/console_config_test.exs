defmodule Console.ConfigTest do
  use ExUnit.Case, async: true

  alias Console.Config

  @secret String.duplicate("s", 64)
  @valid %{
    "AUTOANGEL_CONSOLE_USER" => "ops",
    "AUTOANGEL_CONSOLE_PASSWORD" => "pw",
    "AUTOANGEL_CONSOLE_SECRET_KEY_BASE" => @secret
  }

  defp env(extra), do: Map.merge(@valid, extra)

  test "defaults" do
    assert {:ok, config} = Config.from_env(@valid)
    assert config.port == 4000
    assert config.bind == {127, 0, 0, 1}
    assert config.engine_not_reporting_seconds == 60
    assert config.health_exempt == false
    assert config.tls == nil
    assert config.database_url == nil
    assert config.auth == [{:basic, "ops", "pw"}]
  end

  test "the Engine-not-reporting threshold is configurable and validated" do
    assert {:ok, %{engine_not_reporting_seconds: 120}} =
             Config.from_env(env(%{"AUTOANGEL_CONSOLE_ENGINE_NOT_REPORTING_SECONDS" => "120"}))

    for bad <- ["0", "-5", "abc", "1.5"] do
      assert {:error, [message]} =
               Config.from_env(env(%{"AUTOANGEL_CONSOLE_ENGINE_NOT_REPORTING_SECONDS" => bad}))

      assert message =~ "ENGINE_NOT_REPORTING_SECONDS"
    end
  end

  test "credentials are required" do
    assert {:error, [message]} =
             Config.from_env(%{"AUTOANGEL_CONSOLE_SECRET_KEY_BASE" => @secret})

    assert message =~ "needs credentials"
  end

  test "a bearer token alone is enough, and user needs password" do
    assert {:ok, %{auth: [{:bearer, "tok"}]}} =
             Config.from_env(%{
               "AUTOANGEL_CONSOLE_TOKEN" => "tok",
               "AUTOANGEL_CONSOLE_SECRET_KEY_BASE" => @secret
             })

    assert {:error, [message]} =
             Config.from_env(%{
               "AUTOANGEL_CONSOLE_USER" => "ops",
               "AUTOANGEL_CONSOLE_SECRET_KEY_BASE" => @secret
             })

    assert message =~ "must be set together"
  end

  test "the secret key base is required and long enough" do
    assert {:error, [m1]} = Config.from_env(Map.delete(@valid, "AUTOANGEL_CONSOLE_SECRET_KEY_BASE"))
    assert m1 =~ "required"
    assert {:error, [m2]} = Config.from_env(env(%{"AUTOANGEL_CONSOLE_SECRET_KEY_BASE" => "short"}))
    assert m2 =~ "at least 64 bytes"
  end

  test "port, bind, boolean and TLS are validated, and every problem is reported" do
    assert {:error, problems} =
             Config.from_env(
               env(%{
                 "AUTOANGEL_CONSOLE_PORT" => "70000",
                 "AUTOANGEL_CONSOLE_BIND" => "nope",
                 "AUTOANGEL_CONSOLE_HEALTH_EXEMPT" => "yes",
                 "AUTOANGEL_CONSOLE_TLS_CERT_FILE" => "/does/not/exist.pem"
               })
             )

    assert length(problems) == 4
  end

  test "TLS is optional and needs both existing files" do
    cert = Path.join(System.tmp_dir!(), "console_cfg_cert.pem")
    key = Path.join(System.tmp_dir!(), "console_cfg_key.pem")
    File.write!(cert, "x")
    File.write!(key, "x")
    on_exit(fn -> File.rm(cert) && File.rm(key) end)

    assert {:ok, %{tls: %{cert_file: ^cert, key_file: ^key}}} =
             Config.from_env(
               env(%{
                 "AUTOANGEL_CONSOLE_TLS_CERT_FILE" => cert,
                 "AUTOANGEL_CONSOLE_TLS_KEY_FILE" => key
               })
             )
  end
end

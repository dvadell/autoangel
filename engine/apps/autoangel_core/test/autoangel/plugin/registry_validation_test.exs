defmodule Autoangel.Plugin.RegistryValidationTest do
  # async: false — exercises the shared compile-time plugin registry/config path
  # and a process-global validate_config call counter.
  use ExUnit.Case, async: false

  alias Autoangel.Plugin.Registry
  alias Autoangel.Test.CountingPlugin

  # Requirement 22.6: validate_config/1 is called exactly once per enabled
  # plugin before the Dispatcher starts, the registry refuses to start for
  # invalid/unknown plugin configuration, and errors expose only plugin and
  # field names (never secret values).
  #
  # The registry's plugin set is compiled from
  # `Application.compile_env(:autoangel_core, :plugins, [])`. In the :test build
  # a single test-only plugin, `Autoangel.Test.CountingPlugin`, is registered
  # under the name "counting" (see config/test.exs). Its `validate_config/1`
  # increments a global counter on each call, so the "exactly once per enabled
  # plugin" and "no secret leak" behaviours are exercised directly rather than
  # argued structurally.

  setup do
    CountingPlugin.reset_counter()
    :ok
  end

  describe "compiled plugin set" do
    test "is fixed at compile time and contains only the test-registered plugin" do
      # The set is resolved from compile_env, not loaded from runtime config.
      assert Registry.compiled_plugins() == [{"counting", CountingPlugin}]
    end
  end

  describe "validate/1 accepts valid configuration" do
    test "no plugins configured resolves to an empty plugin list" do
      assert {:ok, []} = Registry.validate(%{})
      assert {:ok, []} = Registry.validate(%{plugins: %{}})
      # Nothing enabled => validate_config is never called.
      assert CountingPlugin.call_count() == 0
    end

    test "explicitly disabled plugin sections are not enabled" do
      assert {:ok, []} = Registry.validate(%{plugins: %{"counting" => false}})
      assert CountingPlugin.call_count() == 0
    end
  end

  describe "validate/1 refuses unknown plugins (22.6)" do
    test "an unknown enabled plugin is rejected by name" do
      assert {:error, ["plugins.email: unknown plugin"]} =
               Registry.validate(%{plugins: %{"email" => %{server: "s"}, enabled: ["email"]}})
    end

    test "each unknown plugin is reported once, naming only the plugin" do
      assert {:error, errors} =
               Registry.validate(%{plugins: %{"email" => %{}, "telegram" => %{}}})

      assert Enum.sort(errors) == [
               "plugins.email: unknown plugin",
               "plugins.telegram: unknown plugin"
             ]
    end

    test "an unknown plugin is rejected before validate_config runs for known ones" do
      # Enable BOTH the real "counting" plugin AND an unknown "email" plugin.
      # validate_available/1 checks unknown_plugins/2 first and returns the
      # error BEFORE validate_enabled/2 invokes validate_config on ANY plugin,
      # so the known "counting" plugin's callback must never run — proving the
      # short-circuit rather than merely relying on nothing-known-being-enabled.
      assert {:error, ["plugins.email: unknown plugin"]} =
               Registry.validate(%{
                 plugins: %{"counting" => %{"recipient" => "x"}, "email" => %{}}
               })

      assert CountingPlugin.call_count() == 0
    end
  end

  describe "validate/1 refuses malformed configuration (22.6)" do
    test "a non-map / non-list plugins value is rejected" do
      assert {:error, ["plugins: invalid configuration"]} =
               Registry.validate(%{plugins: 123})
    end

    test "a non-map top-level configuration is rejected" do
      assert {:error, ["plugins: invalid configuration"]} = Registry.validate("not a map")
    end

    test "a plugin section that is neither a map nor a boolean is rejected" do
      assert {:error, ["plugins.email: invalid configuration"]} =
               Registry.validate(%{plugins: %{"email" => "nope"}})
    end

    test "an empty plugin name is rejected" do
      assert {:error, ["plugins.enabled: invalid plugin name"]} =
               Registry.validate(%{plugins: %{enabled: [""]}})
    end

    test "a duplicate plugin in list form is rejected" do
      assert {:error, ["plugins: duplicate plugin"]} =
               Registry.validate(%{plugins: ["email", "email"]})
    end
  end

  describe "validate_config is called exactly once per enabled plugin (22.6)" do
    test "an enabled plugin with valid config resolves and is validated once" do
      assert {:ok, [%{name: "counting", module: CountingPlugin, config: config}]} =
               Registry.validate(%{plugins: %{"counting" => %{"recipient" => "ops@example"}}})

      assert Map.has_key?(config, "recipient")
      assert CountingPlugin.call_count() == 1
    end

    test "a plugin enabled twice (deduplicated) is still validated exactly once" do
      assert {:ok, [%{name: "counting", module: CountingPlugin}]} =
               Registry.validate(%{
                 plugins: %{
                   "counting" => %{"recipient" => "ops@example"},
                   enabled: ["counting", "counting"]
                 }
               })

      assert CountingPlugin.call_count() == 1
    end

    test "a plugin enabled via list form is validated exactly once" do
      assert {:ok, [%{name: "counting", module: CountingPlugin}]} =
               Registry.validate(%{plugins: [%{name: "counting", recipient: "ops@example"}]})

      # The list-form resolution path also invokes the callback exactly once.
      assert CountingPlugin.call_count() == 1
    end
  end

  describe "validation errors never expose configuration values (22.6)" do
    test "an unknown plugin's secret-bearing config does not leak values into errors" do
      assert {:error, errors} =
               Registry.validate(%{plugins: %{"email" => %{"password" => "SUPERSECRET"}}})

      refute Enum.any?(errors, &String.contains?(&1, "SUPERSECRET"))
      assert errors == ["plugins.email: unknown plugin"]
    end

    test "an invalid known-plugin config reports field names only, not secret values" do
      # Required field :recipient is missing, but a secret is present. The error
      # must name the field (plugins.counting.recipient) and never echo the
      # secret value.
      assert {:error, errors} =
               Registry.validate(%{plugins: %{"counting" => %{"password" => "SUPERSECRET"}}})

      assert errors == ["plugins.counting.recipient"]
      refute Enum.any?(errors, &String.contains?(&1, "SUPERSECRET"))
      # validate_config ran exactly once even though it failed.
      assert CountingPlugin.call_count() == 1
    end
  end
end

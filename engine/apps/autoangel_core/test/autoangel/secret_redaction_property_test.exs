defmodule Autoangel.SecretRedactionPropertyTest do
  @moduledoc """
  Property 52 (Requirements 22.7, 74.3).

  "For any set of generated secret values placed in every secret setting, and
  for every error path (invalid Plugin config, auth failure, startup failure,
  delivery failure, database error), no captured log entry contains any secret
  value or any substring of 8 or more characters of it, and the entries name the
  setting where relevant."

  Topology_A note
  ---------------
  The DATABASE-ERROR path is NOT reachable in Topology_A (the Engine runs with no
  storage backend / no DB), so it is marked N/A here and is NOT forced. Every
  other error path listed by Property 52 is reachable DB-free and is exercised:

    * invalid Plugin config / startup failure — `Autoangel.Config.load/2` with an
      invalid configuration (distinct-credentials violation and a plugin that
      fails validation). Config logs only field-name atoms.
    * auth failure — `Autoangel.Auth.call/2` / `authorized?/3` with wrong or
      absent credentials; the configured credential (the secret) must never
      appear in any log.
    * delivery failure — `Autoangel.Alert.PluginRunner` recording a permanent
      failure whose reason embeds the secret; `sanitize_error/1` reduces any
      non-atom reason to `:redacted`.

  Approach
  --------
  Two complementary strategies, both run:

    A. DIRECT FILTER PROPERTY (deterministic): generate a set of secret values
       and embed a secret into each logger `msg` shape, run
       `LoggerSecretFilter.filter/2`, and assert the RENDERED result contains
       neither the full secret nor any contiguous 8+-character substring of it.
       An independent 8+-substring checker (`contains_8plus_window?/2`) is the
       crux of Property 52.

    B. END-TO-END ERROR-PATH PROPERTY: place generated secrets into a VALID
       config, load it via `Config.load/2` (which wraps the secrets and installs
       the primary logger filter), then drive each reachable error path under
       `capture_log` and assert the captured log leaks no secret and no 8+
       substring. The installed primary filter affects ALL logging while present,
       so every end-to-end section installs it in a tightly scoped block and
       REMOVES it in `on_exit` via `:logger.remove_primary_filter/1`.
  """

  # async: false — the end-to-end sections install a :logger PRIMARY filter,
  # which is global process-wide state; a concurrent test could observe it.
  use ExUnit.Case, async: false
  use ExUnitProperties

  import ExUnit.CaptureLog

  alias Autoangel.Alert.PluginRunner
  alias Autoangel.{Auth, Config, LoggerSecretFilter, Secret}

  @filter_id :autoangel_secrets

  # A plugin whose deliver/2 always fails permanently with a reason that EMBEDS
  # the configured secret. The runner must sanitize this to an atom (:redacted)
  # so the secret never reaches the failure record or any log.
  defmodule SecretLeakingPlugin do
    @behaviour Autoangel.Plugin
    @impl true
    def validate_config(_config), do: :ok

    @impl true
    def deliver(_alert, config) do
      # reason is a tuple carrying the raw secret string — a non-atom reason, so
      # sanitize_reason/1 reduces it to :redacted.
      {:permanent, {:upstream_rejected, config[:secret]}}
    end
  end

  # ---------------------------------------------------------------------------
  # Independent 8+-substring checker — the crux of Property 52.
  #
  # Returns true iff `haystack` contains some contiguous substring of `secret`
  # of length >= 8. It suffices to check every window of length EXACTLY 8,
  # because any longer common substring necessarily contains an 8-character
  # window; if no 8-window of the secret appears, no longer run can either.
  #
  # Implemented independently of the filter (raw byte windows over the secret),
  # so a filter that merely replaced the full secret but left an 8+ fragment
  # behind would be caught.
  # ---------------------------------------------------------------------------
  defp contains_8plus_window?(haystack, secret)
       when is_binary(haystack) and is_binary(secret) do
    windows_of_8(secret)
    |> Enum.any?(fn window -> String.contains?(haystack, window) end)
  end

  # All contiguous 8-character (by codepoint) windows of `secret`. Secrets are
  # generated with length >= 8, so at least one window always exists.
  defp windows_of_8(secret) do
    graphemes = String.graphemes(secret)
    count = length(graphemes)

    if count < 8 do
      []
    else
      for start <- 0..(count - 8) do
        graphemes |> Enum.slice(start, 8) |> Enum.join()
      end
    end
  end

  # Assert a rendered log string leaks neither the full secret nor any 8+ window.
  defp refute_leaks(rendered, secret) when is_binary(rendered) do
    refute String.contains?(rendered, secret),
           "rendered output leaked the full secret value"

    refute contains_8plus_window?(rendered, secret),
           "rendered output leaked an 8+-character substring of the secret"
  end

  defp refute_leaks_any(rendered, secrets) when is_binary(rendered) do
    Enum.each(secrets, &refute_leaks(rendered, &1))
  end

  # Render a logger event's msg to a string the way the filter renders it. The
  # filter emits {:string, redacted} whenever it changed anything; an unchanged
  # event means no secret was present, so rendering the original is safe.
  defp render_event_msg(%{msg: msg}), do: render_msg(msg)
  defp render_msg(msg) when is_binary(msg), do: msg
  defp render_msg({:string, chars}), do: IO.chardata_to_string(chars)

  defp render_msg({:report, report}),
    do: inspect(report, limit: :infinity, printable_limit: :infinity)

  defp render_msg({format, args}) when is_list(args),
    do: format |> :io_lib.format(args) |> IO.iodata_to_binary()

  # ---------------------------------------------------------------------------
  # Generators
  # ---------------------------------------------------------------------------

  # A single secret value: length >= 8 (so an 8-window exists) up to ~40 chars,
  # drawn from ASCII letters, digits and punctuation, including regex/special
  # characters so the filter's literal replacement (not regex) is exercised.
  defp secret_gen do
    gen all(chars <- list_of(secret_char(), min_length: 8, max_length: 40)) do
      List.to_string(chars)
    end
  end

  defp secret_char do
    one_of([
      integer(?a..?z),
      integer(?A..?Z),
      integer(?0..?9),
      # punctuation / regex-special characters as codepoints
      member_of(~c"!@#$%^&*()[]{}+.?|\\/\"'=-_:;,<>")
    ])
  end

  # A set of distinct secrets, one for each of the five secret settings.
  defp five_secrets_gen do
    gen all(secrets <- uniq_list_of(secret_gen(), length: 5)) do
      secrets
    end
  end

  # ===========================================================================
  # A. DIRECT FILTER PROPERTY
  # ===========================================================================

  property "filter removes every secret and every 8+-substring from every msg shape" do
    check all(secret <- secret_gen(), max_runs: 200) do
      secrets = [secret]

      # The secret embedded in each logger msg shape the filter understands.
      events = [
        # 1. bare binary
        %{msg: "token=" <> secret},
        # 2. {:string, chardata}
        %{msg: {:string, ["connecting with ", secret]}},
        # 3. {format, args}
        %{msg: {"connecting with ~s", [secret]}},
        # 4. {:report, map}
        %{msg: {:report, %{password: secret}}}
      ]

      for event <- events do
        filtered = LoggerSecretFilter.filter(event, secrets)
        rendered = render_event_msg(filtered)
        refute_leaks(rendered, secret)
      end

      # Also exercise %Secret{} as a filter pattern (not just a raw binary).
      wrapped_filtered = LoggerSecretFilter.filter(%{msg: "x=" <> secret}, [Secret.new(secret)])
      refute_leaks(render_event_msg(wrapped_filtered), secret)
    end
  end

  property "Secret inspect and to_string always render [REDACTED]" do
    check all(secret <- secret_gen(), max_runs: 200) do
      wrapped = Secret.new(secret)
      assert inspect(wrapped) == "[REDACTED]"
      assert to_string(wrapped) == "[REDACTED]"
      # The wrapped value round-trips but is never shown by inspect/to_string.
      assert Secret.unwrap(wrapped) == secret
    end
  end

  property "the 8+-substring checker itself is sound (self-check)" do
    check all(secret <- secret_gen(), max_runs: 100) do
      # The secret trivially contains an 8+-window of itself.
      assert contains_8plus_window?(secret, secret)
      # "[REDACTED]" (the replacement) shares no 8-char window with a secret made
      # only of the generator's charset unless the secret itself embeds it; the
      # generator can produce such overlaps, so we only assert the positive here
      # and rely on the filter properties for the redacted-output negatives.
      assert render_msg({:string, [secret]}) == secret
    end
  end

  # ===========================================================================
  # B. END-TO-END ERROR-PATH PROPERTY
  #
  # Database-error path: N/A in Topology_A (no DB) — not forced (see @moduledoc).
  # ===========================================================================

  # Build a VALID TOML config string placing the five generated secrets into the
  # five secret settings. ingest and control_api credentials use distinct
  # token+basic pairs so distinct_credentials holds. storage password is set but
  # backend is :none (DB-free). TOML strings escape backslash and double-quote.
  defp valid_config_source([i_tok, i_pass, c_tok, c_pass, s_pass]) do
    """
    [storage]
    backend = "none"
    password = "#{toml_escape(s_pass)}"

    [ingest]
    token = "#{toml_escape(i_tok)}"
    username = "ingest-user"
    password = "#{toml_escape(i_pass)}"

    [control_api]
    token = "#{toml_escape(c_tok)}"
    username = "control-user"
    password = "#{toml_escape(c_pass)}"
    """
  end

  defp toml_escape(value) do
    value
    |> String.replace("\\", "\\\\")
    |> String.replace("\"", "\\\"")
    |> String.replace("\n", "\\n")
    |> String.replace("\r", "\\r")
    |> String.replace("\t", "\\t")
  end

  property "no error-path log leaks any secret or any 8+-substring of it" do
    check all(secrets <- five_secrets_gen(), max_runs: 25) do
      [i_tok, i_pass, c_tok, c_pass, _s_pass] = secrets

      # Loading the VALID config installs the real filter; remove it on exit so
      # it cannot bleed into other tests even if an assertion fails.
      assert {:ok, _config} = Config.load(valid_config_source(secrets), %{})

      try do
        # --- startup / invalid configuration path -----------------------------
        # An INVALID config: ingest and control_api share the same token (a
        # secret value) → distinct_credentials error; also enable the known
        # "counting" plugin WITHOUT its required field → a plugin validation
        # error. Config logs only field-name atoms, never the secret.
        invalid_source = """
        [ingest]
        token = "#{toml_escape(i_tok)}"

        [control_api]
        token = "#{toml_escape(i_tok)}"

        [plugins]
        counting = true
        """

        invalid_log =
          capture_log(fn ->
            assert {:shutdown, {:invalid_configuration, errors}} =
                     catch_exit(Config.load(invalid_source, %{}))

            # The entries name the SETTINGS (field-name atoms), where relevant.
            assert :distinct_credentials in errors
            assert Enum.any?(errors, &(&1 == "plugins.counting.recipient"))
          end)

        # Names the settings but leaks no secret / no 8+-substring.
        assert invalid_log =~ "invalid configuration setting: distinct_credentials"
        assert invalid_log =~ "invalid configuration setting: plugins.counting.recipient"
        refute_leaks_any(invalid_log, secrets)

        # --- delivery failure path --------------------------------------------
        # A plugin whose deliver/2 fails permanently with a reason embedding the
        # secret. The runner sanitizes the reason to :redacted; neither the log
        # nor the recorded failure may contain the secret.
        delivery_log =
          capture_log(fn ->
            {:ok, task_supervisor} = Task.Supervisor.start_link()

            {:ok, runner} =
              PluginRunner.start_link(
                plugin: SecretLeakingPlugin,
                config: %{secret: i_pass},
                task_supervisor: task_supervisor,
                timeout: 1_000,
                max_attempts: 1,
                breaker_threshold: 100,
                observer: self()
              )

            try do
              assert :ok = PluginRunner.enqueue(runner, %{anomaly_id: "a-1"})
              # Wait for the (synchronous-ish) delivery to be recorded.
              assert_receive {:plugin_delivery, SecretLeakingPlugin, :failure}, 2_000

              [record] = PluginRunner.failures(runner)
              # The sanitized failure record must not embed the secret anywhere.
              serialized = inspect(record, limit: :infinity, printable_limit: :infinity)
              refute_leaks(serialized, i_pass)
              # Specifically: the reason was reduced to :redacted.
              assert record.error == {:permanent, :redacted}
              assert record.error_class == :permanent
            after
              GenServer.stop(runner)
              Supervisor.stop(task_supervisor)
            end
          end)

        refute_leaks_any(delivery_log, secrets)

        # --- auth failure path ------------------------------------------------
        # The configured credential IS the secret (control_api token/password).
        # A wrong/absent credential is rejected; no log may contain the secret.
        auth_config = %{
          ingest: %{token: Secret.new(i_tok)},
          control_api: %{
            token: Secret.new(c_tok),
            username: "control-user",
            password: Secret.new(c_pass)
          },
          auth_exemptions: []
        }

        auth_log =
          capture_log(fn ->
            # No authorization header at all.
            conn_absent = Plug.Test.conn(:get, "/metrics")
            refute Auth.authorized?(conn_absent, :control_api, auth_config)
            called_absent = Auth.call(conn_absent, role: :control_api, config: auth_config)
            assert called_absent.halted
            assert called_absent.status == 401

            # A wrong bearer token value.
            conn_wrong =
              Plug.Test.conn(:get, "/metrics")
              |> Plug.Conn.put_req_header("authorization", "Bearer wrong-" <> c_tok)

            refute Auth.authorized?(conn_wrong, :control_api, auth_config)
            called_wrong = Auth.call(conn_wrong, role: :control_api, config: auth_config)
            assert called_wrong.halted
            assert called_wrong.status == 401
          end)

        refute_leaks_any(auth_log, secrets)
      after
        :logger.remove_primary_filter(@filter_id)
      end
    end
  end

  # A belt-and-braces guarantee that NO autoangel_secrets primary filter remains
  # installed after this test module runs, protecting the rest of the suite.
  setup do
    on_exit(fn -> :logger.remove_primary_filter(@filter_id) end)
    :ok
  end
end

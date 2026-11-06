defmodule Autoangel.AuthPropertyTest do
  # async: false because the ingestion-invariant cases start the named
  # Autoangel.SeriesRegistry GenServer and the pipeline writes process-global
  # state (persistent_term / metrics). Running concurrently would race on those.
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Plug.Conn
  import Plug.Test

  alias Autoangel.{Auth, Secret, SeriesRegistry}
  alias Autoangel.HTTP.Control
  alias Autoangel.HTTP.Ingest, as: HTTPIngest

  # **Validates: Requirements 7.1, 7.2, 7.4, 30.7, 45.5, 46.4, 74.1, 74.2, 74.7**
  # Feature: v1, Property 50: authentication on Engine endpoints
  #
  # "For any endpoint of the Engine or the Console and any credential value
  #  (absent, malformed, a mutation of a valid credential, a valid credential of
  #  another role, or any value when the role has no configured credential), the
  #  request is rejected with an authentication error, returns no metric data,
  #  and causes no ingestion, Series creation, Control_API action or
  #  Feedback_Action."
  #
  # This test covers the Engine-relevant subset (Console reqs 57.x/64.x are out
  # of scope for the Engine test). It drives:
  #
  #   * the pure predicate `Auth.authorized?/3` and the plug `Auth.call/2`
  #     against an INDEPENDENT oracle for both roles and every credential
  #     category, and
  #   * the full ingest pipeline `Autoangel.HTTP.Ingest.call/2`
  #     (auth -> rate limit -> pipeline) to prove that an unauthorized request is
  #     halted BEFORE any ingestion or Series creation happens, contrasted with
  #     an authorized request on the same body that does reach the pipeline, and
  #   * the control plug `Autoangel.HTTP.Control.call/2` to prove an unauthorized
  #     /metrics request returns 401 with no metric body.

  @ingest_path "/api/v1/push"
  @metrics_path "/metrics"
  @valid_body "autoangel_property_metric_total 1\n"

  setup_all do
    # Plug.Test rejects non-lowercase header keys under test unless disabled; the
    # generated authorization headers are always lowercase but malformed values
    # exercise odd schemes, so keep validation relaxed for determinism.
    Application.put_env(:plug, :validate_header_keys_during_test, false)
    :ok
  end

  # A credential pair (one for :ingest, one for :control_api) that are always
  # distinct, with token/password wrapped either as plain strings or Secret.new
  # so both representations are exercised.
  defp wrap(value, :plain), do: value
  defp wrap(value, :secret), do: Secret.new(value)

  defp config_generator do
    gen all(
          ingest_tok <- member_of(["ingest-token-a", "ingest-token-b"]),
          ingest_user <- member_of(["ingest-user-a", "ingest-user-b"]),
          ingest_pass <- member_of(["ingest-pass-a", "ingest-pass-b"]),
          control_tok <- member_of(["control-token-a", "control-token-b"]),
          control_user <- member_of(["control-user-a", "control-user-b"]),
          control_pass <- member_of(["control-pass-a", "control-pass-b"]),
          ingest_wrap <- member_of([:plain, :secret]),
          control_wrap <- member_of([:plain, :secret])
        ) do
      %{
        ingest: %{
          token: wrap(ingest_tok, ingest_wrap),
          username: ingest_user,
          password: wrap(ingest_pass, ingest_wrap)
        },
        control_api: %{
          token: wrap(control_tok, control_wrap),
          username: control_user,
          password: wrap(control_pass, control_wrap)
        },
        # a role whose credentials are empty: used for the :no_configured case
        auth_exemptions: []
      }
    end
  end

  defp plain(value) when is_binary(value), do: value
  defp plain(%Secret{} = secret), do: Secret.unwrap(secret)

  defp bearer(token), do: "Bearer " <> token
  defp basic(username, password), do: "Basic " <> Base.encode64(username <> ":" <> password)

  # A valid header (bearer or basic) for the given role's credentials.
  defp valid_header(creds, :bearer), do: bearer(plain(creds.token))
  defp valid_header(creds, :basic), do: basic(creds.username, plain(creds.password))

  # Mutate a valid header by dropping or flipping one character of its payload.
  defp mutate(header) do
    case String.split(header, " ", parts: 2) do
      [scheme, payload] when payload != "" ->
        mutated =
          case :binary.first(payload) do
            ?a -> "b" <> binary_part(payload, 1, byte_size(payload) - 1)
            _ -> "a" <> binary_part(payload, 1, byte_size(payload) - 1)
          end

        scheme <> " " <> mutated

      _ ->
        header <> "x"
    end
  end

  @malformed_headers [
    "Bearer",
    "Bearer ",
    "bearer anything",
    "Basic !!!",
    "Basic ",
    "Token abc",
    "Bearer   "
  ]

  # Build the candidate: returns {header_or_nil, valid_for_role_or_nil}.
  # valid_for_role is the role (:ingest | :control_api) for which this header is
  # an EXACT valid credential, or nil when the header authenticates nobody.
  defp candidate_generator(config) do
    gen all(
          category <-
            member_of([
              :absent,
              :malformed,
              :valid_ingest,
              :valid_control,
              :mutated_ingest,
              :mutated_control,
              :no_configured
            ]),
          scheme <- member_of([:bearer, :basic]),
          malformed <- member_of(@malformed_headers)
        ) do
      case category do
        :absent ->
          {nil, nil}

        :malformed ->
          {malformed, nil}

        :valid_ingest ->
          {valid_header(config.ingest, scheme), :ingest}

        :valid_control ->
          {valid_header(config.control_api, scheme), :control_api}

        :mutated_ingest ->
          {mutate(valid_header(config.ingest, scheme)), nil}

        :mutated_control ->
          {mutate(valid_header(config.control_api, scheme)), nil}

        :no_configured ->
          # a value that would be valid against empty creds: authenticates nobody
          {bearer("any-value"), nil}
      end
    end
  end

  defp put_auth(conn, nil), do: conn
  defp put_auth(conn, header), do: put_req_header(conn, "authorization", header)

  property "authorized?/3 matches an independent oracle for every role and credential" do
    check all(
            config <- config_generator(),
            {header, valid_for} <- candidate_generator(config),
            role <- member_of([:ingest, :control_api])
          ) do
      path = if role == :ingest, do: @ingest_path, else: @metrics_path
      method = if role == :ingest, do: :post, else: :get

      conn = put_auth(conn(method, path), header)

      # The oracle: authorized only when the header is an exact valid credential
      # for THIS role. A valid credential of the other role, a mutation, a
      # malformed value, or an absent header all authenticate nobody.
      should_authorize? = valid_for == role

      assert Auth.authorized?(conn, role, config) == should_authorize?

      called = Auth.call(conn, role: role, config: config)

      if should_authorize? do
        refute called.halted
        assert called.status == nil
      else
        assert called.halted
        assert called.status == 401
        assert called.resp_body == "unauthorized"
        assert get_resp_header(called, "www-authenticate") != []
      end
    end
  end

  property "a credential valid for the other role is rejected on the first role" do
    check all(config <- config_generator(), scheme <- member_of([:bearer, :basic])) do
      ingest_header = valid_header(config.ingest, scheme)
      control_header = valid_header(config.control_api, scheme)

      # control credential presented to the ingest endpoint
      refute Auth.authorized?(
               put_auth(conn(:post, @ingest_path), control_header),
               :ingest,
               config
             )

      # ingest credential presented to the control endpoint
      refute Auth.authorized?(
               put_auth(conn(:get, @metrics_path), ingest_header),
               :control_api,
               config
             )
    end
  end

  property "a role with no configured credential rejects any value" do
    check all(
            config <- config_generator(),
            header <-
              member_of([
                nil,
                "Bearer anything",
                basic("user", "pass"),
                valid_header(%{token: "x", username: "u", password: "p"}, :bearer)
              ]),
            role <- member_of([:ingest, :control_api])
          ) do
      empty_config = Map.put(config, role, %{})
      conn = put_auth(conn(if(role == :ingest, do: :post, else: :get), "/x"), header)

      refute Auth.authorized?(conn, role, empty_config)

      called = Auth.call(conn, role: role, config: empty_config)
      assert called.halted
      assert called.status == 401
    end
  end

  # Start the named SeriesRegistry robustly: stop any already-running instance
  # first (a prior async: false file/iteration may have leaked one whose on_exit
  # has not run yet), and if we still race with a concurrent start, stop the
  # winner and retry once. Mirrors the pattern in series_registry_test.exs.
  defp start_registry(opts) do
    if pid = Process.whereis(SeriesRegistry), do: GenServer.stop(pid)

    case SeriesRegistry.start_link(opts) do
      {:ok, pid} ->
        pid

      {:error, {:already_started, running}} ->
        GenServer.stop(running)
        {:ok, pid} = SeriesRegistry.start_link(opts)
        pid
    end
  end

  # Stop a process tolerantly: it may already be gone (its own on_exit ran, or a
  # later start_registry stopped it), so swallow the :exit from a dead pid.
  defp safe_stop(pid) when is_pid(pid) do
    if Process.alive?(pid), do: GenServer.stop(pid)
  catch
    :exit, _ -> :ok
  end

  defp safe_stop(_), do: :ok

  describe "ingestion is blocked before any ingestion or Series creation" do
    setup do
      pid = start_registry([])

      on_exit(fn -> safe_stop(pid) end)

      :ok
    end

    property "unauthorized ingest halts at 401 with no samples and no Series created" do
      check all(
              config <- config_generator(),
              {header, valid_for} <- candidate_generator(config),
              # only unauthorized candidates for the ingest role
              valid_for != :ingest,
              max_runs: 40
            ) do
        before = SeriesRegistry.count()

        conn =
          conn(:post, @ingest_path, @valid_body)
          |> put_auth(header)

        result =
          HTTPIngest.call(conn,
            config: config,
            worker_fun: fn _ -> self() end,
            storage_writer: self()
          )

        # auth halts before the pipeline: 401, not 202
        assert result.halted
        assert result.status == 401
        assert result.resp_body == "unauthorized"

        # no ingestion: neither the storage cast nor any worker enqueue happened
        refute_received {:"$gen_cast", {:samples, _}}
        refute_received {:samples, _}

        # no Series created
        assert SeriesRegistry.count() == before
      end
    end

    test "an authorized ingest request DOES reach the pipeline (contrast)" do
      config = %{
        ingest: %{token: Secret.new("ingest-token-a")},
        control_api: %{token: Secret.new("control-token-a")},
        auth_exemptions: []
      }

      before = SeriesRegistry.count()

      result =
        conn(:post, @ingest_path, @valid_body)
        |> put_req_header("authorization", "Bearer ingest-token-a")
        |> HTTPIngest.call(
          config: config,
          worker_fun: fn _ -> self() end,
          storage_writer: self()
        )

      refute result.halted
      assert result.status == 202

      # the pipeline ran: the storage writer received the accepted samples and a
      # Series was created for the single metric in the body.
      assert_received {:"$gen_cast", {:samples, samples}}
      assert is_list(samples) and samples != []
      assert SeriesRegistry.count() == before + 1
    end
  end

  describe "control endpoint returns no metric data when unauthorized" do
    setup do
      Autoangel.Boot.set_phase(:ready)
      Autoangel.Metrics.ensure_started()
      :ok
    end

    property "unauthorized /metrics is 401 with no exposition body" do
      check all(
              config <- config_generator(),
              {header, valid_for} <- candidate_generator(config),
              valid_for != :control_api,
              max_runs: 40
            ) do
        result =
          conn(:get, @metrics_path)
          |> put_auth(header)
          |> Control.call(config: config)

        assert result.halted
        assert result.status == 401
        assert result.resp_body == "unauthorized"
        # no metric data leaked
        refute result.resp_body =~ "autoangel_"
      end
    end

    test "an authorized /metrics request DOES return metric data (contrast)" do
      config = %{control_api: %{token: Secret.new("control-token-a")}, auth_exemptions: []}

      result =
        conn(:get, @metrics_path)
        |> put_req_header("authorization", "Bearer control-token-a")
        |> Control.call(config: config)

      refute result.halted
      assert result.status == 200
      assert result.resp_body =~ "autoangel_"
    end
  end
end

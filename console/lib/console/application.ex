defmodule Console.Application do
  @moduledoc """
  Boots the Console: validate settings, start the read-only Repo, run the startup
  checks, and only then start the Endpoint. Any failure logs the specific reason
  and halts with a non-zero status.
  """
  use Application
  require Logger

  alias Console.{Config, Repo, StartupChecks}

  @impl true
  def start(_type, _args) do
    if Application.get_env(:autoangel_console, :autostart, true) do
      boot(System.get_env(), &System.halt/1)
    else
      Supervisor.start_link([], strategy: :one_for_one, name: Console.Supervisor)
    end
  end

  @doc false
  @spec boot(%{optional(String.t()) => String.t()}, (non_neg_integer() -> term()), keyword()) ::
          Supervisor.on_start() | term()
  def boot(env, halt, opts \\ []) do
    case Config.from_env(env) do
      {:ok, config} -> start_checked(config, halt, opts)
      {:error, problems} -> fail(Enum.join(problems, "; "), halt)
    end
  end

  defp start_checked(config, halt, opts) do
    publish(config)
    children = if config.database_url, do: [{Repo, Repo.options(config.database_url)}], else: []
    name = Keyword.get(opts, :name, Console.Supervisor)
    {:ok, sup} = Supervisor.start_link(children, strategy: :one_for_one, name: name)
    query = Keyword.get(opts, :query_fun, &repo_query/1)

    case StartupChecks.run(config.database_url, query) do
      :ok ->
        {:ok, _} = Supervisor.start_child(sup, {Console.Endpoint, endpoint_options(config)})
        {:ok, sup}

      {:error, reason} ->
        Supervisor.stop(sup)
        fail(StartupChecks.describe(reason), halt)
    end
  end

  defp fail(message, halt) do
    Logger.error("Console cannot start: #{message}")
    halt.(1)
  end

  defp repo_query(sql), do: Repo.query(sql, [], timeout: 5_000)

  @published [
    :auth,
    :health_exempt,
    :engine_not_reporting_seconds,
    :engine_url,
    :engine_token,
    :mcp_token,
    :mcp_write_tools,
    :recent_seconds,
    :chart_point_limit,
    :raw_retention_seconds,
    :aggregate_retention_seconds
  ]

  defp publish(config) do
    for key <- @published, do: Application.put_env(:autoangel_console, key, Map.fetch!(config, key))
  end

  defp endpoint_options(%{tls: nil} = config) do
    base(config) ++ [http: [ip: config.bind, port: config.port]]
  end

  defp endpoint_options(%{tls: tls} = config) do
    base(config) ++
      [
        https: [
          ip: config.bind,
          port: config.port,
          certfile: tls.cert_file,
          keyfile: tls.key_file
        ]
      ]
  end

  defp base(config) do
    [
      server: true,
      secret_key_base: config.secret_key_base,
      adapter: Bandit.PhoenixAdapter,
      live_view: [signing_salt: "console-live-view"],
      # Accept the LiveView socket only from the host the page was served from.
      check_origin: :conn
    ]
  end
end

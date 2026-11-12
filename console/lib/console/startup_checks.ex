defmodule Console.StartupChecks do
  @moduledoc """
  Checks run before the Endpoint starts. Every query is a read; nothing is written.

  Order: database configured, connection works, `schema_version` table exists and
  holds a value, and that value is one this build supports. The first failure wins.
  """

  @type query_fun :: (String.t() -> {:ok, %{rows: [[term()]]}} | {:error, term()})

  @type reason ::
          :database_not_configured
          | {:connection_failed, term()}
          | :schema_version_missing
          | :schema_version_empty
          | {:unsupported_schema_version, integer(), [pos_integer()]}

  @doc "Runs the checks against `query_fun`, which executes one read-only SQL string."
  @spec run(String.t() | nil, query_fun(), [pos_integer()]) :: :ok | {:error, reason()}
  def run(database_url, query_fun, supported \\ AutoangelSchema.supported_versions())

  def run(nil, _query_fun, _supported), do: {:error, :database_not_configured}

  def run(_database_url, query_fun, supported) do
    with :ok <- check_connection(query_fun),
         :ok <- check_table(query_fun),
         {:ok, found} <- read_version(query_fun) do
      check_supported(found, supported)
    end
  end

  @doc "A one-line, operator-readable reason; it never contains the database URL."
  @spec describe(reason()) :: String.t()
  def describe(:database_not_configured),
    do: "database is not configured: set AUTOANGEL_CONSOLE_DATABASE_URL"

  def describe({:connection_failed, reason}),
    do: "database connection failed: #{inspect(reason)}"

  def describe(:schema_version_missing),
    do: "the schema_version table does not exist: has the Engine created the schema?"

  def describe(:schema_version_empty),
    do: "the schema_version table has no value: has the Engine created the schema?"

  def describe({:unsupported_schema_version, found, supported}),
    do: "unsupported schema version: found #{found}, supported #{Enum.join(supported, ", ")}"

  defp check_connection(query_fun) do
    case query_fun.("SELECT 1") do
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, {:connection_failed, reason}}
    end
  end

  defp check_table(query_fun) do
    case query_fun.("SELECT to_regclass('public.schema_version')") do
      {:ok, %{rows: [[nil]]}} -> {:error, :schema_version_missing}
      {:ok, _result} -> :ok
      {:error, reason} -> {:error, {:connection_failed, reason}}
    end
  end

  defp read_version(query_fun) do
    case query_fun.("SELECT version FROM schema_version WHERE id = true") do
      {:ok, %{rows: [[version]]}} when is_integer(version) -> {:ok, version}
      {:ok, _no_value} -> {:error, :schema_version_empty}
      {:error, reason} -> {:error, {:connection_failed, reason}}
    end
  end

  defp check_supported(found, supported) do
    if found in supported,
      do: :ok,
      else: {:error, {:unsupported_schema_version, found, supported}}
  end
end

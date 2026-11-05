defmodule AutoangelTsdb.Retention do
  @moduledoc """
  TimescaleDB retention policy management and anomaly cleanup.

  Policy replacement is deliberately explicit: removing a policy before adding its
  configured replacement guarantees that a restart cannot leave two policies for
  the same hypertable or continuous aggregate.
  """

  use GenServer
  require Logger

  @default_raw_seconds 14 * 86_400
  @default_aggregate_seconds 13 * 30 * 86_400
  @default_anomaly_seconds 90 * 86_400
  @default_cleanup_interval_seconds 3_600

  @type retention :: %{
          required(:raw) => pos_integer(),
          required(:aggregate) => pos_integer(),
          required(:anomaly) => pos_integer()
        }

  @doc "Starts the periodic anomaly-retention worker after migrations succeed."
  @spec start_link({module(), term()}, map() | keyword()) :: GenServer.on_start()
  def start_link({query_module, connection}, config) do
    GenServer.start_link(__MODULE__, {query_module, connection, config})
  end

  @doc "Applies raw, aggregate, and refresh policies using the supplied connection."
  @spec apply_policies(term(), map() | keyword(), keyword()) :: {:ok, [term()]} | {:error, term()}
  def apply_policies(connection, config, opts \\ []) do
    retention = retention_config(config)
    query = Keyword.get(opts, :query, &Postgrex.query/3)
    statements = policy_statements(retention)

    Enum.reduce_while(statements, {:ok, []}, fn {name, sql, params}, {:ok, results} ->
      case query.(connection, sql, params) do
        {:ok, result} -> {:cont, {:ok, [{name, result} | results]}}
        {:error, reason} -> {:halt, {:error, {name, reason}}}
      end
    end)
    |> reverse_results()
  end

  @doc "Deletes only ended anomalies older than the configured cutoff."
  @spec cleanup_anomalies(term(), pos_integer(), keyword()) :: {:ok, term()} | {:error, term()}
  def cleanup_anomalies(connection, anomaly_retention_seconds, opts \\ [])
      when is_integer(anomaly_retention_seconds) and anomaly_retention_seconds > 0 do
    query = Keyword.get(opts, :query, &Postgrex.query/3)
    now = Keyword.get(opts, :now, DateTime.utc_now())
    cutoff = DateTime.add(now, -anomaly_retention_seconds, :second)

    query.(
      connection,
      "DELETE FROM anomalies WHERE end_time IS NOT NULL AND end_time < $1",
      [cutoff]
    )
  end

  @doc "Returns the validated retention settings with documented defaults."
  @spec retention_config(map() | keyword()) :: retention()
  def retention_config(config) do
    retention = get_value(config, :retention, %{})

    %{
      raw: positive_duration(get_value(retention, :raw, @default_raw_seconds), :raw),
      aggregate:
        positive_duration(
          get_value(retention, :aggregate, @default_aggregate_seconds),
          :aggregate
        ),
      anomaly: positive_duration(get_value(retention, :anomaly, @default_anomaly_seconds), :anomaly)
    }
  end

  @doc false
  @spec policy_statements(retention()) :: [{atom(), String.t(), [term()]}]
  def policy_statements(%{raw: raw, aggregate: aggregate}) do
    refresh_start = max(raw - 1, 1)

    [
      {:remove_raw, remove_retention_sql("samples"), []},
      {:add_raw, add_retention_sql("samples", raw), []},
      {:remove_minute, remove_retention_sql("samples_1m"), []},
      {:add_minute, add_retention_sql("samples_1m", aggregate), []},
      {:remove_hour, remove_retention_sql("samples_1h"), []},
      {:add_hour, add_retention_sql("samples_1h", aggregate), []},
      {:remove_minute_refresh, remove_refresh_sql("samples_1m"), []},
      {:add_minute_refresh, refresh_sql("samples_1m", refresh_start, "1 minute"), []},
      {:remove_hour_refresh, remove_refresh_sql("samples_1h"), []},
      {:add_hour_refresh, refresh_sql("samples_1h", refresh_start, "1 hour"), []}
    ]
  end

  @impl GenServer
  def init({_query_module, connection, config}) do
    retention = retention_config(config)
    interval = get_value(config, :retention_cleanup_interval, @default_cleanup_interval_seconds)

    if is_integer(interval) and interval > 0 do
      send(self(), :cleanup)

      {:ok,
       %{query: &Postgrex.query/3, connection: connection, retention: retention, interval: interval}}
    else
      {:stop, {:invalid_retention_cleanup_interval, interval}}
    end
  end

  @impl GenServer
  def handle_info(:cleanup, state) do
    case cleanup_anomalies(state.connection, state.retention.anomaly, query: state.query) do
      {:ok, _result} -> :ok
      {:error, reason} -> Logger.error("anomaly retention cleanup failed: #{inspect(reason)}")
    end

    Process.send_after(self(), :cleanup, state.interval * 1_000)
    {:noreply, state}
  end

  defp reverse_results({:ok, results}), do: {:ok, Enum.reverse(results)}
  defp reverse_results(error), do: error

  defp remove_retention_sql(name),
    do: "SELECT remove_retention_policy('#{name}', if_exists => TRUE)"

  defp add_retention_sql(name, seconds),
    do: "SELECT add_retention_policy('#{name}', drop_after => INTERVAL '#{interval_text(seconds)}')"

  defp remove_refresh_sql(name),
    do: "SELECT remove_continuous_aggregate_policy('#{name}', if_exists => TRUE)"

  defp refresh_sql(name, seconds, schedule),
    do:
      "SELECT add_continuous_aggregate_policy('#{name}', start_offset => INTERVAL '#{interval_text(seconds)}', end_offset => INTERVAL '1 second', schedule_interval => INTERVAL '#{schedule}')"

  defp interval_text(seconds) when rem(seconds, 86_400) == 0, do: "#{div(seconds, 86_400)} days"
  defp interval_text(seconds) when rem(seconds, 3_600) == 0, do: "#{div(seconds, 3_600)} hours"
  defp interval_text(seconds), do: "#{seconds} seconds"

  defp positive_duration(value, _name) when is_integer(value) and value > 0, do: value

  defp positive_duration(_value, name),
    do: raise(ArgumentError, "retention #{name} must be a duration greater than zero")

  defp get_value(map, key, default) when is_map(map),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))

  defp get_value(list, key, default) when is_list(list), do: Keyword.get(list, key, default)
end

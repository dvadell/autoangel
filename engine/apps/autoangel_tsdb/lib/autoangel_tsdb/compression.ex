defmodule AutoangelTsdb.Compression do
  @moduledoc """
  The raw-Sample compression policy (Requirements 40.1 to 40.6).

  After migrations the `samples` hypertable is set up for compression (segmented
  by Series, ordered by time) and exactly one compression policy exists, with the
  configured age (`compression.age` in seconds, default one day). A changed age
  replaces the earlier policy instead of adding a second one. If TimescaleDB
  rejects any step, `apply_policy/3` returns the error; the caller logs it and the
  Engine carries on with uncompressed data.
  """

  require Logger

  @default_age_seconds 86_400

  @doc "The default compression age in seconds."
  @spec default_age() :: 86_400
  def default_age, do: @default_age_seconds

  @doc "The validated compression age in seconds."
  @spec age(map() | keyword()) :: pos_integer()
  def age(config) do
    case get(get(config, :compression, %{}), :age, @default_age_seconds) do
      seconds when is_integer(seconds) and seconds > 0 ->
        seconds

      other ->
        raise ArgumentError, "compression age must be a positive integer, got: #{inspect(other)}"
    end
  end

  @doc """
  Sets up compression and replaces the compression policy. Returns `:ok` or
  `{:error, {step, reason}}`.
  """
  @spec apply_policy(term(), map() | keyword(), keyword()) :: :ok | {:error, term()}
  def apply_policy(connection, config, opts \\ []) do
    query = Keyword.get(opts, :query, &Postgrex.query/3)

    statements = [
      {:enable_compression,
       "ALTER TABLE samples SET (timescaledb.compress, timescaledb.compress_segmentby = 'series_id', timescaledb.compress_orderby = 'ts DESC')"},
      {:remove_policy, "SELECT remove_compression_policy('samples', if_exists => TRUE)"},
      {:add_policy,
       "SELECT add_compression_policy('samples', compress_after => INTERVAL '#{interval_text(age(config))}', if_not_exists => TRUE)"}
    ]

    Enum.reduce_while(statements, :ok, fn {step, sql}, :ok ->
      case query.(connection, sql, []) do
        {:ok, _result} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {step, reason}}}
      end
    end)
  rescue
    exception in ArgumentError -> {:error, {:invalid_age, Exception.message(exception)}}
  end

  @doc "Applies the policy; a rejection is logged and the Engine continues uncompressed."
  @spec apply_or_continue(term(), map() | keyword(), keyword()) :: :ok
  def apply_or_continue(connection, config, opts \\ []) do
    case apply_policy(connection, config, opts) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "compression policy not applied, continuing uncompressed: #{inspect(reason)}"
        )

        :ok
    end
  end

  defp interval_text(seconds) when rem(seconds, 86_400) == 0, do: "#{div(seconds, 86_400)} days"
  defp interval_text(seconds) when rem(seconds, 3_600) == 0, do: "#{div(seconds, 3_600)} hours"
  defp interval_text(seconds), do: "#{seconds} seconds"

  defp get(config, key, default) when is_map(config),
    do: Map.get(config, key, Map.get(config, Atom.to_string(key), default))

  defp get(config, key, default) when is_list(config), do: Keyword.get(config, key, default)
  defp get(_config, _key, default), do: default
end

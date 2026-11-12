defmodule Console.Config do
  @moduledoc """
  Reads and validates Console settings from the environment.

  | Variable | Meaning | Default |
  |---|---|---|
  | `AUTOANGEL_CONSOLE_DATABASE_URL` | read-only database URL | none (startup check reports it) |
  | `AUTOANGEL_CONSOLE_BIND` | IPv4 address to listen on | `127.0.0.1` |
  | `AUTOANGEL_CONSOLE_PORT` | listen port | `4000` |
  | `AUTOANGEL_CONSOLE_TLS_CERT_FILE` / `_TLS_KEY_FILE` | optional TLS, both or neither | off |
  | `AUTOANGEL_CONSOLE_ENGINE_NOT_REPORTING_SECONDS` | Heartbeat age before "Engine not reporting" | `60` |
  | `AUTOANGEL_CONSOLE_USER` / `_PASSWORD` | static basic credentials | none |
  | `AUTOANGEL_CONSOLE_TOKEN` | static bearer token | none |
  | `AUTOANGEL_CONSOLE_HEALTH_EXEMPT` | `true` lets `/healthz` skip authentication | `false` |
  | `AUTOANGEL_CONSOLE_SECRET_KEY_BASE` | signs the session cookie, at least 64 bytes | required |

  Credentials are required: with none configured the Console refuses to start.
  """

  @prefix "AUTOANGEL_CONSOLE_"
  @min_secret_bytes 64

  @type auth :: {:basic, String.t(), String.t()} | {:bearer, String.t()}

  @type t :: %{
          database_url: String.t() | nil,
          bind: :inet.ip4_address(),
          port: 1..65_535,
          tls: nil | %{cert_file: String.t(), key_file: String.t()},
          engine_not_reporting_seconds: pos_integer(),
          auth: [auth()],
          health_exempt: boolean(),
          secret_key_base: String.t()
        }

  @doc "Validates the environment. Returns every problem found, not just the first."
  @spec from_env(%{optional(String.t()) => String.t()}) :: {:ok, t()} | {:error, [String.t()]}
  def from_env(env \\ System.get_env()) do
    parsed = [
      port: parse_port(get(env, "PORT")),
      bind: parse_bind(get(env, "BIND")),
      tls: parse_tls(get(env, "TLS_CERT_FILE"), get(env, "TLS_KEY_FILE")),
      engine_not_reporting_seconds:
        parse_positive_integer(
          get(env, "ENGINE_NOT_REPORTING_SECONDS"),
          60,
          "ENGINE_NOT_REPORTING_SECONDS"
        ),
      auth: parse_auth(get(env, "USER"), get(env, "PASSWORD"), get(env, "TOKEN")),
      health_exempt: parse_boolean(get(env, "HEALTH_EXEMPT"), "HEALTH_EXEMPT"),
      secret_key_base: parse_secret(get(env, "SECRET_KEY_BASE"))
    ]

    case Enum.flat_map(parsed, fn {_key, result} -> List.wrap(error_of(result)) end) do
      [] ->
        values = Map.new(parsed, fn {key, {:ok, value}} -> {key, value} end)
        {:ok, Map.put(values, :database_url, get(env, "DATABASE_URL"))}

      errors ->
        {:error, errors}
    end
  end

  defp error_of({:error, message}), do: message
  defp error_of({:ok, _value}), do: nil

  defp get(env, name) do
    case Map.get(env, @prefix <> name) do
      nil -> nil
      value -> if String.trim(value) == "", do: nil, else: value
    end
  end

  defp var(name), do: @prefix <> name

  defp parse_port(nil), do: {:ok, 4000}

  defp parse_port(value) do
    case Integer.parse(value) do
      {port, ""} when port in 1..65_535 -> {:ok, port}
      _ -> {:error, "#{var("PORT")} must be an integer from 1 to 65535"}
    end
  end

  defp parse_bind(nil), do: {:ok, {127, 0, 0, 1}}

  defp parse_bind(value) do
    case :inet.parse_ipv4_address(String.to_charlist(value)) do
      {:ok, ip} -> {:ok, ip}
      {:error, _} -> {:error, "#{var("BIND")} must be an IPv4 address"}
    end
  end

  defp parse_tls(nil, nil), do: {:ok, nil}

  defp parse_tls(cert, key) when is_binary(cert) and is_binary(key) do
    cond do
      not File.regular?(cert) -> {:error, "#{var("TLS_CERT_FILE")} does not name a readable file"}
      not File.regular?(key) -> {:error, "#{var("TLS_KEY_FILE")} does not name a readable file"}
      true -> {:ok, %{cert_file: cert, key_file: key}}
    end
  end

  defp parse_tls(_cert, _key),
    do: {:error, "#{var("TLS_CERT_FILE")} and #{var("TLS_KEY_FILE")} must be set together"}

  defp parse_positive_integer(nil, default, _name), do: {:ok, default}

  defp parse_positive_integer(value, _default, name) do
    case Integer.parse(value) do
      {n, ""} when n > 0 -> {:ok, n}
      _ -> {:error, "#{var(name)} must be a positive integer"}
    end
  end

  defp parse_boolean(nil, _name), do: {:ok, false}
  defp parse_boolean(value, _name) when value in ["true", "false"], do: {:ok, value == "true"}
  defp parse_boolean(_value, name), do: {:error, "#{var(name)} must be true or false"}

  defp parse_auth(user, password, token) do
    basic = if user && password, do: [{:basic, user, password}], else: []
    bearer = if token, do: [{:bearer, token}], else: []

    cond do
      is_nil(user) != is_nil(password) ->
        {:error, "#{var("USER")} and #{var("PASSWORD")} must be set together"}

      basic ++ bearer == [] ->
        {:error,
         "set #{var("USER")} with #{var("PASSWORD")}, or #{var("TOKEN")}: the Console needs credentials"}

      true ->
        {:ok, basic ++ bearer}
    end
  end

  defp parse_secret(nil), do: {:error, "#{var("SECRET_KEY_BASE")} is required"}

  defp parse_secret(value) when byte_size(value) >= @min_secret_bytes, do: {:ok, value}

  defp parse_secret(_value),
    do: {:error, "#{var("SECRET_KEY_BASE")} must be at least #{@min_secret_bytes} bytes"}
end

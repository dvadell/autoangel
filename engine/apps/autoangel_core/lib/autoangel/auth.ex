defmodule Autoangel.Auth do
  @moduledoc "Plug authentication for the Engine's role-separated HTTP listeners."

  import Plug.Conn

  alias Autoangel.Secret

  @roles [:ingest, :control_api]
  @exempt_paths ["/healthz", "/readyz"]

  @type role :: :ingest | :control_api

  @doc "Initializes a role-specific authentication plug."
  @spec init(keyword()) :: keyword()
  def init(opts) when is_list(opts) do
    role = Keyword.fetch!(opts, :role)

    if role in @roles do
      opts
    else
      raise ArgumentError, "unsupported authentication role"
    end
  end

  @doc "Authenticates a request using only the configured credentials for its role."
  @spec call(Plug.Conn.t(), keyword()) :: Plug.Conn.t()
  def call(conn, opts) do
    role = Keyword.fetch!(opts, :role)
    config = Keyword.get(opts, :config, Application.get_env(:autoangel_core, :config, %{}))

    if exempt?(conn, role, config) or authorized?(conn, role, config) do
      conn
    else
      unauthorized(conn)
    end
  end

  @doc "Returns whether the request contains an exact credential for the selected role."
  @spec authorized?(Plug.Conn.t(), role(), map()) :: boolean()
  def authorized?(conn, role, config) when role in @roles and is_map(config) do
    credentials = role_credentials(config, role)

    case get_req_header(conn, "authorization") do
      [authorization] ->
        bearer_authorized?(authorization, credentials) or
          basic_authorized?(authorization, credentials)

      _ ->
        false
    end
  end

  def authorized?(_conn, _role, _config), do: false

  defp exempt?(conn, :control_api, config) when is_map(config) do
    conn.method == "GET" and conn.request_path in @exempt_paths and
      configured_exemptions(config) |> Enum.member?(conn.request_path)
  end

  defp exempt?(_conn, _role, _config), do: false

  defp configured_exemptions(config) when is_map(config) do
    config
    |> Map.get(:auth_exemptions, [])
    |> List.wrap()
    |> Enum.filter(&(&1 in @exempt_paths))
  end

  defp role_credentials(config, role) do
    config
    |> Map.get(role, %{})
    |> case do
      credentials when is_map(credentials) -> credentials
      _ -> %{}
    end
  end

  defp bearer_authorized?(authorization, credentials) do
    case String.split(authorization, " ", parts: 2) do
      [scheme, token] when scheme == "Bearer" and token != "" ->
        constant_time_equal?(token, secret_value(credentials[:token]))

      _ ->
        false
    end
  end

  defp basic_authorized?(authorization, credentials) do
    case String.split(authorization, " ", parts: 2) do
      [scheme, encoded] when scheme == "Basic" and encoded != "" ->
        with {:ok, decoded} <- Base.decode64(encoded),
             true <- Base.encode64(decoded) == encoded,
             [username, password] <- String.split(decoded, ":", parts: 2),
             true <- username != "" and password != "" do
          constant_time_equal?(username, credentials[:username]) and
            constant_time_equal?(password, secret_value(credentials[:password]))
        else
          _ -> false
        end

      _ ->
        false
    end
  end

  defp secret_value(%Secret{} = secret), do: Secret.unwrap(secret)
  defp secret_value(value) when is_binary(value), do: value
  defp secret_value(_value), do: nil

  defp constant_time_equal?(left, right) when is_binary(left) and is_binary(right) do
    :crypto.hash_equals(:crypto.hash(:sha256, left), :crypto.hash(:sha256, right))
  end

  defp constant_time_equal?(_left, _right), do: false

  defp unauthorized(conn) do
    conn
    |> put_resp_header("www-authenticate", "Bearer, Basic realm=\"autoangel\"")
    |> send_resp(401, "unauthorized")
    |> halt()
  end
end

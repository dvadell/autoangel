defmodule Autoangel.HTTP.Listener do
  @moduledoc "Starts the Engine's authenticated Bandit listeners with an all-or-nothing TLS policy."

  use Supervisor
  require Logger

  @default_ingest_port 9_400
  @default_control_port 9_401

  @type tls_options :: %{cert_file: binary() | nil, key_file: binary() | nil}

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl Supervisor
  def init(opts) do
    config = Keyword.get(opts, :config, Application.get_env(:autoangel_core, :config, %{}))

    with {:ok, tls} <- tls_options(config),
         {:ok, ingest} <- listener_options(:ingest, config, tls),
         {:ok, control} <- listener_options(:control_api, config, tls),
         :ok <- distinct_ports(ingest, control) do
      children = [bandit_child(ingest), bandit_child(control)]
      Supervisor.init(children, strategy: :one_for_one)
    else
      {:error, reason} ->
        Logger.error("HTTP listener configuration failed: #{format_error(reason)}")
        {:stop, reason}
    end
  end

  @doc "Builds the shared Bandit transport options, validating both TLS files together."
  @spec tls_options(map()) :: {:ok, tls_options()} | {:error, term()}
  def tls_options(config) when is_map(config) do
    tls = Map.get(config, :tls, %{})
    cert_file = value(tls, :cert_file)
    key_file = value(tls, :key_file)

    cond do
      is_nil(cert_file) and is_nil(key_file) ->
        {:ok, %{cert_file: nil, key_file: nil}}

      is_nil(cert_file) ->
        tls_error(:cert_file, cert_file, :missing_certificate)

      is_nil(key_file) ->
        tls_error(:key_file, key_file, :missing_key)

      true ->
        with :ok <- validate_pem_file(cert_file, :cert_file),
             :ok <- validate_pem_file(key_file, :key_file) do
          {:ok, %{cert_file: cert_file, key_file: key_file}}
        end
    end
  end

  def tls_options(_), do: {:error, {:tls, :invalid_configuration}}

  @spec listener_options(atom(), map(), tls_options()) :: {:ok, keyword()} | {:error, term()}
  def listener_options(role, config, tls) when role in [:ingest, :control_api] do
    ports = Map.get(config, :http, %{})
    port_key = if role == :ingest, do: :ingest_port, else: :control_port
    port = Map.get(ports, port_key, default_port(role))

    if is_integer(port) and port in 1..65_535 do
      {:ok,
       [
         plug: plug_for(role),
         scheme: scheme(tls),
         port: port
       ] ++ tls_transport_options(tls)}
    else
      {:error, {:port, port_key}}
    end
  end

  defp bandit_child(options), do: {Bandit, options}

  defp distinct_ports(ingest, control) do
    if Keyword.fetch!(ingest, :port) == Keyword.fetch!(control, :port),
      do: {:error, {:port, :must_be_distinct}},
      else: :ok
  end

  defp plug_for(:ingest), do: Autoangel.HTTP.AuthenticatedIngest
  defp plug_for(:control_api), do: Autoangel.HTTP.Control

  defp scheme(%{cert_file: nil, key_file: nil}), do: :http
  defp scheme(_tls), do: :https

  defp tls_transport_options(%{cert_file: nil, key_file: nil}), do: []
  defp tls_transport_options(tls), do: [certfile: tls.cert_file, keyfile: tls.key_file]

  defp default_port(:ingest), do: @default_ingest_port
  defp default_port(:control_api), do: @default_control_port

  defp value(map, key) when is_map(map), do: Map.get(map, key)
  defp value(_, _), do: nil

  defp tls_error(field, path, reason) do
    Logger.error("TLS #{field} configuration failed for #{inspect(path)}: #{reason}")
    {:error, {:tls, field, reason}}
  end

  defp validate_pem_file(path, field) do
    expected = if field == :cert_file, do: :certificate, else: :private_key

    with {:ok, contents} <- File.read(path),
         entries when entries != [] <- :public_key.pem_decode(contents),
         true <- Enum.any?(entries, &pem_entry_valid?(&1, expected)) do
      :ok
    else
      _ ->
        Logger.error("TLS #{field} file failed validation: #{path}")
        {:error, {:tls, field, :invalid_file}}
    end
  rescue
    _ ->
      Logger.error("TLS #{field} file failed validation: #{path}")
      {:error, {:tls, field, :invalid_file}}
  end

  defp pem_entry_valid?({type, der, _encryption}, :certificate)
       when type in [:Certificate, :certificate, :CERTIFICATE] do
    :public_key.pkix_decode_cert(der, :otp)
    true
  rescue
    _ -> false
  end

  defp pem_entry_valid?({type, der, encryption}, :private_key)
       when type in [:RSAPrivateKey, :ECPrivateKey, :PrivateKeyInfo, :private_key] do
    :public_key.pem_entry_decode({type, der, encryption})
    true
  rescue
    _ -> false
  end

  defp pem_entry_valid?(_, _), do: false

  defp format_error({:tls, field, reason}), do: "TLS #{field}: #{reason}"
  defp format_error({:port, field}), do: "invalid #{field}"
  defp format_error(reason), do: inspect(reason)
end

defmodule Autoangel.HTTP.AuthenticatedIngest do
  @moduledoc "Composes ingest authentication with the existing push pipeline."

  alias Autoangel.HTTP.Ingest

  @spec init(keyword()) :: keyword()
  def init(opts), do: Autoangel.Auth.init(Keyword.put(opts, :role, :ingest))

  @spec call(Plug.Conn.t(), keyword()) :: Plug.Conn.t()
  def call(conn, opts) do
    conn = Ingest.call(conn, opts)

    if conn.halted or conn.state == :sent,
      do: conn,
      else: Plug.Conn.send_resp(conn, 404, "not found")
  end
end

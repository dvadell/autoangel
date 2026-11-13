defmodule Console.MCP.Server do
  @moduledoc """
  JSON-RPC 2.0 for MCP, written directly (no MCP library; see `docs/decisions.md`).
  It only translates messages to and from `Console.MCP.Tools`.

  `handle/1` takes the raw request body and returns `{:reply, json}` or
  `:noreply` (a notification, or a response from the client). The HTTP endpoint
  at `/mcp` and the stdio forwarder share this path, so both transports give the
  same answer for the same request.
  """

  alias Console.MCP.Tools

  @versions ["2025-06-18", "2025-03-26", "2024-11-05"]

  @parse_error -32_700
  @invalid_request -32_600
  @method_not_found -32_601
  @invalid_params -32_602

  @spec handle(binary()) :: {:reply, binary()} | :noreply
  def handle(body) when is_binary(body) do
    case Jason.decode(body) do
      {:ok, %{} = message} when not is_struct(message) -> dispatch(message)
      {:ok, _other} -> {:reply, failure(nil, @invalid_request, "invalid request")}
      {:error, _reason} -> {:reply, failure(nil, @parse_error, "parse error")}
    end
  end

  defp dispatch(%{"jsonrpc" => "2.0", "method" => method} = message) when is_binary(method) do
    case Map.fetch(message, "id") do
      {:ok, id} when is_binary(id) or is_integer(id) -> request(id, method, message["params"])
      {:ok, _bad_id} -> {:reply, failure(nil, @invalid_request, "invalid request id")}
      :error -> :noreply
    end
  end

  # A response from the client (to a server request we never send), or junk.
  defp dispatch(%{"jsonrpc" => "2.0", "id" => _id} = message)
       when is_map_key(message, "result") or is_map_key(message, "error"),
       do: :noreply

  defp dispatch(_message), do: {:reply, failure(nil, @invalid_request, "invalid request")}

  defp request(id, "initialize", params) do
    requested = if is_map(params), do: params["protocolVersion"], else: nil
    version = if requested in @versions, do: requested, else: hd(@versions)

    success(id, %{
      "protocolVersion" => version,
      "capabilities" => %{"tools" => %{"listChanged" => false}},
      "serverInfo" => %{"name" => "autoangel-console", "version" => "0.1.0"}
    })
  end

  defp request(id, "ping", _params), do: success(id, %{})
  defp request(id, "tools/list", _params), do: success(id, %{"tools" => Tools.list()})

  # A write tool called while write tools are off is a known tool: `Tools.call/2`
  # answers it with the stable `write_tools_disabled` error. Any other name is a
  # protocol error.
  defp request(id, "tools/call", %{"name" => name} = params) when is_binary(name) do
    if Tools.known?(name),
      do: tool_result(id, Tools.call(name, params["arguments"])),
      else: {:reply, failure(id, @invalid_params, "unknown tool: #{name}")}
  end

  defp request(id, "tools/call", _params),
    do: {:reply, failure(id, @invalid_params, "tools/call needs a tool name")}

  defp request(id, method, _params),
    do: {:reply, failure(id, @method_not_found, "method not found: #{method}")}

  defp tool_result(id, {:ok, result}), do: success(id, content(result, false))
  defp tool_result(id, {:error, error}), do: success(id, content(%{"error" => error}, true))

  defp content(payload, error?) do
    %{
      "content" => [%{"type" => "text", "text" => Jason.encode!(payload)}],
      "isError" => error?
    }
  end

  defp success(id, result),
    do: {:reply, Jason.encode!(%{"jsonrpc" => "2.0", "id" => id, "result" => result})}

  defp failure(id, code, message),
    do:
      Jason.encode!(%{
        "jsonrpc" => "2.0",
        "id" => id,
        "error" => %{"code" => code, "message" => message}
      })
end

defmodule Console.MCP.Controller do
  @moduledoc """
  The streamable HTTP endpoint at `/mcp`. It only accepts `POST` with one JSON-RPC
  message and answers with `application/json` (or 202 for a notification); there
  is no server-sent event stream, so `GET` answers 405. `Console.MCP.Auth` has
  already run. An `Origin` header from another host is refused (DNS rebinding).
  """
  use Phoenix.Controller, formats: [:json]

  import Plug.Conn

  alias Console.MCP.Server

  @max_body 1_048_576

  def handle(%Plug.Conn{method: "POST"} = conn, _params) do
    with :ok <- check_origin(conn),
         {:ok, body, conn} <- read_body(conn, length: @max_body) do
      case Server.handle(body) do
        {:reply, json} ->
          conn |> put_resp_content_type("application/json") |> send_resp(200, json)

        :noreply ->
          send_resp(conn, 202, "")
      end
    else
      :forbidden -> send_resp(conn, 403, "")
      {:more, _partial, conn} -> send_resp(conn, 413, "")
      {:error, _reason} -> send_resp(conn, 400, "")
    end
  end

  def handle(conn, _params) do
    conn |> put_resp_header("allow", "POST") |> send_resp(405, "")
  end

  defp origin_host(origin), do: origin |> URI.parse() |> Map.get(:host)

  defp check_origin(conn) do
    case get_req_header(conn, "origin") do
      [] -> :ok
      [origin | _] -> if origin_host(origin) == conn.host, do: :ok, else: :forbidden
    end
  end
end

defmodule Console.MCP.Stdio do
  @moduledoc """
  `autoangel-console mcp-stdio`: an escript that connects a stdio MCP client to the
  Console's `/mcp` endpoint (Requirements 59.2 to 59.4).

  Each line on stdin is one JSON-RPC message. The line is sent unchanged as the
  body of a `POST` to the endpoint with the MCP credential, and the response body
  is written to stdout followed by a newline. A `202` (a notification) writes
  nothing. When the endpoint is unreachable or refuses the request, a JSON-RPC
  error for that request is written and the forwarder keeps reading.

  Settings (environment): `AUTOANGEL_CONSOLE_MCP_URL` (default
  `http://127.0.0.1:4000/mcp`) and `AUTOANGEL_CONSOLE_MCP_TOKEN`.
  """

  @default_url "http://127.0.0.1:4000/mcp"
  @timeout 30_000

  @type post_result :: {:ok, non_neg_integer(), binary()} | {:error, term()}
  @type post :: (binary() -> post_result())

  @doc "Escript entry point."
  @spec main([String.t()]) :: :ok
  def main(["mcp-stdio"]) do
    {:ok, _} = Application.ensure_all_started(:inets)
    {:ok, _} = Application.ensure_all_started(:ssl)
    :ok = :io.setopts(:standard_io, [:binary, {:encoding, :latin1}])

    url = System.get_env("AUTOANGEL_CONSOLE_MCP_URL", @default_url)
    token = System.get_env("AUTOANGEL_CONSOLE_MCP_TOKEN", "")
    run(&read_line/0, &write/1, post_fun(url, token))
  end

  def main(_args) do
    IO.puts(:stderr, "usage: autoangel-console mcp-stdio")
    System.halt(2)
  end

  @doc """
  Forwards every line from `read` through `post` and hands each output line to
  `write`, until `read` returns `:eof`. Exposed so the transports can be compared
  without a terminal or a network.
  """
  @spec run((-> binary() | :eof), (binary() -> any()), post()) :: :ok
  def run(read, write, post) do
    case read.() do
      :eof ->
        :ok

      line ->
        line |> String.trim_trailing("\n") |> String.trim_trailing("\r") |> forward(write, post)
        run(read, write, post)
    end
  end

  defp forward("", _write, _post), do: :ok

  defp forward(message, write, post) do
    case post.(message) do
      {:ok, 202, _body} -> :ok
      {:ok, 200, body} -> write.(body)
      {:ok, status, _body} -> fail(message, write, "the Console answered #{status}")
      {:error, _reason} -> fail(message, write, "the Console MCP endpoint is unreachable")
    end
  end

  # A notification has no id and gets no answer; anything else gets an error.
  defp fail(message, write, text) do
    case Jason.decode(message) do
      {:ok, %{"id" => id}} when is_binary(id) or is_integer(id) -> write.(error(id, text))
      {:ok, %{"method" => _}} -> :ok
      _ -> write.(error(nil, text))
    end
  end

  defp error(id, text) do
    Jason.encode!(%{
      "jsonrpc" => "2.0",
      "id" => id,
      "error" => %{"code" => -32_000, "message" => text}
    })
  end

  defp read_line do
    case IO.binread(:stdio, :line) do
      data when is_binary(data) -> data
      _eof_or_error -> :eof
    end
  end

  defp write(line), do: IO.binwrite(:stdio, line <> "\n")

  @doc "A `post` function that talks to `url` over HTTP with a bearer `token`."
  @spec post_fun(String.t(), String.t()) :: post()
  def post_fun(url, token) do
    fn body ->
      headers = [
        {~c"authorization", String.to_charlist("Bearer " <> token)},
        {~c"accept", ~c"application/json"}
      ]

      request = {String.to_charlist(url), headers, ~c"application/json", body}

      case :httpc.request(:post, request, [timeout: @timeout, connect_timeout: 5_000],
             body_format: :binary
           ) do
        {:ok, {{_, status, _}, _headers, response}} -> {:ok, status, response}
        {:error, reason} -> {:error, reason}
      end
    end
  end
end

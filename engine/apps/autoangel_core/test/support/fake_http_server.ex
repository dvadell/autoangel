defmodule Autoangel.Test.FakeHttpServer do
  @moduledoc """
  A tiny real HTTP server on a random loopback port for plugin tests. It answers
  every request with the configured status and body and reports each request to
  the test as `{:request, method, path, headers, body}`.
  """

  @doc "Starts the server; returns `{port, stop_fun}`. `status` may be changed with `set_status/2`."
  @spec start(pid(), non_neg_integer()) :: {pos_integer(), pid()}
  def start(test, status) do
    {:ok, agent} = Agent.start_link(fn -> status end)

    {:ok, listen} =
      :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: {127, 0, 0, 1}])

    {:ok, port} = :inet.port(listen)
    acceptor = spawn_link(fn -> accept(listen, test, agent) end)
    {port, %{agent: agent, acceptor: acceptor, listen: listen}}
  end

  def set_status(%{agent: agent}, status), do: Agent.update(agent, fn _ -> status end)

  def stop(%{agent: agent, acceptor: acceptor, listen: listen}) do
    Process.unlink(acceptor)
    Process.exit(acceptor, :kill)
    :gen_tcp.close(listen)
    if Process.alive?(agent), do: Agent.stop(agent)
    :ok
  catch
    :exit, _ -> :ok
  end

  defp accept(listen, test, agent) do
    case :gen_tcp.accept(listen) do
      {:ok, socket} ->
        spawn(fn -> serve(socket, test, agent) end)
        accept(listen, test, agent)

      {:error, _} ->
        :ok
    end
  end

  defp serve(socket, test, agent) do
    {head, rest} = read_head(socket, "")
    [request_line | header_lines] = String.split(head, "\r\n")
    [method, path | _] = String.split(request_line, " ")

    headers =
      for line <- header_lines,
          [k, v] <- [String.split(line, ":", parts: 2)],
          into: %{},
          do: {String.downcase(k), String.trim(v)}

    length = headers |> Map.get("content-length", "0") |> String.to_integer()
    body = read_body(socket, rest, length)
    send(test, {:request, method, path, headers, body})
    status = Agent.get(agent, & &1)

    :gen_tcp.send(
      socket,
      "HTTP/1.1 #{status} X\r\ncontent-length: 2\r\ncontent-type: text/plain\r\nconnection: close\r\n\r\nok"
    )

    :gen_tcp.close(socket)
  end

  defp read_head(socket, acc) do
    case String.split(acc, "\r\n\r\n", parts: 2) do
      [head, rest] ->
        {head, rest}

      [_incomplete] ->
        {:ok, data} = :gen_tcp.recv(socket, 0, 2_000)
        read_head(socket, acc <> data)
    end
  end

  defp read_body(_socket, rest, length) when byte_size(rest) >= length,
    do: binary_part(rest, 0, length)

  defp read_body(socket, rest, length) do
    {:ok, data} = :gen_tcp.recv(socket, 0, 2_000)
    read_body(socket, rest <> data, length)
  end
end

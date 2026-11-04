defmodule Autoangel.LoggerSecretFilter do
  @moduledoc "Logger filter that replaces configured secret substrings in messages."

  def filter(event, secrets) do
    do_filter(event, secrets)
  rescue
    _ -> redact_event(event)
  catch
    _, _ -> redact_event(event)
  end

  defp do_filter(%{msg: msg} = event, secrets) do
    rendered = render_message(msg)
    redacted = redact(rendered, secrets)

    if rendered == redacted do
      event
    else
      %{event | msg: {:string, redacted}}
    end
  end

  defp do_filter(event, _secrets), do: event
  defp redact_event(%{msg: _} = event), do: %{event | msg: {:string, "[REDACTED]"}}
  defp redact_event(event), do: event

  defp render_message(message) when is_binary(message), do: message
  defp render_message({:string, chars}), do: chardata_to_string(chars)

  defp render_message({:report, report}),
    do: inspect(report, limit: :infinity, printable_limit: :infinity)

  defp render_message({format, args}) when is_list(args),
    do: :io_lib.format(format, args) |> IO.iodata_to_binary()

  defp render_message(_message), do: "[REDACTED]"

  defp redact(message, secrets) when is_binary(message) do
    Enum.reduce(secrets, message, fn secret, acc ->
      Enum.reduce(secret_patterns(secret), acc, &String.replace(&2, &1, "[REDACTED]"))
    end)
  end

  defp secret_patterns(%Autoangel.Secret{} = secret),
    do: secret_patterns(Autoangel.Secret.unwrap(secret))

  defp secret_patterns(secret) when is_binary(secret) do
    escaped = inspect(secret, limit: :infinity, printable_limit: :infinity)
    escaped = String.trim_leading(escaped, "\"") |> String.trim_trailing("\"")
    [secret, escaped] |> Enum.uniq()
  end

  defp chardata_to_string(chars) do
    IO.chardata_to_string(chars)
  rescue
    _ -> "[REDACTED]"
  end
end

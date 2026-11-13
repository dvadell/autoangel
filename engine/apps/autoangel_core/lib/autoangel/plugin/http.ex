defmodule Autoangel.Plugin.Http do
  @moduledoc """
  What the HTTP alert plugins share (Requirements 24.1 to 24.4): destination
  validation, one POST per delivery attempt, and the split of every reply into
  success, retryable and permanent.

  | Reply | Outcome |
  |---|---|
  | 2xx | delivered |
  | 408, 425, 429, 5xx | retryable |
  | network error, timeout | retryable |
  | other 4xx, 3xx | permanent (the destination or the credentials are wrong) |

  A plugin passes `:http_client` in its configuration to replace the network:
  a function taking the URL, the headers and the body and returning
  `{:ok, {status, headers, body}}`, `{status, headers, body}` or `{:error, reason}`.
  Reasons never include the URL, because it can hold a secret token.
  """

  alias Autoangel.Secret

  @default_timeout 10_000

  @doc "Classifies an HTTP status as `:ok`, `:retryable` or `:permanent`."
  @spec classify_status(integer()) :: :ok | :retryable | :permanent
  def classify_status(status) when status in 200..299, do: :ok
  def classify_status(status) when status in [408, 425, 429], do: :retryable
  def classify_status(status) when status >= 500, do: :retryable
  def classify_status(_status), do: :permanent

  @doc "Turns a reply from an HTTP client into a delivery outcome."
  @spec outcome(term()) :: Autoangel.Plugin.delivery_outcome()
  def outcome({:ok, {{_version, status, _reason}, headers, body}}),
    do: outcome({status, headers, body})

  def outcome({:ok, {status, headers, body}}), do: outcome({status, headers, body})

  def outcome({status, _headers, _body}) when is_integer(status) do
    case classify_status(status) do
      :ok -> :ok
      :retryable -> {:retryable, {:http_status, status}}
      :permanent -> {:permanent, {:http_status, status}}
    end
  end

  def outcome({:error, reason}), do: {:retryable, {:http_error, safe_reason(reason)}}
  def outcome(_other), do: {:retryable, :invalid_http_response}

  # A client error can carry the request, and with it the URL and its token.
  defp safe_reason(reason) when is_atom(reason), do: reason
  defp safe_reason({reason, _detail}) when is_atom(reason), do: reason
  defp safe_reason(_reason), do: :request_failed

  @doc "Sends one POST and returns the delivery outcome."
  @spec post(String.t(), [{String.t(), String.t()}], binary(), map()) ::
          Autoangel.Plugin.delivery_outcome()
  def post(url, headers, body, config) do
    case Map.get(config, :http_client) do
      client when is_function(client, 3) ->
        outcome(client.(url, headers, body))

      _network ->
        _ = :inets.start()
        _ = :ssl.start()
        timeout = Map.get(config, :timeout, @default_timeout)

        request =
          {String.to_charlist(url),
           Enum.map(headers, fn {k, v} -> {String.to_charlist(k), String.to_charlist(v)} end),
           content_type(headers), body}

        outcome(:httpc.request(:post, request, [timeout: timeout], []))
    end
  end

  defp content_type(headers) do
    case Enum.find(headers, fn {name, _} -> String.downcase(name) == "content-type" end) do
      {_, value} -> String.to_charlist(value)
      nil -> ~c"text/plain"
    end
  end

  @doc "True for an http or https URL with a host and no whitespace."
  @spec valid_url?(term(), keyword()) :: boolean()
  def valid_url?(value, opts \\ []) do
    schemes = Keyword.get(opts, :schemes, ["http", "https"])
    value |> unwrap() |> url_with_scheme?(schemes)
  end

  defp url_with_scheme?(value, schemes) when is_binary(value) do
    uri = URI.parse(value)

    not String.match?(value, ~r/\s/) and uri.scheme in schemes and is_binary(uri.host) and
      uri.host != ""
  end

  defp url_with_scheme?(_value, _schemes), do: false

  @doc "A configuration value, with a secret unwrapped."
  @spec unwrap(term()) :: term()
  def unwrap(%Secret{} = secret), do: Secret.unwrap(secret)
  def unwrap(value), do: value

  @doc "True for a map of header names to values, with no line breaks in either."
  @spec valid_headers?(term()) :: boolean()
  def valid_headers?(headers) when is_map(headers) do
    Enum.all?(headers, fn {name, value} ->
      is_binary(name) and Regex.match?(~r/\A[!#$%&'*+.^_`|~0-9A-Za-z-]+\z/, name) and
        is_binary(unwrap(value)) and not String.match?(unwrap(value), ~r/[\r\n]/)
    end)
  end

  def valid_headers?(_other), do: false

  @doc "The field names that failed, in order, for a list of `{field, valid?}` checks."
  @spec failed([{atom(), boolean()}]) :: :ok | {:error, [atom()]}
  def failed(checks) do
    case for({field, false} <- checks, do: field) do
      [] -> :ok
      fields -> {:error, fields}
    end
  end

  @doc "Cuts text to at most `max` characters."
  @spec truncate(String.t(), pos_integer()) :: String.t()
  def truncate(text, max), do: String.slice(text, 0, max)
end

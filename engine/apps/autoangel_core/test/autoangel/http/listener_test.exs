defmodule Autoangel.HTTP.ListenerTest do
  use ExUnit.Case, async: true

  alias Autoangel.HTTP.Listener

  test "uses plain HTTP when TLS is not configured" do
    assert {:ok, %{cert_file: nil, key_file: nil}} = Listener.tls_options(%{})

    assert {:ok, options} =
             Listener.listener_options(:ingest, %{}, %{cert_file: nil, key_file: nil})

    assert options[:scheme] == :http
    assert options[:port] == 9_400
    refute Keyword.has_key?(options, :certfile)
  end

  test "requires both TLS files and reports the missing file" do
    assert {:error, {:tls, :cert_file, :missing_certificate}} =
             Listener.tls_options(%{tls: %{key_file: "/tmp/key.pem"}})

    assert {:error, {:tls, :key_file, :missing_key}} =
             Listener.tls_options(%{tls: %{cert_file: "/tmp/cert.pem"}})
  end

  test "rejects unreadable TLS files instead of falling back to HTTP" do
    config = %{tls: %{cert_file: "/does/not/exist/cert.pem", key_file: "/does/not/exist/key.pem"}}

    assert {:error, {:tls, :cert_file, :invalid_file}} = Listener.tls_options(config)
  end
end

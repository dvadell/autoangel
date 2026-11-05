defmodule Autoangel.Plugin.EmailTest do
  use ExUnit.Case, async: true

  alias Autoangel.Plugin.Email

  test "reports all required missing configuration fields" do
    assert {:error, fields} = Email.validate_config(%{})
    assert Enum.sort(fields) == [:recipients, :sender, :server]
  end

  test "accepts a configured server, sender and recipients" do
    assert :ok =
             Email.validate_config(%{
               server: "smtp.example",
               sender: "a@example",
               recipients: ["b@example"]
             })
  end

  test "classifies SMTP replies" do
    assert Email.classify_response(250) == :ok
    assert Email.classify_response(354) == :ok
    assert Email.classify_response(421) == :retryable
    assert Email.classify_response(535) == :permanent
    assert Email.classify_response(550) == :permanent
  end

  test "rejects an empty recipient list and non-string recipients" do
    assert {:error, fields} = Email.validate_config(%{server: "smtp", sender: "a", recipients: []})
    assert :recipients in fields

    assert {:error, fields} = Email.validate_config(%{server: "smtp", sender: "a", recipients: [1]})
    assert :recipients in fields
  end
end

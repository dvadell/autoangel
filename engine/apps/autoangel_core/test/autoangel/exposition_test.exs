defmodule Autoangel.ExpositionTest do
  use ExUnit.Case, async: true

  alias Autoangel.Exposition

  test "parses metadata, escaped labels, special values and timestamps" do
    body =
      "# HELP http_requests_total Total\\nrequests\n# TYPE http_requests_total counter\nhttp_requests_total{method=\"get\",path=\"/\\\"x\"} +Inf 1700000000000\n"

    assert {:ok, [family]} = Exposition.parse(body)
    assert family.name == "http_requests_total"
    assert family.help == "Total\nrequests"
    assert family.type == :counter
    assert [%Exposition.Sample{} = sample] = family.samples
    assert sample.labels == %{"method" => "get", "path" => "/\"x"}
    assert sample.value == :inf
    assert sample.timestamp == 1_700_000_000_000
    assert sample.type_hint == :counter
  end

  test "prints labels in sorted order and round trips" do
    families = [
      %Exposition.Family{
        name: "metric",
        samples: [
          %Exposition.Sample{
            name: "metric",
            labels: %{"z" => "a", "a" => "line\nvalue"},
            value: 1.25,
            timestamp: 42
          }
        ]
      }
    ]

    text = families |> Exposition.print() |> IO.iodata_to_binary()

    assert text == "metric{a=\"line\\nvalue\",z=\"a\"} 1.25 42\n"
    assert {:ok, [family]} = Exposition.parse(text)
    assert hd(family.samples).labels == %{"a" => "line\nvalue", "z" => "a"}
  end

  test "reports the first invalid line" do
    assert {:error, %{line: 2}} = Exposition.parse("valid_metric 1\nnot valid metric 2\n")
  end
end

defmodule Autoangel.ExpositionTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

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

  # **Validates: Requirements 1.5, 2.5**
  property "Feature: v1, Property 2: an invalid line is reported by its line number" do
    check all(
            valid_line_count <- integer(0..20),
            invalid_line <-
              member_of([
                "not valid metric 2",
                "metric 1.2.3",
                "metric{label=\"unterminated} 1",
                "metric{label=\"x\" bad} 1",
                "metric 1 not-an-integer"
              ])
          ) do
      valid_lines =
        Enum.take(1..20, valid_line_count)
        |> Enum.map(fn index -> "metric_#{index} #{index}.0" end)

      expected_line = valid_line_count + 1
      body = Enum.join(valid_lines ++ [invalid_line, "metric_after_invalid 1.0"], "\n")

      assert {:error, %{line: ^expected_line}} = Exposition.parse(body)
    end
  end
end

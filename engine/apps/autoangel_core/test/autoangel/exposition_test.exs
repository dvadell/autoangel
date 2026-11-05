defmodule Autoangel.ExpositionTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  import StreamData

  alias Autoangel.Exposition

  # **Validates: Requirements 1.6, 1.7**
  property "Feature: v1, Property 1: exposition round trip" do
    check all(families <- list_of(family_generator(), min_length: 1, max_length: 8), max_runs: 100) do
      families =
        families
        |> Enum.with_index()
        |> Enum.map(fn {family, index} ->
          name = "metric_#{index}"

          %Exposition.Family{
            name: name,
            samples: Enum.map(family.samples, &%{&1 | name: name})
          }
        end)

      text = families |> Exposition.print() |> IO.iodata_to_binary()
      assert {:ok, parsed} = Exposition.parse(text)

      expected = Enum.flat_map(families, & &1.samples)
      actual = Enum.flat_map(parsed, & &1.samples)

      assert length(actual) == length(expected)

      expected
      |> Enum.sort_by(&sample_identity/1)
      |> Enum.zip(Enum.sort_by(actual, &sample_identity/1))
      |> Enum.each(fn {original, round_tripped} ->
        assert round_tripped.name == original.name
        assert round_tripped.labels == original.labels
        assert round_tripped.timestamp == original.timestamp
        assert values_equal?(round_tripped.value, original.value)
      end)
    end
  end

  defp family_generator do
    map(
      list_of(sample_generator(), min_length: 1, max_length: 8),
      &%Exposition.Family{name: "placeholder", samples: &1}
    )
  end

  defp sample_generator do
    gen all(
          labels <- map_of(label_name_generator(), label_value_generator(), max_length: 4),
          value <-
            one_of([
              float(min: -1.0e6, max: 1.0e6),
              member_of([:nan, :inf, :neg_inf])
            ]),
          timestamp <- one_of([constant(nil), integer(-1_000_000_000_000..1_000_000_000_000)])
        ) do
      %Exposition.Sample{labels: labels, value: value, timestamp: timestamp}
    end
  end

  defp label_name_generator do
    map(integer(0..100), &"label_#{&1}")
  end

  defp label_value_generator do
    one_of([
      string(:alphanumeric, max_length: 12),
      member_of(["", "line\nvalue", "quote\"slash\\"])
    ])
  end

  defp sample_identity(sample),
    do:
      {sample.name, Enum.sort(sample.labels), sample.timestamp,
       :erlang.term_to_binary(sample.value)}

  defp values_equal?(:nan, :nan), do: true
  defp values_equal?(left, right), do: left == right

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

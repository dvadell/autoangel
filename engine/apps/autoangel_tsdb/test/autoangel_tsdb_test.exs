defmodule AutoangelTsdbTest do
  use ExUnit.Case, async: true

  test "binary COPY payload has PostgreSQL header, rows and trailer" do
    rows = [{<<1::unsigned-big-128>>, 123, 4.5}]
    payload = AutoangelTsdb.copy_binary(rows)

    assert <<"PGCOPY\n", 0xFF, "\r\n\0", 0::signed-big-32, 0::signed-big-32, 3::signed-big-16,
             16::signed-big-32, 1::unsigned-big-128, 8::signed-big-32, 123::signed-big-64,
             8::signed-big-32, 4.5::float-big, -1::signed-big-16>> = payload
  end

  test "benchmark data uses monotonic 10 to 60 second intervals per series" do
    rows = AutoangelTsdb.benchmark_rows(20_000, 2)

    assert length(rows) == 40_000
    assert rows |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> length() == 20_000

    assert Enum.all?(Enum.chunk_every(rows, 2), fn [{_, first, _}, {_, second, _}] ->
             (second - first) in 10_000_000..60_000_000
           end)
  end
end

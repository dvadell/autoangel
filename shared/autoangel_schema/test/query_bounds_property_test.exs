defmodule AutoangelSchema.QueryBoundsPropertyTest do
  # Feature: v1, Property 57: Console queries stay within bounds
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias AutoangelSchema.Query
  alias AutoangelSchema.Query.Bounds

  @max Bounds.maximums()
  @end_time ~U[2026-01-01 00:00:00Z]

  defp request do
    gen all(
          span <- integer(0..(2 * @max.range_seconds)),
          series_limit <- integer(1..500),
          row_limit <- integer(1..100_000)
        ) do
      [
        range: {DateTime.add(@end_time, -span, :second), @end_time},
        series_limit: series_limit,
        row_limit: row_limit
      ]
    end
  end

  property "resolved bounds never exceed the maximums and report each clamp" do
    check all(opts <- request()) do
      {from, to} = opts[:range]
      assert {:ok, b} = Bounds.resolve(opts)

      assert DateTime.diff(b.to, b.from, :second) <= @max.range_seconds
      assert b.series_limit <= @max.series
      assert b.row_limit <= @max.rows
      assert b.to == to

      assert :range in b.clamps == DateTime.diff(to, from, :second) > @max.range_seconds
      assert :series_limit in b.clamps == opts[:series_limit] > @max.series
      assert :row_limit in b.clamps == opts[:row_limit] > @max.rows
    end
  end

  property "built queries carry the clamped row limit and Series cap" do
    check all(opts <- request(), ids <- list_of(constant(Ecto.UUID.generate()), max_length: 120)) do
      {:ok, b} = Bounds.resolve(opts)

      assert %Ecto.Query.LimitExpr{params: [{limit, _}]} = Query.samples(ids, b).limit
      assert limit == b.row_limit
      assert length(Enum.take(ids, b.series_limit)) <= @max.series
    end
  end

  test "missing or invalid bounds are refused" do
    range = {~U[2026-01-01 00:00:00Z], ~U[2026-01-02 00:00:00Z]}

    assert {:error, {:missing, :range}} = Bounds.resolve(series_limit: 1, row_limit: 1)
    assert {:error, {:missing, :series_limit}} = Bounds.resolve(range: range, row_limit: 1)
    assert {:error, {:missing, :row_limit}} = Bounds.resolve(range: range, series_limit: 1)

    assert {:error, {:invalid, :row_limit}} =
             Bounds.resolve(range: range, series_limit: 1, row_limit: 0)

    assert {:error, {:invalid, :range}} =
             Bounds.resolve(range: {elem(range, 1), elem(range, 0)}, series_limit: 1, row_limit: 1)
  end

  test "supported versions include the library version" do
    assert AutoangelSchema.version() in AutoangelSchema.supported_versions()
  end
end

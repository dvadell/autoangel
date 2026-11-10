defmodule AutoangelReplay.SmokeTest do
  @moduledoc "Task 24.1 smoke check: readers and injectors work end to end."
  use ExUnit.Case, async: true

  alias AutoangelReplay.{Inject, Reader, Series}

  test "a CSV file is read and every anomaly type is injected deterministically" do
    path = Path.join(System.tmp_dir!(), "replay-smoke-#{System.unique_integer([:positive])}.csv")
    rows = for i <- 0..199, do: ~s("cpu{host=""a""}",#{i * 60_000},#{10 + rem(i, 5)})
    File.write!(path, Enum.join(["series,timestamp_ms,value" | rows], "\n"))
    on_exit(fn -> File.rm(path) end)

    assert {:ok, [%Series{name: "cpu", labels: %{"host" => "a"}} = s]} = Reader.read(path)
    assert length(s.samples) == 200

    assert {:ok, {out, labels}} = Inject.run([s], 42)
    assert Enum.sort(Enum.map(labels, & &1.type)) == Enum.sort(Inject.types())
    assert Inject.run([s], 42) == {:ok, {out, labels}}
  end

  test "types go only to Series long enough; too-short input is an error, not a silent skip" do
    long = Series.new("long", %{}, for(i <- 0..199, do: {i * 1_000, 1.0 * rem(i, 7)}))
    short = Series.new("short", %{}, for(i <- 0..7, do: {i * 1_000, 1.0}))

    assert {:ok, {_out, labels}} = Inject.run([short, long], 1)
    assert Enum.sort(Enum.map(labels, & &1.type)) == Enum.sort(Inject.types())
    assert Enum.all?(labels, &(&1.series_key == Series.key(long)))

    assert {:error, {:insufficient_capacity, 5, 0}} = Inject.run([short], 1)
  end

  test "a short Series next to a long one never makes a fitting plan fail" do
    long = Series.new("long", %{}, for(i <- 0..199, do: {i * 1_000, 1.0 * rem(i, 7)}))

    for len <- 14..39 do
      short = Series.new("short", %{}, for(i <- 0..(len - 1), do: {i * 1_000, 1.0 * rem(i, 3)}))
      assert {:ok, {_out, labels}} = Inject.run([long, short], len)
      assert Enum.sort(Enum.map(labels, & &1.type)) == Enum.sort(Inject.types())
    end
  end

  test "a plan larger than the Series can hold is an error, not a silent shrink" do
    long = Series.new("long", %{}, for(i <- 0..199, do: {i * 1_000, 1.0 * rem(i, 7)}))
    assert {:error, {:insufficient_capacity, 150, 15}} = Inject.run([long], 1, per_type: 30)
    assert {:ok, {_out, labels}} = Inject.run([long], 1, per_type: 3)
    assert length(labels) == 15
    assert_raise ArgumentError, ~r/:per_type/, fn -> Inject.run([long], 1, per_type: 0) end
  end
end

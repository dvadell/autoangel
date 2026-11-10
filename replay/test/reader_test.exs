defmodule AutoangelReplay.ReaderTest do
  @moduledoc "Task 24.5: replay input readers (Requirements 77.1, 77.7)."
  use ExUnit.Case, async: true

  alias AutoangelReplay.{Reader, Series}

  defp write(ext, contents) do
    path = Path.join(System.tmp_dir!(), "reader-#{System.unique_integer([:positive])}#{ext}")
    File.write!(path, contents)
    on_exit(fn -> File.rm(path) end)
    path
  end

  defp export(result),
    do:
      JSON.encode!(%{
        "status" => "success",
        "data" => %{"resultType" => "matrix", "result" => result}
      })

  describe "CSV" do
    test "reads Series grouped by selector, ordered by timestamp, with special values" do
      path =
        write(".csv", """
        series,timestamp_ms,value
        "cpu{host=""a"",job=""x""}",2000,2.5
        cpu{host="a"},1000,NaN
        "cpu{host=""a"",job=""x""}",1000,1.5
        up,3000,+Inf
        up,1000,-Inf
        """)

      assert {:ok, series} = Reader.read(path)
      by_key = Map.new(series, &{Series.key(&1), &1.samples})

      assert by_key[{"cpu", [{"host", "a"}, {"job", "x"}]}] == [{1000, 1.5}, {2000, 2.5}]
      assert by_key[{"cpu", [{"host", "a"}]}] == [{1000, :nan}]
      assert by_key[{"up", []}] == [{1000, :neg_inf}, {3000, :inf}]
    end

    test "an escaped quote inside a label value survives" do
      # File text: "m{p=""a\""b""}",1,1  ->  selector m{p="a\"b"}  ->  value a"b
      path =
        write(".csv", ~S(series,timestamp_ms,value) <> "\n" <> ~S("m{p=""a\""b""}",1,1) <> "\n")

      assert {:ok, [%Series{labels: %{"p" => ~s(a"b)}}]} = Reader.read(path)
    end

    test "a bad header, row, selector or value is reported as unreadable" do
      for body <- [
            "ts,value\n1,2\n",
            "series,timestamp_ms,value\ncpu,notanumber,1\n",
            "series,timestamp_ms,value\n9cpu,1,1\n",
            "series,timestamp_ms,value\ncpu,1,abc\n",
            "series,timestamp_ms,value\ncpu{host=a},1,1\n"
          ] do
        path = write(".csv", body)
        assert {:error, {:unreadable, ^path, _reason}} = Reader.read(path), body
      end
    end

    test "a header-only file has no Series" do
      path = write(".csv", "series,timestamp_ms,value\n")
      assert {:error, {:no_series, ^path}} = Reader.read(path)
    end
  end

  describe "Prometheus range export JSON" do
    test "reads the matrix, converting seconds to milliseconds" do
      path =
        write(
          ".json",
          export([
            %{
              "metric" => %{"__name__" => "load", "host" => "a"},
              "values" => [[1_700_000_000.5, "1.5"], [1_700_000_060, "NaN"]]
            }
          ])
        )

      assert {:ok, [s]} = Reader.read(path)
      assert Series.key(s) == {"load", [{"host", "a"}]}
      assert s.samples == [{1_700_000_000_500, 1.5}, {1_700_000_060_000, :nan}]
    end

    test "a Series with no Samples is reported by name" do
      path =
        write(
          ".json",
          export([
            %{"metric" => %{"__name__" => "ok"}, "values" => [[1, "1"]]},
            %{"metric" => %{"__name__" => "load", "host" => "a"}, "values" => []}
          ])
        )

      assert {:error, {:empty_series, ~s(load{host="a"})}} = Reader.read(path)
    end

    test "an empty result has no Series" do
      path = write(".json", export([]))
      assert {:error, {:no_series, ^path}} = Reader.read(path)
    end

    test "non-JSON, non-matrix or malformed values are unreadable" do
      for body <- [
            "{not json",
            JSON.encode!(%{"data" => %{"resultType" => "vector", "result" => []}}),
            export([%{"metric" => %{"__name__" => "x"}, "values" => [[1, 2]]}]),
            export([%{"metric" => %{"__name__" => "x"}, "values" => [[1, "oops"]]}])
          ] do
        path = write(".json", body)
        assert {:error, {:unreadable, ^path, _}} = Reader.read(path), body
      end
    end
  end

  describe "files" do
    test "a missing file is unreadable and names the path" do
      assert {:error, {:unreadable, "/no/such/file.csv", :enoent}} =
               Reader.read("/no/such/file.csv")
    end

    test "an unsupported extension is unreadable" do
      path = write(".txt", "x")
      assert {:error, {:unreadable, ^path, {:unsupported_format, ".txt"}}} = Reader.read(path)
    end

    test "a duplicate Series within or across files is rejected" do
      a = write(".csv", "series,timestamp_ms,value\nup,1,1\n")
      b = write(".json", export([%{"metric" => %{"__name__" => "up"}, "values" => [[2, "1"]]}]))
      assert {:error, {:unreadable, _, {:duplicate_series, "up"}}} = Reader.read_all([a, b])

      dup =
        write(
          ".json",
          export([
            %{"metric" => %{"__name__" => "up"}, "values" => [[1, "1"]]},
            %{"metric" => %{"__name__" => "up"}, "values" => [[2, "1"]]}
          ])
        )

      assert {:error, {:unreadable, ^dup, {:duplicate_series, "up"}}} = Reader.read(dup)
    end

    test "read_all stops at the first error and returns no Series" do
      good = write(".csv", "series,timestamp_ms,value\nup,1,1\n")

      assert {:error, {:unreadable, "/missing.csv", :enoent}} =
               Reader.read_all([good, "/missing.csv"])
    end
  end
end

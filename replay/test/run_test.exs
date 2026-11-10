defmodule AutoangelReplay.RunTest do
  @moduledoc """
  Task 24.2: replay through an in-process Engine on a simulated clock, the
  matching rule and the report (Requirements 77.4, 77.5, 77.6).
  """
  use ExUnit.Case, async: false

  alias AutoangelReplay.{Run, Series}

  @step 60_000
  @base 1_700_000_000_000

  # Two days of one-minute Samples with small seeded noise, a four-Sample spike
  # after the learning day (long enough for the default Persistence_Rule of
  # three Samples) and a three-hour silence.
  defp input do
    :rand.seed(:exsss, 3)
    spike_i = 1_700
    silent = 2_000..2_179

    samples =
      for i <- 0..2_880, i not in silent do
        v = 50.0 + :rand.uniform()
        {@base + i * @step, if(i in spike_i..(spike_i + 3), do: v + 400.0, else: v)}
      end

    s = Series.new("flat", %{"host" => "a"}, samples)
    key = Series.key(s)

    labels = [
      %{
        series_key: key,
        type: :spike,
        start_ms: @base + spike_i * @step,
        end_ms: @base + spike_i * @step
      },
      %{
        series_key: key,
        type: :silence,
        start_ms: @base + Enum.min(silent) * @step,
        end_ms: @base + Enum.max(silent) * @step
      }
    ]

    {[s], labels}
  end

  test "a replay detects the injected anomalies through the push API on simulated time" do
    {series, labels} = input()
    report = Run.run(series, labels, 11)

    assert report.seed == 11
    assert report.config_digest =~ ~r/^[0-9a-f]{64}$/
    assert report.per_type.silence.recall == 1.0
    assert report.per_type.spike.recall == 1.0
    assert report.overall.recall == 1.0
    assert report.alerts >= 2
    assert is_float(report.overall.alerts_per_day)
  end

  test "the same input and seed produce an identical report" do
    {series, labels} = input()
    assert Run.run(series, labels, 5) == Run.run(series, labels, 5)
  end

  test "the configuration digest ignores transport settings only" do
    {:ok, a} = Autoangel.Config.load("[http]\ningest_port = 1001\ncontrol_port = 1002\n", %{})
    {:ok, b} = Autoangel.Config.load("[http]\ningest_port = 2001\ncontrol_port = 2002\n", %{})
    {:ok, c} = Autoangel.Config.load("[persistence]\nn = 5\n", %{})
    assert Run.digest(a) == Run.digest(b)
    refute Run.digest(a) == Run.digest(c)
  end

  test "AutoangelReplay.replay/2 reads, injects every type and reports" do
    day = 86_400_000
    step = 300_000
    path = Path.join(System.tmp_dir!(), "replay-run-#{System.unique_integer([:positive])}.csv")

    rows =
      for i <- 0..(div(3 * day, step) - 1) do
        t = @base + i * step
        ~s("load{host=""a""}",#{t},#{100.0 + 20.0 * :math.sin(2 * :math.pi() * rem(t, day) / day)})
      end

    File.write!(path, Enum.join(["series,timestamp_ms,value" | rows], "\n"))
    on_exit(fn -> File.rm(path) end)

    assert {:ok, %{labels: labels, report: report}} = AutoangelReplay.replay([path], 9)

    assert Enum.sort(Enum.uniq(Enum.map(labels, & &1.type))) ==
             Enum.sort(AutoangelReplay.Inject.types())

    assert Map.keys(report.per_type) |> Enum.sort() == Enum.sort(AutoangelReplay.Inject.types())
    assert report.seed == 9
    assert {:ok, %{report: ^report}} = AutoangelReplay.replay([path], 9)
  end

  test "label values with quotes, backslashes and newlines are pushed intact" do
    s =
      Series.new(
        "odd",
        %{"path" => ~s(C:\\a "b"\nc)},
        for(i <- 0..20, do: {@base + i * @step, 1.0})
      )

    report = Run.run([s], [], 1)
    assert report.alerts == 0
    assert Series.display(s) == ~s(odd{path="C:\\\\a \\"b\\"\\nc"})
  end

  test "an unreadable input reports an error and no metrics" do
    assert {:error, {:unreadable, "/nonexistent.csv", :enoent}} =
             AutoangelReplay.replay(["/nonexistent.csv"], 1)
  end
end

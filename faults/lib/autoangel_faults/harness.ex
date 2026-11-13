defmodule AutoangelFaults.Harness do
  @moduledoc """
  Fault-injection harness (Task 29.1, Requirement 73.1).

  `run/2` replays one fixture through the in-process Engine on the simulated
  clock, with a real TimescaleDB behind Toxiproxy, and returns the alerts the
  capture plugin received plus what ended up in the database. A fault is a
  function the harness calls after each replayed timestamp (`:fault`).

  `compare/2` checks a fault run against a reference run of the same fixture and
  returns one `{name, pass?, detail}` per check. `report/1` renders them.
  """

  alias Autoangel.Storage.Writer
  alias AutoangelFaults.{Database, Toxiproxy}
  alias AutoangelReplay.{Run, Series}

  @step 300_000
  @base 1_700_000_000_000

  @type check :: {String.t(), boolean(), String.t()}

  @doc """
  The shared fixture: three Series of 5-minute Samples over about 2.4 days, with
  seeded noise and a four-Sample spike on two of them after the learning period.
  """
  @spec fixture() :: {[Series.t()], [map()]}
  def fixture do
    :rand.seed(:exsss, 11)

    series =
      for {host, spike?} <- [{"a", true}, {"b", true}, {"c", false}] do
        samples =
          for i <- 0..700 do
            v = 50.0 + :rand.uniform()
            {@base + i * @step, if(spike? and i in 500..503, do: v + 400.0, else: v)}
          end

        Series.new("fault_metric", %{"host" => host}, samples)
      end

    labels =
      for s <- series, s.labels["host"] != "c" do
        %{
          series_key: Series.key(s),
          type: :spike,
          start_ms: @base + 500 * @step,
          end_ms: @base + 500 * @step
        }
      end

    {series, labels}
  end

  @spec run({[Series.t()], [map()]}, keyword()) :: map()
  def run({series, labels}, opts \\ []) do
    database = Database.create()
    url = Database.engine_url(database, Toxiproxy.port())

    toml =
      """
      [storage]
      backend = "timescaledb"
      connection = "#{url}"
      """ <> "\n" <> Keyword.get(opts, :toml, "")

    pushed = Enum.sum(for s <- series, do: length(s.samples))

    result =
      Run.run(
        series,
        labels,
        11,
        config_toml: toml,
        plugins: Keyword.get(opts, :plugins, ["capture"]),
        include_alerts: true,
        keep_running: true,
        on_step: Keyword.get(opts, :fault)
      )

    try do
      drained? = drain(Keyword.get(opts, :drain_ms, 90_000))
      writer = Writer.stats(Writer)

      %{
        found: result.found |> Enum.sort(),
        report: Map.drop(result, [:found, :stop]),
        writer: writer,
        drained?: drained?,
        db: Database.counts(database),
        pushed: pushed
      }
    after
      result.stop.()
      Toxiproxy.reset()
    end
  end

  # Waits until the Storage_Writer has nothing queued.
  defp drain(timeout) when timeout <= 0, do: false

  defp drain(timeout) do
    case Writer.stats(Writer) do
      %{queued: 0} ->
        true

      _ ->
        Process.sleep(250)
        drain(timeout - 250)
    end
  end

  @doc "Alerts and stored Samples of a fault run against the reference run."
  @spec compare(map(), map()) :: [check()]
  def compare(reference, fault) do
    lost = fault.pushed - fault.db.samples

    [
      check(
        "reference run raised the injected alerts",
        reference.found != [],
        inspect(reference.found)
      ),
      check(
        "delivered alerts equal the reference run",
        fault.found == reference.found,
        "reference #{length(reference.found)}, fault #{length(fault.found)}"
      ),
      check("Storage_Writer queue drained", fault.drained?, "queued #{fault.writer.queued}"),
      check(
        "no Sample stored twice",
        fault.db.samples == fault.db.distinct_samples,
        "rows #{fault.db.samples}, distinct #{fault.db.distinct_samples}"
      ),
      check(
        "no Sample lost and none dropped",
        lost == 0 and fault.writer.dropped == 0,
        "pushed #{fault.pushed}, stored #{fault.db.samples}, dropped #{fault.writer.dropped}"
      )
    ]
  end

  @spec check(String.t(), boolean(), String.t()) :: check()
  def check(name, pass?, detail), do: {name, pass? == true, detail}

  @doc "One line per check."
  @spec report([check()]) :: String.t()
  def report(checks) do
    Enum.map_join(checks, "\n", fn {name, pass?, detail} ->
      "#{if pass?, do: "PASS", else: "FAIL"}  #{name} (#{detail})"
    end)
  end

  @doc "True when every check passed."
  @spec passed?([check()]) :: boolean()
  def passed?(checks), do: Enum.all?(checks, &elem(&1, 1))
end

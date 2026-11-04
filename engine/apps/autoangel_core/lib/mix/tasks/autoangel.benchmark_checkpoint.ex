defmodule Mix.Tasks.Autoangel.BenchmarkCheckpoint do
  @moduledoc "Benchmarks checkpoint write and restore for the StateStore."

  @shortdoc "Benchmark StateStore checkpoint write and restore"
  use Mix.Task

  @series_count 20_000
  @state_size 4_500

  @impl Mix.Task
  def run(_args) do
    Mix.Task.run("app.start", [])
    path = Path.join(System.tmp_dir!(), "autoangel-benchmark-#{System.unique_integer([:positive])}")
    {:ok, store} = Autoangel.StateStore.start_link(path: path)

    entries =
      for id <- 1..@series_count,
          do: {id, %{state: :binary.copy(<<0>>, @state_size), status: :learning}}

    {write_us, :ok} = :timer.tc(fn -> Autoangel.StateStore.checkpoint(store, entries) end)
    {restore_us, {:ok, checkpoint}} = :timer.tc(fn -> Autoangel.StateStore.restore(store) end)
    {replace_us, :ok} = :timer.tc(fn -> Autoangel.StateStore.checkpoint(store, entries) end)
    size = directory_size(path)

    line =
      "- CubDB checkpoint (#{@series_count} Series, #{@state_size} byte state): write #{write_us / 1_000} ms, restore #{restore_us / 1_000} ms, replacement #{replace_us / 1_000} ms, on-disk size #{size} bytes (restored #{length(checkpoint.entries)} entries)."

    decisions = Path.expand("../../../../../../docs/decisions.md", __DIR__)
    document = File.read!(decisions)
    marker = "## Storage gate"
    updated = String.replace(document, marker, "#{marker}\n\n#{line}", global: false)
    File.write!(decisions, updated)
    IO.puts(line)
    GenServer.stop(store)
    File.rm_rf!(path)
  end

  defp directory_size(path) do
    path
    |> Path.join("**/*")
    |> Path.wildcard()
    |> Enum.filter(&File.regular?/1)
    |> Enum.map(fn file -> File.stat!(file).size end)
    |> Enum.sum()
  end
end

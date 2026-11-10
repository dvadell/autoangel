defmodule AutoangelReplay do
  @moduledoc """
  Replay_Harness entry point (Requirement 77).

      {:ok, %{labels: labels, report: report}} = AutoangelReplay.replay(paths, seed)

  Reads the recorded Series, injects every anomaly type with the given seed,
  replays them through an in-process Engine on a simulated clock and reports
  recall, precision and alerts per simulated day, overall and per type, with
  the configuration digest and the seed.
  """

  alias AutoangelReplay.{Inject, Reader, Run, Series}

  @spec replay([Path.t()] | [Series.t()], integer(), keyword()) ::
          {:ok, %{labels: [Inject.label()], report: map()}} | {:error, term()}
  def replay(input, seed, opts \\ []) do
    with {:ok, series} <- load(input),
         {:ok, {injected, labels}} <- Inject.run(series, seed, opts) do
      {:ok, %{labels: labels, report: Run.run(injected, labels, seed)}}
    end
  end

  defp load([%Series{} | _] = series), do: {:ok, series}
  defp load(paths) when is_list(paths), do: Reader.read_all(paths)
end

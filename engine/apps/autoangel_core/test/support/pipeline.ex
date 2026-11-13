defmodule Autoangel.Test.Pipeline do
  @moduledoc """
  Drives one Series through the real kernel and `Decision`, one Sample at a time,
  the way a Worker does, and collects the events.
  """

  alias Autoangel.Detect.Decision
  alias Autoangel.Detect.Decision.Meta
  alias Autoangel.Kernel.{Params, State}
  alias Autoangel.Kernel.Scoring.Elixir, as: Kernel

  @default_cfg %{
    learning: %{duration: 1, cycles: 2},
    persistence: %{n: 3, m: 5},
    missing_data: %{multiple: 3.0}
  }

  @type run :: %{
          events: [{non_neg_integer(), term()}],
          meta: Meta.t(),
          results: [map()],
          state: binary()
        }

  @doc """
  Runs `samples` (`{ts_ms, value}` or `{ts_ms, value, kind}`) and returns the
  events as `{sample_index, event}`, the final `Meta`, every kernel result and
  the final encoded kernel state. Options: `:cfg`, `:params`, `:meta`, `:state`.
  """
  @spec run([tuple()], keyword()) :: run()
  def run(samples, opts \\ []) do
    cfg = Map.merge(@default_cfg, Keyword.get(opts, :cfg, %{}))
    params = Keyword.get(opts, :params, %Params{})
    meta = Keyword.get(opts, :meta, %Meta{key: {"pipeline", []}, kind: :gauge})
    state = Keyword.get(opts, :state, State.encode(State.new(kind: :gauge)))

    {{meta, state}, {events, results}} =
      samples
      |> Enum.with_index()
      |> Enum.reduce({{meta, state}, {[], []}}, fn {sample, index}, {{meta, state}, {ev, res}} ->
        {ts, value, kind} = normalize(sample)
        {:ok, state, [result]} = Kernel.score(state, [{ts, value, kind}], params)
        result = Map.merge(result, %{kind: kind, recv_ms: ts})
        {meta, events} = Decision.step(meta, result, ts, cfg)
        {{meta, state}, {ev ++ Enum.map(events, &{index, &1}), [result | res]}}
      end)

    %{events: events, meta: meta, results: Enum.reverse(results), state: state}
  end

  defp normalize({ts, value}), do: {ts, value, :gauge}
  defp normalize({_ts, _value, _kind} = sample), do: sample

  @doc "Events of one kind (`:anomaly_started` and so on) whose Anomaly has `kind`."
  @spec anomalies(run(), atom(), atom()) :: [map()]
  def anomalies(%{events: events}, event, kind) do
    for {_i, {^event, %{kind: ^kind} = anomaly}} <- events, do: anomaly
  end

  @doc "A deterministic noise stream in `[-amplitude, amplitude]`."
  @spec noise(integer(), non_neg_integer(), number()) :: [float()]
  def noise(seed, count, amplitude) do
    :rand.seed(:exsss, {seed, seed + 1, seed + 2})
    for _ <- 1..count//1, do: (:rand.uniform() * 2 - 1) * amplitude
  end
end

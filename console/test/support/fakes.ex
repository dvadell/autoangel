defmodule Console.Test.FakeData do
  @moduledoc """
  In-memory `Console.Data`. A test stores its fixtures with `put/1`; setting
  `:fail` to true makes every call answer `{:error, :database}`. State lives in
  the application env, so it is shared with the LiveView processes under test.
  """
  @behaviour Console.Data

  def put(fixtures), do: Application.put_env(:autoangel_console, __MODULE__, Map.new(fixtures))

  defp get(key, default),
    do: Map.get(Application.get_env(:autoangel_console, __MODULE__, %{}), key, default)

  defp answer(fun), do: if(get(:fail, false), do: {:error, :database}, else: {:ok, fun.()})

  @impl true
  def list_anomalies(now, recent),
    do: answer(fn -> Console.AnomalyList.select(get(:anomalies, []), now, recent) end)

  @impl true
  def candidates(ids),
    do:
      answer(fn ->
        get(:candidates, [])
        |> Enum.filter(&(&1.anomaly_id in ids))
        |> Enum.group_by(& &1.anomaly_id)
      end)

  @impl true
  def list_series(term),
    do: answer(fn -> Enum.filter(get(:series, []), &Console.SeriesSearch.matches?(&1, term)) end)

  @impl true
  def series(ids), do: answer(fn -> Enum.filter(get(:series, []), &(&1.id in ids)) end)

  @impl true
  def anomaly(id), do: answer(fn -> Enum.find(get(:anomalies, []), &(&1.id == id)) end)

  @impl true
  def chart(_series, from, to, now) do
    answer(fn ->
      chart = get(:chart, %{tier: :raw, points: [], bands: []})
      Map.put(chart, :markers, Console.Chart.markers(get(:anomalies, []), from, to, now))
    end)
  end

  @impl true
  def latest_heartbeat, do: answer(fn -> get(:heartbeat, nil) end)
end

defmodule Console.Test.FakeEngine do
  @moduledoc "A fake Control_API client that records each feedback request it receives."
  @behaviour Console.EngineClient

  def put(opts), do: Application.put_env(:autoangel_console, __MODULE__, Map.new(opts))

  defp get(key, default),
    do: Map.get(Application.get_env(:autoangel_console, __MODULE__, %{}), key, default)

  @impl true
  def ready?, do: get(:ready, true)

  @impl true
  def feedback(action, target) do
    if pid = get(:test_pid, nil), do: send(pid, {:feedback, action, target})
    get(:feedback_result, :ok)
  end
end

defmodule Console.Live.EngineStatus do
  @moduledoc """
  `on_mount` hook for every page. It reads the latest Heartbeat and the Engine's
  `/readyz` on mount and every 10 s, sets `@status`, and handles the shared
  `"feedback"` event (one Control_API request per press, aimed at the page's
  `@feedback_target`). Database or Engine failures never stop the page.
  """
  import Phoenix.Component, only: [assign: 3]
  import Phoenix.LiveView

  alias Console.{Banner, Data, EngineClient}

  @tick_ms 10_000

  def on_mount(:default, _params, _session, socket) do
    socket =
      socket
      |> assign(:status, status(connected?(socket)))
      |> assign(:feedback_result, nil)
      |> assign(:feedback_target, nil)
      |> attach_hook(:engine_status_tick, :handle_info, &tick/2)
      |> attach_hook(:feedback, :handle_event, &feedback/3)

    if connected?(socket), do: Process.send_after(self(), :console_tick, @tick_ms)
    {:cont, socket}
  end

  defp tick(:console_tick, socket) do
    Process.send_after(self(), :console_tick, @tick_ms)
    {:halt, assign(socket, :status, status(true))}
  end

  defp tick(_message, socket), do: {:cont, socket}

  defp feedback("feedback", %{"action" => label}, socket) do
    result =
      with {:ok, action} <- EngineClient.api_action(label),
           %{} = target <- socket.assigns.feedback_target,
           true <- socket.assigns.status.engine_ready || :not_ready,
           :ok <- EngineClient.impl().feedback(action, target) do
        {:ok, label}
      else
        :error -> {:error, "unknown action"}
        nil -> {:error, "nothing to apply it to"}
        :not_ready -> {:error, "the Engine is not ready"}
        {:error, reason} -> {:error, reason}
      end

    {:halt, assign(socket, :feedback_result, result)}
  end

  defp feedback(_event, _params, socket), do: {:cont, socket}

  @doc false
  def status(check_engine?) do
    now = DateTime.utc_now()
    threshold = Application.get_env(:autoangel_console, :engine_not_reporting_seconds, 60)

    heartbeat =
      case Data.impl().latest_heartbeat() do
        {:ok, ts} -> ts
        {:error, :database} -> nil
      end

    %{
      heartbeat: heartbeat,
      not_reporting: Banner.not_reporting?(heartbeat, now, threshold),
      engine_ready: check_engine? and EngineClient.impl().ready?()
    }
  end
end

defmodule Console.AnomalyListLive do
  @moduledoc "`/`: open Anomalies and those ended within the recent period, with stored candidates."
  use Phoenix.LiveView

  import Console.Components

  alias Console.Data

  @impl true
  def mount(_params, _session, socket) do
    now = DateTime.utc_now()
    recent = Application.get_env(:autoangel_console, :recent_seconds, 86_400)

    loaded =
      with {:ok, anomalies} <- Data.impl().list_anomalies(now, recent),
           {:ok, candidates} <- Data.impl().candidates(Enum.map(anomalies, & &1.id)) do
        {:ok, anomalies, candidates}
      end

    socket =
      case loaded do
        {:ok, anomalies, candidates} ->
          assign(socket, anomalies: anomalies, candidates: candidates, error: false)

        {:error, :database} ->
          assign(socket, anomalies: [], candidates: %{}, error: true)
      end

    {:ok, assign(socket, recent_hours: div(recent, 3600))}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <.page status={@status}>
      <h1>Anomalies</h1>
      <p :if={@error} class="error" role="alert" id="db-error">
        The database could not be read. Try again shortly.
      </p>
      <p :if={!@error and @anomalies == []} id="empty">
        No open Anomalies and none ended in the last {@recent_hours} hours.
      </p>
      <table :if={@anomalies != []} id="anomalies">
        <thead>
          <tr>
            <th scope="col">Series</th>
            <th scope="col">State</th>
            <th scope="col">Kind</th>
            <th scope="col">Start</th>
            <th scope="col">End</th>
            <th scope="col">anomaly score</th>
            <th scope="col">Observed</th>
            <th scope="col">Expected</th>
            <th scope="col">Baseline</th>
            <th scope="col">Candidates</th>
          </tr>
        </thead>
        <tbody>
          <tr :for={a <- @anomalies} id={"anomaly-#{a.id}"}>
            <td><a href={"/anomalies/#{a.id}"}>{series_label(a.series)}</a></td>
            <td>
              {if a.end_time, do: "ended", else: "open"}
              <span :if={a.learning}>(learning)</span>
            </td>
            <td>{a.kind}<span :if={a.classification}> ({a.classification})</span></td>
            <td>{format_time(a.start_time)}</td>
            <td>{format_time(a.end_time)}</td>
            <td>{number(a.score)}</td>
            <td>{number(a.observed)}</td>
            <td>{number(a.expected_lower)} to {number(a.expected_upper)}</td>
            <td>{a.baseline_type || "—"}</td>
            <td><.candidates list={Map.get(@candidates, a.id, [])} /></td>
          </tr>
        </tbody>
      </table>
    </.page>
    """
  end

  attr(:list, :list, required: true)

  def candidates(assigns) do
    ~H"""
    <span :if={@list == []} class="no-candidates">No correlated candidates were stored.</span>
    <ol :if={@list != []}>
      <li :for={c <- Enum.sort_by(@list, & &1.rank)}>
        {series_label(c.series)}
        <span :if={c.weak} class="weak">(weak evidence)</span>
      </li>
    </ol>
    """
  end
end

defmodule Console.SeriesLive.Index do
  @moduledoc "`/series`: every Series with its stored status; case-sensitive substring search."
  use Phoenix.LiveView

  import Console.Components

  @impl true
  def mount(params, _session, socket), do: {:ok, load(socket, Map.get(params, "q", ""))}

  @impl true
  def handle_event("search", %{"q" => term}, socket), do: {:noreply, load(socket, term)}

  defp load(socket, term) do
    case Console.Data.impl().list_series(term) do
      {:ok, series} -> assign(socket, term: term, series: series, error: false)
      {:error, :database} -> assign(socket, term: term, series: [], error: true)
    end
  end

  @impl true
  def render(assigns) do
    ~H"""
    <.page status={@status}>
      <h1>Series</h1>
      <form phx-change="search" phx-submit="search" id="search">
        <label for="q">Search name, label name or label value (case-sensitive)</label>
        <input type="search" id="q" name="q" value={@term} phx-debounce="300" autocomplete="off" />
      </form>
      <p :if={@error} class="error" role="alert" id="db-error">
        The database could not be read. Your search “{@term}” is kept; try again shortly.
      </p>
      <p :if={!@error and @series == [] and @term != ""} id="no-match">
        No Series match “{@term}”.
      </p>
      <p :if={!@error and @series == [] and @term == ""} id="empty">No Series are stored yet.</p>
      <table :if={@series != []} id="series">
        <thead>
          <tr><th scope="col">Series</th><th scope="col">Kind</th><th scope="col">Status</th></tr>
        </thead>
        <tbody>
          <tr :for={s <- @series} id={"series-#{s.id}"}>
            <td><a href={"/series/#{s.id}"}>{series_label(s)}</a></td>
            <td>{s.kind}</td>
            <td>{s.status}</td>
          </tr>
        </tbody>
      </table>
    </.page>
    """
  end
end

defmodule Console.SeriesLive.Show do
  @moduledoc "`/series/:id`: chart over a chosen range with band and Anomaly markers, plus feedback."
  use Phoenix.LiveView

  import Console.Components

  alias Console.{Chart, Data, EngineClient}

  @impl true
  def mount(%{"id" => id}, _session, socket) do
    socket =
      case Ecto.UUID.cast(id) do
        {:ok, uuid} -> lookup(socket, uuid)
        :error -> assign(socket, series: nil, error: nil)
      end

    {:ok, socket}
  end

  defp lookup(socket, id) do
    case Data.impl().series([id]) do
      {:ok, [series | _]} ->
        assign(socket,
          series: series,
          error: nil,
          feedback_target: EngineClient.series_target(series)
        )

      {:ok, []} ->
        assign(socket, series: nil, error: nil)

      {:error, :database} ->
        assign(socket, series: nil, error: :database)
    end
  end

  @impl true
  def handle_params(params, _uri, %{assigns: %{series: %{} = series}} = socket) do
    label = if List.keymember?(Chart.ranges(), params["range"], 0), do: params["range"], else: "24h"
    now = DateTime.utc_now()
    from = DateTime.add(now, -Chart.range_seconds(label), :second)

    socket = assign(socket, range: label, from: from, to: now)

    case Data.impl().chart(series, from, now, now, []) do
      {:ok, chart} -> {:noreply, assign(socket, chart: chart, chart_error: false)}
      {:error, :database} -> {:noreply, assign(socket, chart: nil, chart_error: true)}
    end
  end

  def handle_params(_params, _uri, socket), do: {:noreply, socket}

  @impl true
  def render(%{series: nil} = assigns) do
    ~H"""
    <.page status={@status}>
      <h1>Series</h1>
      <p :if={@error == :database} class="error" role="alert" id="db-error">
        The database could not be read. Try again shortly.
      </p>
      <p :if={@error != :database} id="not-found">This Series was not found.</p>
    </.page>
    """
  end

  def render(assigns) do
    ~H"""
    <.page status={@status}>
      <h1>{series_label(@series)}</h1>
      <p>Kind: {@series.kind}. Status: {@series.status}.</p>
      <.feedback_buttons status={@status} result={@feedback_result} />
      <nav aria-label="Range" id="ranges">
        <.link
          :for={{label, _} <- Console.Chart.ranges()}
          patch={"/series/#{@series.id}?range=#{label}"}
          aria-current={if label == @range, do: "true", else: "false"}
        >
          {label}
        </.link>
      </nav>
      <p :if={@chart_error} class="error" role="alert" id="db-error">
        The chart data could not be read. Try again shortly.
      </p>
      <p :if={@chart && @chart.points == []} id="no-data">No data in this range.</p>
      <.chart
        :if={@chart && @chart.points != []}
        chart={@chart}
        from={@from}
        to={@to}
        label={series_label(@series)}
      />
      <p :if={@series.kind == "counter"}>Counter values are shown as a per-second rate.</p>
    </.page>
    """
  end
end

defmodule Console.AnomalyLive.Show do
  @moduledoc "`/anomalies/:id`: every stored field, a chart around the Anomaly, candidates, feedback."
  use Phoenix.LiveView

  import Console.Components

  alias Console.{Data, EngineClient}

  @margin 3_600

  @impl true
  def mount(%{"id" => id}, _session, socket) do
    socket =
      case Data.impl().anomaly(id) do
        {:ok, nil} -> assign(socket, anomaly: nil, error: :not_found)
        {:ok, anomaly} -> loaded(socket, anomaly)
        {:error, :database} -> assign(socket, anomaly: nil, error: :database)
      end

    {:ok, socket}
  end

  defp loaded(socket, anomaly) do
    now = DateTime.utc_now()
    from = DateTime.add(anomaly.start_time, -@margin, :second)
    to = if anomaly.end_time, do: DateTime.add(anomaly.end_time, @margin, :second), else: now
    to = if DateTime.compare(to, now) == :gt, do: now, else: to

    chart =
      case anomaly.series && Data.impl().chart(anomaly.series, from, to, now, []) do
        {:ok, chart} -> chart
        _ -> nil
      end

    candidates =
      case Data.impl().candidates([anomaly.id]) do
        {:ok, map} -> {:ok, Map.get(map, anomaly.id, [])}
        {:error, :database} -> :error
      end

    target = if anomaly.series, do: EngineClient.series_target(anomaly.series)

    assign(socket,
      anomaly: anomaly,
      error: nil,
      chart: chart,
      from: from,
      to: to,
      candidates: candidates,
      feedback_target: target
    )
  end

  @impl true
  def render(%{anomaly: nil} = assigns) do
    ~H"""
    <.page status={@status}>
      <h1>Anomaly</h1>
      <p :if={@error == :database} class="error" role="alert" id="db-error">
        The database could not be reached. Try again shortly.
      </p>
      <p :if={@error == :not_found} id="not-found">This Anomaly was not found.</p>
    </.page>
    """
  end

  def render(assigns) do
    ~H"""
    <.page status={@status}>
      <h1>Anomaly on {series_label(@anomaly.series)}</h1>
      <dl id="fields">
        <dt>State</dt><dd>{if @anomaly.end_time, do: "ended", else: "open"}</dd>
        <dt>Kind</dt><dd>{@anomaly.kind}</dd>
        <dt>Classification</dt><dd>{@anomaly.classification || "—"}</dd>
        <dt>Start</dt><dd>{format_time(@anomaly.start_time)}</dd>
        <dt>End</dt><dd>{format_time(@anomaly.end_time)}</dd>
        <dt>anomaly score</dt><dd>{number(@anomaly.score)}</dd>
        <dt>Observed</dt><dd>{number(@anomaly.observed)}</dd>
        <dt>Expected</dt>
        <dd>{number(@anomaly.expected_lower)} to {number(@anomaly.expected_upper)}</dd>
        <dt>Baseline</dt><dd>{@anomaly.baseline_type || "—"}</dd>
        <dt>Learning</dt><dd>{if @anomaly.learning, do: "yes", else: "no"}</dd>
        <dt>Stored status</dt><dd>{@anomaly.status}</dd>
      </dl>
      <.feedback_buttons status={@status} result={@feedback_result} />
      <h2>Chart</h2>
      <p :if={is_nil(@chart)} class="error" role="alert">The chart data could not be read.</p>
      <p :if={@chart && @chart.points == []} id="no-data">No data around this Anomaly.</p>
      <.chart
        :if={@chart && @chart.points != []}
        chart={@chart}
        from={@from}
        to={@to}
        label={series_label(@anomaly.series)}
      />
      <h2>Correlated candidates</h2>
      <p :if={@candidates == :error} class="error" role="alert">
        The candidates could not be read.
      </p>
      <Console.AnomalyListLive.candidates :if={@candidates != :error} list={elem(@candidates, 1)} />
    </.page>
    """
  end
end

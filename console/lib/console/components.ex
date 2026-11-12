defmodule Console.Layouts do
  @moduledoc "Root HTML layout. LiveView JavaScript is served from the phoenix deps, no build step."
  use Phoenix.Component

  def root(assigns) do
    ~H"""
    <!DOCTYPE html>
    <html lang="en">
      <head>
        <meta charset="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        <meta name="csrf-token" content={Phoenix.Controller.get_csrf_token()} />
        <title>Autoangel Console</title>
        <style>
          body { font-family: system-ui, sans-serif; margin: 0; color: #1a1a1a; background: #fff; }
          header, main { padding: 0.75rem 1.5rem; }
          header { border-bottom: 1px solid #ccc; display: flex; gap: 1.5rem; align-items: center; }
          a { color: #0b4f9c; }
          table { border-collapse: collapse; width: 100%; }
          th, td { text-align: left; padding: 0.3rem 0.5rem; border-bottom: 1px solid #ddd; vertical-align: top; }
          .banner { background: #fff3cd; color: #5c4400; border: 1px solid #c9a227; padding: 0.5rem 1.5rem; }
          .error { background: #fde2e1; color: #6b0f0b; border: 1px solid #c0392b; padding: 0.5rem; }
          .notice { background: #e2f0e3; color: #134d1a; border: 1px solid #2e7d32; padding: 0.5rem; }
          .weak { font-style: italic; color: #555; }
          button[disabled] { opacity: 0.5; cursor: not-allowed; }
          svg { max-width: 100%; height: auto; border: 1px solid #ddd; background: #fafafa; }
        </style>
        <script defer src="/assets/phoenix/phoenix.min.js"></script>
        <script defer src="/assets/live/phoenix_live_view.min.js"></script>
        <script>
          window.addEventListener("DOMContentLoaded", () => {
            const token = document.querySelector("meta[name='csrf-token']").getAttribute("content");
            const socket = new LiveView.LiveSocket("/live", Phoenix.Socket, {params: {_csrf_token: token}});
            socket.connect();
          });
        </script>
      </head>
      <body>
        {@inner_content}
      </body>
    </html>
    """
  end
end

defmodule Console.Components do
  @moduledoc "Shared view pieces: page frame with the Engine banner, chart, feedback buttons, formatting."
  use Phoenix.Component

  attr(:status, :map, required: true)
  slot(:inner_block, required: true)

  def page(assigns) do
    ~H"""
    <header>
      <strong>Autoangel Console</strong>
      <nav aria-label="Main">
        <a href="/">Anomalies</a> · <a href="/series">Series</a>
      </nav>
    </header>
    <div :if={@status.not_reporting} class="banner" role="status" id="engine-banner">
      Engine not reporting.
      <span :if={@status.heartbeat}>Last Heartbeat: {format_time(@status.heartbeat)}.</span>
      <span :if={!@status.heartbeat}>No Heartbeat recorded.</span>
      Stored data is still shown.
    </div>
    <main>
      {render_slot(@inner_block)}
    </main>
    """
  end

  attr(:status, :map, required: true)
  attr(:result, :any, default: nil)

  def feedback_buttons(assigns) do
    ~H"""
    <div class="feedback" id="feedback">
      <button
        type="button"
        phx-click="feedback"
        phx-value-action="mute"
        phx-disable-with="Sending…"
        disabled={!@status.engine_ready}
      >
        mute
      </button>
      <button
        type="button"
        phx-click="feedback"
        phx-value-action="this is normal"
        phx-disable-with="Sending…"
        disabled={!@status.engine_ready}
      >
        this is normal
      </button>
      <span :if={!@status.engine_ready}>Feedback is unavailable: the Engine is not ready.</span>
      <p :if={match?({:ok, _}, @result)} class="notice" role="status">
        Applied: {elem(@result, 1)}.
      </p>
      <p :if={match?({:error, _}, @result)} class="error" role="alert">
        Not applied: {elem(@result, 1)}.
      </p>
    </div>
    """
  end

  attr(:chart, :map, required: true)
  attr(:from, :any, required: true)
  attr(:to, :any, required: true)
  attr(:label, :string, required: true)

  @width 800
  @height 240

  def chart(assigns) do
    assigns = assign(assigns, :geometry, geometry(assigns.chart, assigns.from, assigns.to))

    ~H"""
    <svg
      viewBox={"0 0 #{@geometry.width} #{@geometry.height}"}
      role="img"
      aria-label={"#{@label}: #{length(@chart.points)} points, #{@chart.tier} tier, #{length(@chart.markers)} anomalies"}
      id="chart"
      data-tier={@chart.tier}
    >
      <rect
        :for={m <- @geometry.markers}
        x={m.x}
        y="0"
        width={m.w}
        height={@geometry.height}
        fill="#f6c3c0"
        class="marker"
      />
      <polygon :if={@geometry.band != ""} points={@geometry.band} fill="#cfe3f7" class="band" />
      <polyline points={@geometry.line} fill="none" stroke="#0b4f9c" stroke-width="1.5" />
    </svg>
    <p>Range {format_time(@from)} to {format_time(@to)}, {@chart.tier} data.</p>
    """
  end

  defp geometry(chart, from, to) do
    t0 = DateTime.to_unix(from, :millisecond)
    span = max(DateTime.to_unix(to, :millisecond) - t0, 1)

    values =
      Enum.map(chart.points, & &1.value) ++
        Enum.flat_map(chart.bands, &[&1.lower_bound, &1.upper_bound])

    values = Enum.filter(values, &is_number/1)
    {lo, hi} = if values == [], do: {0.0, 1.0}, else: Enum.min_max(values)
    hi = if hi == lo, do: lo + 1.0, else: hi

    x = fn ts -> Float.round((DateTime.to_unix(ts, :millisecond) - t0) / span * @width, 1) end
    y = fn v -> Float.round(@height - (v - lo) / (hi - lo) * (@height - 10) - 5, 1) end

    line =
      for %{value: v, ts: ts} <- chart.points, is_number(v), do: "#{x.(ts)},#{y.(v)}"

    bands = Enum.filter(chart.bands, &(is_number(&1.lower_bound) and is_number(&1.upper_bound)))

    band =
      Enum.map(bands, &"#{x.(&1.ts)},#{y.(&1.upper_bound)}") ++
        Enum.map(Enum.reverse(bands), &"#{x.(&1.ts)},#{y.(&1.lower_bound)}")

    markers =
      for m <- chart.markers do
        x1 = max(x.(m.start_time), 0.0)
        x2 = min(x.(m.end_time || to), @width * 1.0)
        %{x: x1, w: max(x2 - x1, 2.0)}
      end

    %{
      width: @width,
      height: @height,
      line: Enum.join(line, " "),
      band: Enum.join(band, " "),
      markers: markers
    }
  end

  @doc "UTC time as `YYYY-MM-DD HH:MM:SS UTC`."
  def format_time(nil), do: "—"

  def format_time(%DateTime{} = t),
    do: t |> DateTime.truncate(:second) |> Calendar.strftime("%Y-%m-%d %H:%M:%S UTC")

  @doc "A Series as `name{k=\"v\",...}`."
  def series_label(nil), do: "(unknown Series)"

  def series_label(%{name: name, labels: labels}) do
    case Enum.sort(labels || %{}) do
      [] -> name
      pairs -> name <> "{" <> Enum.map_join(pairs, ",", fn {k, v} -> "#{k}=\"#{v}\"" end) <> "}"
    end
  end

  def number(nil), do: "—"
  def number(v) when is_float(v), do: :erlang.float_to_binary(v, [:compact, decimals: 4])
  def number(v), do: to_string(v)
end

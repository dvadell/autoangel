defmodule Console.LiveTest do
  use ExUnit.Case, async: false

  import Phoenix.ConnTest
  import Phoenix.LiveViewTest

  alias Console.Test.{FakeData, FakeEngine}

  @endpoint Console.Endpoint

  @series %{
    id: "11111111-1111-1111-1111-111111111111",
    name: "http_requests_total",
    labels: %{"job" => "api"},
    kind: "counter",
    status: "anomalous"
  }
  @other %{
    id: "22222222-2222-2222-2222-222222222222",
    name: "cpu_load",
    labels: %{"host" => "Web1"},
    kind: "gauge",
    status: "healthy"
  }

  defp anomaly(overrides) do
    Map.merge(
      %{
        id: "aaaaaaaa-0000-0000-0000-000000000001",
        series_id: @series.id,
        series: @series,
        start_time: DateTime.add(DateTime.utc_now(), -600, :second),
        end_time: nil,
        score: 91.5,
        observed: 120.0,
        expected_lower: 10.0,
        expected_upper: 20.0,
        baseline_type: "rolling",
        status: "open",
        kind: "value",
        classification: nil,
        learning: false
      },
      Map.new(overrides)
    )
  end

  setup do
    Application.put_env(:autoangel_console, :auth, [{:bearer, "tok"}])
    FakeData.put(series: [@series, @other], heartbeat: DateTime.utc_now())
    FakeEngine.put(ready: true, test_pid: self())

    conn =
      build_conn()
      |> Plug.Conn.put_req_header("authorization", "Bearer tok")

    {:ok, conn: conn}
  end

  describe "authentication" do
    test "a page without credentials returns 401 and no data" do
      FakeData.put(series: [@series], anomalies: [anomaly([])])
      conn = get(build_conn(), "/")
      assert conn.status == 401
      assert conn.resp_body == ""
    end

    test "a LiveView page without credentials is refused before any socket opens" do
      conn = get(build_conn(), "/series")
      assert conn.status == 401
      assert conn.resp_body == ""
    end
  end

  describe "AnomalyListLive" do
    test "lists open Anomalies with the score labelled and candidates by rank", %{conn: conn} do
      a = anomaly([])

      FakeData.put(
        series: [@series, @other],
        anomalies: [a],
        heartbeat: DateTime.utc_now(),
        candidates: [
          %{anomaly_id: a.id, rank: 2, weak: true, series: @series, series_id: @series.id},
          %{anomaly_id: a.id, rank: 1, weak: false, series: @other, series_id: @other.id}
        ]
      )

      {:ok, view, html} = live(conn, "/")
      assert html =~ "anomaly score"
      assert html =~ "91.5"
      assert has_element?(view, "#anomaly-#{a.id}", "open")
      assert html =~ "(weak evidence)"

      [first | _] =
        Regex.scan(
          ~r/cpu_load|http_requests_total\{job/,
          html |> String.split("<ol>") |> List.last()
        )

      assert first == ["cpu_load"]
    end

    test "empty state", %{conn: conn} do
      {:ok, view, _} = live(conn, "/")
      assert has_element?(view, "#empty")
    end

    test "no-candidates state", %{conn: conn} do
      FakeData.put(series: [@series], anomalies: [anomaly([])], heartbeat: DateTime.utc_now())
      {:ok, _view, html} = live(conn, "/")
      assert html =~ "No correlated candidates were stored."
    end

    test "database-error state", %{conn: conn} do
      FakeData.put(fail: true)
      {:ok, view, _} = live(conn, "/")
      assert has_element?(view, "#db-error")
    end
  end

  describe "SeriesLive.Index" do
    test "lists all Series and searches case-sensitively", %{conn: conn} do
      {:ok, view, html} = live(conn, "/series")
      assert html =~ "cpu_load"
      assert html =~ "http_requests_total"

      html = view |> element("#search") |> render_change(%{"q" => "Web"})
      assert html =~ "cpu_load"
      refute html =~ "http_requests_total"

      html = view |> element("#search") |> render_change(%{"q" => "web"})
      assert html =~ "No Series match “web”"
    end

    test "database error keeps the search term", %{conn: conn} do
      {:ok, view, _} = live(conn, "/series")
      FakeData.put(fail: true)
      html = view |> element("#search") |> render_change(%{"q" => "cpu"})
      assert html =~ "Your search “cpu” is kept"
      assert html =~ ~s(value="cpu")
    end
  end

  describe "SeriesLive.Show" do
    test "chart with tier, band and markers", %{conn: conn} do
      now = DateTime.utc_now()

      FakeData.put(
        series: [@series],
        heartbeat: now,
        anomalies: [anomaly([])],
        chart: %{
          tier: :raw,
          points: [
            %{ts: DateTime.add(now, -3_000, :second), value: 1.0},
            %{ts: DateTime.add(now, -60, :second), value: 5.0}
          ],
          bands: [
            %{ts: DateTime.add(now, -3_000, :second), lower_bound: 0.5, upper_bound: 2.0},
            %{ts: DateTime.add(now, -60, :second), lower_bound: 0.5, upper_bound: 2.0}
          ]
        }
      )

      {:ok, view, _} = live(conn, "/series/#{@series.id}?range=1h")
      assert has_element?(view, "#chart[data-tier=raw]")
      assert has_element?(view, "#chart .band")
      assert has_element?(view, "#chart .marker")
      assert has_element?(view, ~s(#ranges a[aria-current=true]), "1h")
    end

    test "no-data, not-found and database-error states", %{conn: conn} do
      {:ok, view, _} = live(conn, "/series/#{@series.id}")
      assert has_element?(view, "#no-data")

      {:ok, view, _} = live(conn, "/series/33333333-3333-3333-3333-333333333333")
      assert has_element?(view, "#not-found")

      {:ok, view, _} = live(conn, "/series/not-a-uuid")
      assert has_element?(view, "#not-found")

      FakeData.put(fail: true)
      {:ok, view, _} = live(conn, "/series/#{@series.id}")
      assert has_element?(view, "#db-error")
    end
  end

  describe "AnomalyLive.Show" do
    test "shows every field and candidates", %{conn: conn} do
      a = anomaly(classification: "spike")
      FakeData.put(series: [@series], anomalies: [a], heartbeat: DateTime.utc_now())
      {:ok, _view, html} = live(conn, "/anomalies/#{a.id}")

      for text <- [
            "anomaly score",
            "91.5",
            "120.0",
            "10.0",
            "20.0",
            "rolling",
            "spike",
            "No correlated candidates were stored."
          ] do
        assert html =~ text
      end
    end

    test "not-found and database-unreachable pages", %{conn: conn} do
      {:ok, view, _} = live(conn, "/anomalies/aaaaaaaa-0000-0000-0000-000000000099")
      assert has_element?(view, "#not-found")

      FakeData.put(fail: true)
      {:ok, view, _} = live(conn, "/anomalies/aaaaaaaa-0000-0000-0000-000000000001")
      assert has_element?(view, "#db-error")
    end
  end

  describe "Engine-not-reporting banner" do
    test "hidden for a fresh Heartbeat", %{conn: conn} do
      {:ok, view, _} = live(conn, "/series")
      refute has_element?(view, "#engine-banner")
    end

    test "shows the last Heartbeat time when it is old, and data is still served", %{conn: conn} do
      old = DateTime.add(DateTime.utc_now(), -3_600, :second)
      FakeData.put(series: [@series], heartbeat: old)
      {:ok, view, html} = live(conn, "/series")
      assert has_element?(view, "#engine-banner", "Last Heartbeat")
      assert html =~ "http_requests_total"
    end

    test "says no Heartbeat recorded when there is none", %{conn: conn} do
      FakeData.put(series: [], heartbeat: nil)
      {:ok, view, _} = live(conn, "/series")
      assert has_element?(view, "#engine-banner", "No Heartbeat recorded.")
    end

    test "re-reads on every tick", %{conn: conn} do
      {:ok, view, _} = live(conn, "/series")
      refute has_element?(view, "#engine-banner")
      FakeData.put(series: [], heartbeat: nil)
      send(view.pid, :console_tick)
      assert has_element?(view, "#engine-banner")
    end
  end

  describe "feedback" do
    test "one Control_API request per press, aimed at the Series, with confirmation", %{conn: conn} do
      {:ok, view, _} = live(conn, "/series/#{@series.id}")

      html = view |> element(~s(button[phx-value-action="mute"])) |> render_click()

      assert_received {:feedback, "mute",
                       %{
                         "series" => %{
                           "name" => "http_requests_total",
                           "labels" => %{"job" => "api"}
                         }
                       }}

      refute_received {:feedback, _, _}
      assert html =~ "Applied: mute."

      view |> element(~s(button[phx-value-action="this is normal"])) |> render_click()
      assert_received {:feedback, "this was normal", _}
    end

    test "a refused request shows not applied", %{conn: conn} do
      FakeEngine.put(
        ready: true,
        test_pid: self(),
        feedback_result: {:error, "the Engine refused the target"}
      )

      a = anomaly([])
      FakeData.put(series: [@series], anomalies: [a], heartbeat: DateTime.utc_now())
      {:ok, view, _} = live(conn, "/anomalies/#{a.id}")
      html = view |> element(~s(button[phx-value-action="mute"])) |> render_click()
      assert html =~ "Not applied: the Engine refused the target."
    end

    test "buttons are disabled while readiness fails and come back after recovery", %{conn: conn} do
      FakeEngine.put(ready: false, test_pid: self())
      {:ok, view, _} = live(conn, "/series/#{@series.id}")
      assert has_element?(view, ~s(button[phx-value-action="mute"][disabled]))

      FakeEngine.put(ready: true, test_pid: self())
      send(view.pid, :console_tick)
      refute has_element?(view, ~s(button[phx-value-action="mute"][disabled]))
    end

    test "a press while not ready sends nothing", %{conn: conn} do
      FakeEngine.put(ready: false, test_pid: self())
      {:ok, view, _} = live(conn, "/series/#{@series.id}")
      render_hook(view, "feedback", %{"action" => "mute"})
      refute_received {:feedback, _, _}
      assert render(view) =~ "Not applied: the Engine is not ready."
    end
  end
end

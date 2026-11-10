defmodule Autoangel.Alert.RenderTest do
  use ExUnit.Case, async: true

  alias Autoangel.Alert.Policy
  alias Autoangel.Alert.Render

  @anomaly %{
    id: "abc123",
    series_key: {"cpu_usage", [{"host", "h1"}, {"job", "web"}]},
    start_ms: 1_700_000_000_000,
    score: 87.46,
    observed: 12.5,
    lower: 1.0,
    upper: 2.0,
    baseline_type: :short_term
  }

  defp cand(rank, name, weak? \\ false),
    do: %{series_key: {name, [{"host", "h1"}]}, rank: rank, weak?: weak?, evidence: %{}}

  test "builds every required field" do
    alert = Render.build(@anomaly, [], [])
    assert alert.name == "cpu_usage"
    assert alert.labels == %{"host" => "h1", "job" => "web"}
    assert alert.observed == 12.5
    assert alert.range == %{lower: 1.0, upper: 2.0}
    assert alert.start_time_iso == "2023-11-14T22:13:20Z"
    assert alert.anomaly_id == "abc123"
  end

  test "text carries all fields, labels the score without a percent sign" do
    text = Render.build(@anomaly, [], []) |> Render.text()
    assert text =~ "metric: cpu_usage"
    assert text =~ "host=h1, job=web"
    assert text =~ "observed value: 12.5"
    assert text =~ "expected range lower: 1.0"
    assert text =~ "expected range upper: 2.0"
    assert text =~ "baseline type: short_term"
    assert text =~ "anomaly score: 87.5"
    assert text =~ "start: 2023-11-14T22:13:20Z"
    refute text =~ "%"
  end

  test "empty candidate list yields the no-candidates statement" do
    text = Render.build(@anomaly, [], []) |> Render.text()
    assert text =~ "no candidates found inside the Correlation_Window"
    refute text =~ "candidates:"
  end

  test "keeps the lowest ranks up to the maximum, ascending, with weak markers" do
    cands = [cand(3, "c"), cand(1, "a", true), cand(2, "b"), cand(4, "d")]
    alert = Render.build(@anomaly, cands, max_candidates: 2)
    assert Enum.map(alert.candidates, & &1.rank) == [1, 2]
    assert alert.candidate_count == 4

    text = Render.text(alert)
    assert text =~ "candidates:\n1. a [host=h1] (weak evidence)\n2. b [host=h1]"
    refute text =~ "3. c"
  end

  test "Console link only when configured" do
    refute Render.build(@anomaly, [], []).console_url
    refute Render.build(@anomaly, [], console_base_url: nil).console_url

    alert = Render.build(@anomaly, [], console_base_url: "https://c.example/")
    assert alert.console_url == "https://c.example/anomalies/abc123"
    assert Render.text(alert) =~ "console: https://c.example/anomalies/abc123"
  end

  test "score formatting is an integer or one decimal, clamped to 0-100" do
    assert Render.score(88) == "88"
    assert Render.score(88.0) == "88"
    assert Render.score(87.46) == "87.5"
    assert Render.score(130) == "100"
    assert Render.score(-4) == "0"
    assert Render.score(nil) == nil
  end

  test "self-monitoring alerts render their own message" do
    msg = "Autoangel Engine self-monitoring alert: this alert concerns the Engine itself"
    assert Render.text(%{message: msg}) == msg
  end

  test "Policy builds the delivered alert through Render with configured options" do
    policy =
      Policy.new(
        plugins: [:p],
        render: [console_base_url: "https://c.example", max_candidates: 1]
      )

    cands = [cand(2, "b"), cand(1, "a")]

    {_policy, actions} =
      Policy.on_event(
        policy,
        {:anomaly_started, @anomaly, cands},
        1_700_000_000_000
      )

    assert [{:deliver, :p, alert} | _] = Enum.filter(actions, &match?({:deliver, _, _}, &1))
    assert alert.console_url == "https://c.example/anomalies/abc123"
    assert [%{rank: 1}] = alert.candidates
  end

  test "missing values are stated explicitly rather than dropped" do
    text = Render.build(%{@anomaly | observed: nil, baseline_type: nil}, [], []) |> Render.text()
    assert text =~ "observed value: none"
    assert text =~ "baseline type: none"
  end

  test "an out of range timestamp does not crash rendering" do
    assert %{start_time_iso: nil} = Render.build(%{@anomaly | start_ms: 10 ** 30}, [], [])
  end
end

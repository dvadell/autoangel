defmodule Autoangel.Alert.BannedWordsTest do
  @moduledoc """
  Task 20.3 (Req 17.7, 19.7): rendered alert templates never use causal or
  probabilistic wording or a percent sign.

  Two checks: every string literal in the alert-rendering sources, and the text
  actually produced for representative alerts.
  """
  use ExUnit.Case, async: true

  alias Autoangel.Alert.Render

  @banned ~r/\b(caus(e[ds]?|ing|al\w*)|root cause|probab\w*|likel\w*|chance\w*|percent)\b|%/i

  @root Path.expand("../../../../..", __DIR__)
  # Add every new alert-rendering module (resolution or Incident notifications,
  # Control_API alert text) here so its templates are checked too.
  @sources [
    "apps/autoangel_core/lib/autoangel/alert/render.ex",
    "apps/autoangel_core/lib/autoangel/self_monitor.ex",
    "apps/engine_plugin_email/lib/autoangel/plugin/email.ex",
    "apps/engine_plugin_telegram/lib/engine_plugin_telegram.ex"
  ]

  defp string_literals(path) do
    {:ok, ast} = path |> File.read!() |> Code.string_to_quoted()

    {_, acc} =
      Macro.prewalk(ast, [], fn
        # Documentation explains the rule rather than rendering alert text, so
        # replace @moduledoc/@doc nodes with a leaf and skip their contents.
        {:@, _, [{doc, _, _}]}, acc when doc in [:moduledoc, :doc] -> {:skipped, acc}
        binary, acc when is_binary(binary) -> {binary, [binary | acc]}
        node, acc -> {node, acc}
      end)

    acc
  end

  test "no template string in the rendering sources uses banned wording" do
    offenders =
      for rel <- @sources,
          path = Path.join(@root, rel),
          literal <- string_literals(path),
          literal =~ @banned,
          do: {rel, literal}

    assert offenders == [], "banned wording in templates: #{inspect(offenders)}"
  end

  test "the sources list points at real files" do
    for rel <- @sources, do: assert(File.exists?(Path.join(@root, rel)), rel)
  end

  test "rendered alerts contain no banned wording" do
    anomaly = %{
      id: "id1",
      series_key: {"requests_total", [{"job", "api"}]},
      start_ms: 1_700_000_000_000,
      score: 99.95,
      observed: 3.0,
      lower: 1.0,
      upper: 2.0,
      baseline_type: :short_term
    }

    candidates = [
      %{series_key: {"cpu", [{"host", "a"}]}, rank: 1, weak?: true},
      %{series_key: {"mem", [{"host", "a"}]}, rank: 2, weak?: false}
    ]

    for cands <- [[], candidates], base <- [nil, "https://console.example"] do
      text = anomaly |> Render.build(cands, console_base_url: base) |> Render.text()
      refute text =~ @banned, text
    end
  end
end

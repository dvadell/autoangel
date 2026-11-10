defmodule AutoangelReplay.ReportPropertyTest do
  @moduledoc """
  Property 69: replay metrics follow the matching rule (Requirement 77.4).
  For any labels and alerts, recall, precision and alerts per simulated day,
  overall and per type, equal an independent reference computation.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias AutoangelReplay.Report

  @types [:spike, :drop, :level_shift, :silence, :drift]
  @keys [{"a", []}, {"b", [{"h", "1"}]}, {"c", []}]
  @day 86_400_000

  defp label_gen do
    gen all(
          key <- member_of(@keys),
          type <- member_of(@types),
          start <- integer(0..100_000),
          len <- integer(0..5_000)
        ) do
      %{series_key: key, type: type, start_ms: start, end_ms: start + len}
    end
  end

  defp alert_gen,
    do:
      map(tuple({member_of(@keys), integer(-1_000..120_000)}), fn {k, t} ->
        %{series_key: k, start_ms: t}
      end)

  defp params_gen do
    gen all(
          m <- integer(0..3_000),
          n <- integer(0..4),
          sweep <- integer(0..1_000),
          intervals <- list_of(one_of([constant(nil), integer(0..2_000)]), length: length(@keys)),
          span <- one_of([constant(0), integer(1..(10 * @day))])
        ) do
      %{
        persistence_ms: m,
        persistence_n: n,
        sweep_ms: sweep,
        # A Series may have no typical interval recorded (counted as 0).
        typical_interval_ms:
          for({k, i} <- Enum.zip(@keys, intervals), i != nil, into: %{}, do: {k, i}),
        span_ms: span
      }
    end
  end

  # --- reference, written directly from the design text -----------------------

  defp ref_match?(alert, label, p) do
    tol =
      max(p.persistence_ms, p.persistence_n * Map.get(p.typical_interval_ms, label.series_key, 0)) +
        if(label.type == :silence, do: 2 * p.sweep_ms, else: 0)

    alert.series_key == label.series_key and label.start_ms <= alert.start_ms and
      alert.start_ms <= label.end_ms + tol
  end

  defp div_or_nil(_a, 0), do: nil
  defp div_or_nil(a, b), do: a / b

  defp reference(labels, alerts, p) do
    days = if p.span_ms == 0, do: nil, else: p.span_ms / @day
    recalled = for l <- labels, Enum.any?(alerts, &ref_match?(&1, l, p)), do: l
    matching = for a <- alerts, Enum.any?(labels, &ref_match?(a, &1, p)), do: a

    per_type =
      for t <- Enum.uniq(Enum.map(labels, & &1.type)), into: %{} do
        of_t = for a <- alerts, Enum.any?(labels, &(&1.type == t and ref_match?(a, &1, p))), do: a

        {t,
         %{
           recall:
             div_or_nil(Enum.count(recalled, &(&1.type == t)), Enum.count(labels, &(&1.type == t))),
           precision: div_or_nil(length(of_t), length(alerts)),
           alerts_per_day: days && length(of_t) / days
         }}
      end

    %{
      overall: %{
        recall: div_or_nil(length(recalled), length(labels)),
        precision: div_or_nil(length(matching), length(alerts)),
        alerts_per_day: days && length(alerts) / days
      },
      per_type: per_type
    }
  end

  property "computed metrics equal the reference computation" do
    check all(
            labels <- list_of(label_gen(), max_length: 12),
            alerts <- list_of(alert_gen(), max_length: 20),
            params <- params_gen(),
            max_runs: 500
          ) do
      assert Report.compute(labels, alerts, params) == reference(labels, alerts, params)
    end
  end

  property "alerts on the boundaries of the match window" do
    check all(label <- label_gen(), params <- params_gen(), max_runs: 300) do
      tol = Report.tolerance(label, params)
      at = fn t -> %{series_key: label.series_key, start_ms: t} end

      assert Report.matches?(at.(label.start_ms), label, params)
      assert Report.matches?(at.(label.end_ms + tol), label, params)
      refute Report.matches?(at.(label.start_ms - 1), label, params)
      refute Report.matches?(at.(label.end_ms + tol + 1), label, params)

      other = Enum.find(@keys, &(&1 != label.series_key))
      refute Report.matches?(%{series_key: other, start_ms: label.start_ms}, label, params)
    end
  end
end

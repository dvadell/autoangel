defmodule Autoangel.Alert.DispatchPolicyPropertyTest do
  @moduledoc """
  Property 28: dispatch decisions match the policy model
  (Req 18.9, 27.1, 27.2, 27.3, 27.5, 27.6, 27.7, 30.1).

  Random interleavings of Anomaly starts (value or missing kind, learning or
  not, with an empty or non-empty Candidate list), resolutions, Series mutes and
  clock advances are fed to `Autoangel.Alert.Policy` and to a small reference
  model; every step must produce the same deliveries and Incident opens/closes.

  Scope note: Incident grouping (Req 28.1, 28.3) is a P1 feature that is not
  implemented, so the model has single-Series Incidents only, and Incident mutes
  have no observable effect until resolution notifications (Req 29, P1) exist.
  Extend this property with grouping and Incident-mute steps when they land.
  """
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Autoangel.Alert.Policy

  @cooldown 1_000
  @plugins [:email, :telegram]
  @series for i <- 1..4, do: {"m", [{"host", "h#{i}"}]}

  defp step_gen do
    one_of([
      tuple(
        {constant(:start), member_of(@series), member_of([:value, :missing]), boolean(),
         member_of([[], [%{series_key: {"c", []}, rank: 1, weak?: false, evidence: %{}}]])}
      ),
      tuple({constant(:resolve), member_of(@series), member_of([:value, :missing])}),
      tuple({constant(:mute), member_of(@series)}),
      tuple({constant(:advance), integer(1..1_500)})
    ])
  end

  # --- reference model --------------------------------------------------------
  # Per Series: the opener anomaly id of its open Incident (or nil) and the end
  # of its cooldown. A start opens an Incident iff it is not learning, the Series
  # has no open Incident and no active cooldown; it is delivered to every plugin
  # unless the Series is muted. An Incident closes when its opener resolves, and
  # the cooldown runs from the close time.

  defp model_start(m, key, id, learning?, now) do
    cond do
      learning? ->
        {m, []}

      Map.get(m.open, key) ->
        {m, []}

      Map.get(m.cooldown, key, 0) > now ->
        {m, []}

      true ->
        {put_in(m.open[key], id), [{:open, key} | model_deliveries(m, key, id)]}
    end
  end

  defp model_deliveries(m, key, id) do
    if key in m.muted, do: [], else: for(p <- @plugins, do: {p, id})
  end

  defp model_resolve(m, key, id, now) do
    if Map.get(m.open, key) == id do
      m = %{m | open: Map.delete(m.open, key), cooldown: Map.put(m.cooldown, key, now + @cooldown)}
      {m, [{:close, key}]}
    else
      {m, []}
    end
  end

  # --- policy observations ----------------------------------------------------

  defp observe(actions) do
    Enum.flat_map(actions, fn
      {:open_incident, inc} -> [{:open, inc.series_key}]
      {:close_incident, inc} -> [{:close, inc.series_key}]
      {:deliver, plugin, alert} -> [{plugin, alert.anomaly_id}]
      _ -> []
    end)
  end

  property "Policy decisions equal the reference model at every step" do
    check all(steps <- list_of(step_gen(), max_length: 60), max_runs: 300) do
      init = %{
        policy: Policy.new(plugins: @plugins, cooldown_ms: @cooldown),
        model: %{open: %{}, cooldown: %{}, muted: MapSet.new()},
        # The currently open anomaly per {series, kind} in the event stream.
        live: %{},
        now: 0,
        n: 0
      }

      Enum.reduce(steps, init, fn step, acc -> run(step, acc) end)
    end
  end

  defp run({:advance, ms}, acc), do: %{acc | now: acc.now + ms}

  defp run({:mute, key}, acc) do
    %{
      acc
      | policy: Policy.mute(acc.policy, {:series, key}),
        model: %{acc.model | muted: MapSet.put(acc.model.muted, key)}
    }
  end

  # A Series cannot start a second anomaly of the same kind while one is open,
  # mirroring Decision; such generated steps are skipped.
  defp run({:start, key, kind, learning?, cands}, acc) do
    if Map.has_key?(acc.live, {key, kind}) do
      acc
    else
      id = "a#{acc.n}"

      anomaly = %{
        id: id,
        series_key: key,
        kind: kind,
        start_ms: acc.now,
        score: 90.0,
        learning?: learning?
      }

      {policy, actions} = Policy.on_event(acc.policy, {:anomaly_started, anomaly, cands}, acc.now)
      {model, expected} = model_start(acc.model, key, id, learning?, acc.now)

      assert observe(actions) == expected,
             "start #{id} on #{inspect(key)} learning?=#{learning?} at #{acc.now}"

      %{
        acc
        | policy: policy,
          model: model,
          live: Map.put(acc.live, {key, kind}, anomaly),
          n: acc.n + 1
      }
    end
  end

  defp run({:resolve, key, kind}, acc) do
    case Map.pop(acc.live, {key, kind}) do
      {nil, _} ->
        acc

      {anomaly, live} ->
        ended = Map.put(anomaly, :end_ms, acc.now)
        {policy, actions} = Policy.on_event(acc.policy, {:anomaly_resolved, ended}, acc.now)
        {model, expected} = model_resolve(acc.model, key, anomaly.id, acc.now)
        assert observe(actions) == expected, "resolve #{anomaly.id} at #{acc.now}"
        %{acc | policy: policy, model: model, live: live}
    end
  end
end

defmodule Autoangel.Alert.PolicyMembershipPropertyTest do
  @moduledoc """
  Property 27 (validates Requirements 27.4, 28.4).

  "For any sequence of Anomaly start and end events across any set of Series,
  with grouping on or off, every Series is a member of at most one open Incident
  at every point, and every Anomaly start that is not suppressed by cooldown,
  learning or an existing Incident either opens one new Incident or joins exactly
  one open Incident."

  Grouping is not implemented in `Autoangel.Alert.Policy`; each start opens its own
  single-series Incident. We exercise a random interleaving of starts, resolves and
  clock advances over a small set of Series and compare `Policy` against an
  independent reference model of the membership/suppression invariant.
  """

  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Autoangel.Alert.Policy

  @cooldown_ms 1_000

  # ── Generators ────────────────────────────────────────────────────────────

  defp series_key_gen do
    gen all(idx <- integer(1..5)) do
      {"metric", [{"host", "h#{idx}"}]}
    end
  end

  # A single step in the scenario: a start, a resolve, or a clock advance.
  # `delta` is the (non-negative) amount `now_ms` moves forward BEFORE the
  # step is applied, keeping the virtual clock monotonic.
  defp step_gen do
    gen all(
          delta <- integer(0..1_500),
          step <-
            one_of([
              gen all(
                    key <- series_key_gen(),
                    learning? <- boolean()
                  ) do
                {:start, key, learning?}
              end,
              gen all(key <- series_key_gen()) do
                {:resolve, key}
              end,
              constant(:advance)
            ])
        ) do
      {delta, step}
    end
  end

  defp scenario_gen do
    list_of(step_gen(), max_length: 40)
  end

  # ── Reference model ─────────────────────────────────────────────────────────
  #
  # Per series we track whether it currently has an open incident and, when
  # closed, the cooldown expiry. We never read Policy internals to derive it.
  #
  # model: %{series_key => %{open?: boolean, cooldown_until: integer, open_id: term | nil}}
  #
  # `open_id` is the anomaly id that opened the series' current incident. Policy
  # keys anomalies by :id and only closes an incident when EVERY contained
  # anomaly has an :end_ms, so a resolve must reference that same id to resolve
  # the single anomaly (grouping is unimplemented, so each incident holds one).

  defp model_start(model, key, open_id, learning?, now) do
    entry = Map.get(model, key, %{open?: false, cooldown_until: 0, open_id: nil})

    cond do
      learning? -> {model, :suppressed}
      entry.open? -> {model, :suppressed}
      # Policy's cooldown_active? uses strictly-greater-than `now`.
      entry.cooldown_until > now -> {model, :suppressed}
      true -> {Map.put(model, key, %{entry | open?: true, open_id: open_id}), :opened}
    end
  end

  # Each incident is single-anomaly here (grouping unimplemented), so resolving
  # that one anomaly (end_ms set) closes the series' open incident and starts
  # its cooldown. Returns the open anomaly id to resolve, or nil when none open.
  defp model_resolve(model, key, now) do
    case Map.get(model, key) do
      %{open?: true, open_id: open_id} = entry ->
        updated = %{entry | open?: false, open_id: nil, cooldown_until: now + @cooldown_ms}
        {Map.put(model, key, updated), :closed, open_id}

      _ ->
        {model, :noop, nil}
    end
  end

  # ── Structural invariants on Policy state (checked after every event) ────────

  defp assert_membership_invariants(state) do
    # series_incident entries point only at currently-open incidents.
    for {series_key, incident_id} <- state.series_incident do
      incident = Map.get(state.incidents, incident_id)

      assert incident != nil,
             "series #{inspect(series_key)} maps to missing incident #{inspect(incident_id)}"

      assert Map.get(incident, :open?) == true,
             "series #{inspect(series_key)} maps to non-open incident #{inspect(incident_id)}"

      assert series_key in Map.get(incident, :series_keys, []),
             "series #{inspect(series_key)} not listed in its incident's series_keys"
    end

    # A series maps to at most one incident: map keys are unique by construction,
    # so the invariant is that every open incident's series_keys are disjoint.
    open_series_lists =
      state.incidents
      |> Map.values()
      |> Enum.filter(&(Map.get(&1, :open?) == true))
      |> Enum.map(&Map.get(&1, :series_keys, []))

    all = Enum.concat(open_series_lists)

    assert length(all) == length(Enum.uniq(all)),
           "a series is a member of more than one open incident: #{inspect(all)}"
  end

  defp open_action_count(actions), do: Enum.count(actions, &match?({:open_incident, _}, &1))
  defp close_action_count(actions), do: Enum.count(actions, &match?({:close_incident, _}, &1))

  # ── Property ─────────────────────────────────────────────────────────────────

  property "membership, suppression and cooldown invariants hold for any event sequence" do
    check all(scenario <- scenario_gen()) do
      state = Policy.new(cooldown_ms: @cooldown_ms, now_ms: 0)
      # fold: {policy_state, model, now_ms, next_anomaly_id}
      Enum.reduce(scenario, {state, %{}, 0, 0}, fn {delta, step}, {state, model, now, id} ->
        now = now + delta

        case step do
          :advance ->
            {state, model, now, id}

          {:start, key, learning?} ->
            open_id = {:anomaly, id}

            anomaly = %{
              id: open_id,
              series_key: key,
              start_ms: now,
              end_ms: nil,
              learning?: learning?
            }

            {state, actions} = Policy.on_event(state, {:anomaly_started, anomaly}, now)
            {model, expected} = model_start(model, key, open_id, learning?, now)

            assert_membership_invariants(state)

            # Invariant 3: a start opens at most one incident.
            opens = open_action_count(actions)
            assert opens <= 1, "a single start opened #{opens} incidents"

            # Invariant 4: suppression matches the reference model exactly.
            case expected do
              :opened ->
                assert opens == 1,
                       "model expected an open for #{inspect(key)} but Policy suppressed it"

                # When opened the series now maps to the id of the incident in
                # the returned {:open_incident, inc} action — not just that some
                # entry exists.
                {:open_incident, opened_incident} =
                  Enum.find(actions, &match?({:open_incident, _}, &1))

                assert Map.has_key?(state.series_incident, key),
                       "opened incident but series #{inspect(key)} not registered"

                assert Map.get(state.series_incident, key) == opened_incident.id,
                       "series #{inspect(key)} must map to the opened incident's id " <>
                         "#{inspect(opened_incident.id)}"

              :suppressed ->
                assert opens == 0,
                       "model expected suppression for #{inspect(key)} but Policy opened one"
            end

            {state, model, now, id + 1}

          {:resolve, key} ->
            {model, expected, open_id} = model_resolve(model, key, now)

            # Resolve the SAME anomaly that opened the incident; Policy only
            # closes when every anomaly in the incident has an :end_ms. When no
            # incident is open for the series, any id works (Policy no-ops).
            anomaly = %{
              id: open_id || {:anomaly, id},
              series_key: key,
              start_ms: now,
              end_ms: now,
              learning?: false
            }

            {state, actions} = Policy.on_event(state, {:anomaly_resolved, anomaly}, now)

            assert_membership_invariants(state)

            closes = close_action_count(actions)

            case expected do
              :closed ->
                assert closes == 1,
                       "model expected a close for #{inspect(key)} but Policy did not"

                # Closing removes the series from the open-membership map.
                refute Map.has_key?(state.series_incident, key),
                       "closed incident but series #{inspect(key)} still registered as open"

              :noop ->
                assert closes == 0,
                       "model expected no close for #{inspect(key)} but Policy closed one"
            end

            {state, model, now, id}
        end
      end)
    end
  end

  # ── Focused cooldown example (Invariant 5) ────────────────────────────────────

  property "cooldown suppresses re-open within window and permits it after expiry" do
    check all(
            key <- series_key_gen(),
            post_close_delta <- integer(0..3_000)
          ) do
      state = Policy.new(cooldown_ms: @cooldown_ms, now_ms: 0)

      open = %{id: {:anomaly, 0}, series_key: key, start_ms: 0, end_ms: nil, learning?: false}
      {state, actions} = Policy.on_event(state, {:anomaly_started, open}, 0)
      assert open_action_count(actions) == 1

      close = %{id: {:anomaly, 0}, series_key: key, start_ms: 0, end_ms: 100, learning?: false}
      {state, actions} = Policy.on_event(state, {:anomaly_resolved, close}, 100)
      assert close_action_count(actions) == 1
      # cooldown expiry is 100 + @cooldown_ms
      expiry = 100 + @cooldown_ms

      now = 100 + post_close_delta
      reopen = %{id: {:anomaly, 1}, series_key: key, start_ms: now, end_ms: nil, learning?: false}
      {state, actions} = Policy.on_event(state, {:anomaly_started, reopen}, now)

      # Policy's cooldown_active? is `cooldowns[key] > now`, so a re-open is
      # suppressed only while the clock is strictly BEFORE expiry (now < expiry)
      # and is ALLOWED the instant the cool-off has ended (now >= expiry), i.e.
      # at close + cooldown_ms per requirement 27.3.
      if now >= expiry do
        assert open_action_count(actions) == 1,
               "cooldown elapsed (now=#{now} >= expiry=#{expiry}) but re-open was suppressed"

        assert Map.has_key?(state.series_incident, key)
      else
        assert open_action_count(actions) == 0,
               "within cooldown (now=#{now} < expiry=#{expiry}) but re-open was allowed"
      end
    end
  end

  # ── Deterministic cooldown boundary example (Invariant 5) ─────────────────────
  #
  # Pins BOTH sides of the cooldown boundary with fixed values instead of relying
  # on the generator landing exactly on `expiry`. With cooldown_ms = 1000 and a
  # resolve at close_ms = 100, expiry = close_ms + cooldown_ms = 1100. Policy's
  # `cooldown_active?` is `cooldowns[key] > now`, so a start at `expiry - 1` is
  # strictly before expiry and SUPPRESSED, while a start at exactly `expiry` IS
  # allowed (per requirement 27.3).

  test "a re-open exactly at cooldown expiry is allowed but one tick earlier is suppressed" do
    key = {"metric", [{"host", "boundary"}]}
    close_ms = 100
    expiry = close_ms + @cooldown_ms

    # Open, then resolve at close_ms so the cooldown starts.
    base = Policy.new(cooldown_ms: @cooldown_ms, now_ms: 0)

    open = %{id: {:anomaly, 0}, series_key: key, start_ms: 0, end_ms: nil, learning?: false}
    {base, actions} = Policy.on_event(base, {:anomaly_started, open}, 0)
    assert open_action_count(actions) == 1

    close = %{id: {:anomaly, 0}, series_key: key, start_ms: 0, end_ms: close_ms, learning?: false}
    {base, actions} = Policy.on_event(base, {:anomaly_resolved, close}, close_ms)
    assert close_action_count(actions) == 1

    # One tick BEFORE expiry: suppressed, no new incident opens.
    before_now = expiry - 1

    before_start = %{
      id: {:anomaly, 1},
      series_key: key,
      start_ms: before_now,
      end_ms: nil,
      learning?: false
    }

    {_state, before_actions} =
      Policy.on_event(base, {:anomaly_started, before_start}, before_now)

    assert open_action_count(before_actions) == 0,
           "start at expiry-1 (#{before_now}) must be suppressed by cooldown"

    # Exactly AT expiry: allowed, exactly one incident opens. Reuse `base` (the
    # earlier suppressed attempt opened nothing, so state is unchanged).
    at_start = %{
      id: {:anomaly, 2},
      series_key: key,
      start_ms: expiry,
      end_ms: nil,
      learning?: false
    }

    {state, at_actions} = Policy.on_event(base, {:anomaly_started, at_start}, expiry)

    assert open_action_count(at_actions) == 1,
           "start exactly at expiry (#{expiry}) must be allowed"

    assert Map.has_key?(state.series_incident, key)
  end
end

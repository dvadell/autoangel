defmodule Autoangel.FeedbackIdempotencePropertyTest do
  @moduledoc """
  Property 33: Feedback is idempotent (Req 30.6). For any prior mute and
  sensitivity state, Feedback_Action and existing target, applying the action
  twice through `POST /api/v1/feedback` leaves the same mute and sensitivity
  state (in memory and in the State_Store) as applying it once, and both
  requests succeed.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Plug.Conn
  import Plug.Test

  alias Autoangel.{Feedback, SeriesRegistry, StateStore}
  alias Autoangel.HTTP.Control

  @config %{control_api: %{token: "t"}, auth_exemptions: []}
  @names ~w(a b c d)
  @keys for n <- @names, do: SeriesRegistry.canonical_key(n, %{"host" => "h"})
  # Incident 1 spans a and b; incident 2 spans c alone.
  @incidents %{{:incident, 1} => Enum.take(@keys, 2), {:incident, 2} => [Enum.at(@keys, 2)]}

  setup do
    Autoangel.Boot.set_phase(:ready)
    on_exit(fn -> Autoangel.Boot.set_phase(:starting) end)
    :ok
  end

  defp ref(key), do: %{id: SeriesRegistry.series_id(key), key: key, worker: 0, kind: :gauge}

  defp start(prior_mutes, prior_overrides, base) do
    dir = Path.join(System.tmp_dir!(), "aa-p33-#{System.unique_integer([:positive])}")
    {:ok, store} = StateStore.start_link(name: nil, path: dir)
    for m <- prior_mutes, do: :ok = StateStore.put_mute(store, m, true)
    for {k, v} <- prior_overrides, do: :ok = StateStore.put_sensitivity(store, ref(k).id, v)
    {:ok, mutes} = Agent.start_link(fn -> MapSet.new(prior_mutes) end)

    {:ok, fb} =
      Feedback.start_link(
        name: nil,
        state_store: store,
        base_sensitivity: base,
        lookup_series: fn key -> if key in @keys, do: {:ok, ref(key)}, else: :error end,
        incident_series: &Map.fetch(@incidents, &1),
        apply_mute: fn target -> Agent.update(mutes, &MapSet.put(&1, target)) end,
        apply_sensitivity: fn _ref, _v -> :ok end
      )

    %{dir: dir, store: store, mutes: mutes, fb: fb}
  end

  defp snapshot(s) do
    %{
      stored_mutes: StateStore.records(s.store, :mute),
      stored_overrides: StateStore.records(s.store, :sensitivity),
      memory_mutes: Agent.get(s.mutes, & &1),
      memory_overrides: Feedback.overrides(s.fb)
    }
  end

  defp post(s, action, target) do
    conn(:post, "/api/v1/feedback", JSON.encode!(%{"action" => action, "target" => target}))
    |> put_req_header("authorization", "Bearer t")
    |> Control.call(config: @config, feedback: s.fb)
    |> Map.fetch!(:status)
  end

  defp target_gen do
    one_of([
      map(member_of(@names), &%{"series" => %{"name" => &1, "labels" => %{"host" => "h"}}}),
      map(member_of(["1", "2"]), &%{"incident_id" => &1})
    ])
  end

  property "applying the same action twice equals applying it once" do
    check all(
            action <- member_of(["mute", "this was normal"]),
            target <- target_gen(),
            prior_mutes <-
              list_of(member_of(Enum.map(@keys, &{:series, &1}) ++ Map.keys(@incidents)),
                max_length: 3
              ),
            prior <- list_of(tuple({member_of(@keys), float(min: 0.05, max: 2.0)}), max_length: 3),
            base <- float(min: 0.05, max: 2.0),
            max_runs: 80
          ) do
      prior_overrides = Map.new(prior)
      s = start(Enum.uniq(prior_mutes), prior_overrides, base)

      assert post(s, action, target) == 200
      once = snapshot(s)
      assert post(s, action, target) == 200
      assert snapshot(s) == once

      GenServer.stop(s.fb)
      GenServer.stop(s.store)
      Agent.stop(s.mutes)
      File.rm_rf!(s.dir)
    end
  end
end

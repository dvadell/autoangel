defmodule Autoangel.InvalidFeedbackPropertyTest do
  @moduledoc """
  Property 34: invalid feedback changes nothing (Req 30.3). For any
  authenticated feedback request with an unknown action, a missing action or
  target, or a target the Engine does not hold, the response is a client error
  naming the invalid field, and the mute state, sensitivity state and State_Store
  contents are unchanged.
  """
  use ExUnit.Case, async: false
  use ExUnitProperties

  import Plug.Conn
  import Plug.Test

  alias Autoangel.{Feedback, SeriesRegistry, StateStore}
  alias Autoangel.HTTP.Control

  @config %{control_api: %{token: "t"}, auth_exemptions: []}
  @known SeriesRegistry.canonical_key("known", %{"host" => "h"})
  @known_target %{"series" => %{"name" => "known", "labels" => %{"host" => "h"}}}

  setup do
    Autoangel.Boot.set_phase(:ready)
    dir = Path.join(System.tmp_dir!(), "aa-p34-#{System.unique_integer([:positive])}")
    {:ok, store} = StateStore.start_link(name: nil, path: dir)
    id = SeriesRegistry.series_id(@known)
    :ok = StateStore.put_sensitivity(store, id, 0.75)
    :ok = StateStore.put_mute(store, {:incident, 9}, true)
    test = self()

    {:ok, fb} =
      Feedback.start_link(
        name: nil,
        state_store: store,
        lookup_series: fn
          @known -> {:ok, %{id: id, key: @known, worker: 0, kind: :gauge}}
          _ -> :error
        end,
        incident_series: fn
          {:incident, 9} -> {:ok, [@known]}
          _ -> :error
        end,
        apply_mute: &send(test, {:mutated, :mute, &1}),
        apply_sensitivity: &send(test, {:mutated, :sensitivity, &1, &2})
      )

    on_exit(fn ->
      Autoangel.Boot.set_phase(:starting)
      File.rm_rf(dir)
    end)

    {:ok, store: store, fb: fb}
  end

  defp snapshot(ctx) do
    {StateStore.records(ctx.store, :mute), StateStore.records(ctx.store, :sensitivity),
     Feedback.overrides(ctx.fb)}
  end

  defp post(fb, body) do
    conn =
      conn(:post, "/api/v1/feedback", body)
      |> put_req_header("authorization", "Bearer t")
      |> Control.call(config: @config, feedback: fb)

    {conn.status, JSON.decode!(conn.resp_body)}
  end

  defp text, do: string(:printable, max_length: 12)

  # Incident 9 is held; a string that parses to it would be a valid target.
  defp held_incident?(s), do: Integer.parse(s) == {9, ""}

  # Each generated request pairs a body with the field it must be rejected for.
  defp invalid_gen do
    one_of([
      # unknown action (anything but the two valid ones), valid target
      map(
        filter(text(), &(&1 not in ["mute", "this was normal"])),
        &{%{"action" => &1, "target" => @known_target}, "action"}
      ),
      # non-string action
      map(
        one_of([integer(), boolean(), list_of(integer(), max_length: 2)]),
        &{%{"action" => &1, "target" => @known_target}, "action"}
      ),
      # missing action
      constant({%{"target" => @known_target}, "action"}),
      # missing target
      map(member_of(["mute", "this was normal"]), &{%{"action" => &1}, "target"}),
      # target the Engine does not hold: unknown series
      map(tuple({member_of(["mute", "this was normal"]), filter(text(), &(&1 != "known"))}), fn {a,
                                                                                                 n} ->
        {%{"action" => a, "target" => %{"series" => %{"name" => n, "labels" => %{"host" => "h"}}}},
         "target"}
      end),
      # unknown or malformed incident id
      map(
        tuple({member_of(["mute", "this was normal"]), one_of([integer(10..999), text()])}),
        fn {a, i} ->
          {%{"action" => a, "target" => %{"incident_id" => i}}, "target"}
        end
      ),
      # malformed target shapes
      map(
        tuple(
          {member_of(["mute", "this was normal"]),
           one_of([
             integer(),
             text(),
             constant(nil),
             constant(%{}),
             constant(%{"series" => %{"labels" => %{}}})
           ])}
        ),
        fn {a, t} ->
          {%{"action" => a, "target" => t}, "target"}
        end
      )
    ])
  end

  property "invalid requests are rejected naming the field and change nothing", ctx do
    before = snapshot(ctx)

    check all({request, field} <- invalid_gen(), max_runs: 300) do
      assert {400, %{"error" => "invalid_field", "field" => ^field}} =
               post(ctx.fb, JSON.encode!(request))

      refute_received {:mutated, _, _}
      refute_received {:mutated, _, _, _}
      assert snapshot(ctx) == before
    end
  end

  property "non-object or malformed bodies are rejected as the body field", ctx do
    before = snapshot(ctx)

    check all(
            body <-
              one_of([
                text(),
                map(list_of(integer(), max_length: 3), &JSON.encode!/1),
                constant("null")
              ]),
            max_runs: 100
          ) do
      body = if match?({:ok, %{}}, JSON.decode(body)), do: "[]", else: body
      assert {400, %{"error" => "invalid_field", "field" => "body"}} = post(ctx.fb, body)
      refute_received {:mutated, _, _}
      refute_received {:mutated, _, _, _}
      assert snapshot(ctx) == before
    end
  end
end

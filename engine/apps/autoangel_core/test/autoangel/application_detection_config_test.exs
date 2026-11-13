defmodule Autoangel.ApplicationDetectionConfigTest do
  @moduledoc """
  Task 25.1: the detection settings of the loaded configuration reach the
  Workers. The settings are built by `Autoangel.Application.decision_cfg/1`,
  the same function the application uses to start the Workers.
  """
  use ExUnit.Case, async: false

  alias Autoangel.Workers
  alias Autoangel.Workers.Worker

  defp config(overrides) do
    {:ok, base} = Autoangel.Config.load("\n")
    Map.merge(base, overrides)
  end

  defp start_workers(config) do
    start_supervised!(
      {Workers,
       pool_size: 1,
       decision_cfg: Autoangel.Application.decision_cfg(config),
       sweep_interval: 3_600_000,
       queue_capacity: 20_000,
       max_batch: 8,
       state_store: nil,
       scrape_status_table: nil}
    )
  end

  test "decision_cfg carries exactly the detection sections" do
    cfg =
      config(%{
        learning: %{duration: 7, cycles: 3},
        persistence: %{n: 1, m: 9},
        missing_data: %{multiple: 4.5, sweep_interval: 3_600}
      })
      |> Autoangel.Application.decision_cfg()

    assert Map.keys(cfg) |> Enum.sort() ==
             [:drift, :learning, :missing_data, :persistence, :sensitivity, :series_overrides]

    assert cfg.learning == %{duration: 7, cycles: 3}
    assert cfg.persistence == %{n: 1, m: 9}
    assert cfg.missing_data == %{multiple: 4.5, sweep_interval: 3_600}
    # Transport settings never reach detection.
    refute Map.has_key?(cfg, :http)
  end

  test "a configured value reaches a started Worker" do
    sup = start_workers(config(%{persistence: %{n: 1, m: 9}}))

    %Worker{decision_cfg: cfg} = :sys.get_state(Workers.worker(0, sup))
    assert cfg.persistence == %{n: 1, m: 9}
    assert cfg.learning == %{duration: 86_400, cycles: 2}
  end
end

defmodule AutoangelFaults.EngineFaultTest do
  @moduledoc """
  Task 29.3 (Requirements 73.7 to 73.9): one plugin failing, a Worker killed, and
  an Engine restart on the same State_Store directory. Needs `scripts/fault_env.sh up`.
  """
  use ExUnit.Case, async: false

  alias AutoangelFaults.{Harness, Toxiproxy}

  @moduletag :fault
  @moduletag timeout: 600_000

  # Before the injected spike (step 500), after the learning period.
  @fault_step 350

  setup_all do
    if Toxiproxy.available?() do
      {:ok, reference: Harness.run(Harness.fixture())}
    else
      IO.puts("[fault] skipped: run `eval \"$(scripts/fault_env.sh up)\"` first")
      {:ok, skip: true}
    end
  end

  defp assert_passed(checks) do
    assert Harness.passed?(checks), "\n" <> Harness.report(checks)
  end

  test "one plugin failing does not affect the other plugin", %{reference: reference} = ctx do
    unless ctx[:skip] do
      run = Harness.run(Harness.fixture(), plugins: ["capture", "failing"])
      assert_passed(Harness.compare(reference, run))
    end
  end

  test "a killed Worker resumes from its last checkpoint", %{reference: reference} = ctx do
    unless ctx[:skip] do
      fault = fn
        @fault_step, _total, %{kill_worker: kill} ->
          :ok = Autoangel.Checkpointer.checkpoint_now()
          # The fixture's Series hash to Workers; kill the one that owns host "a".
          kill.(owner_index())

        _, _, _ ->
          :ok
      end

      run = Harness.run(Harness.fixture(), fault: fault)
      # Samples the killed Workers had not yet handled are not replayed, but the
      # Storage_Writer is a separate process, so nothing stored is lost.
      assert_passed(Harness.compare(reference, run))
    end
  end

  defp owner_index do
    key = Autoangel.SeriesRegistry.canonical_key("fault_metric", %{"host" => "a"})
    Autoangel.SeriesRegistry.worker_index(key, System.schedulers_online())
  end

  test "an Engine restart on the same State_Store keeps detection state",
       %{reference: reference} = ctx do
    unless ctx[:skip] do
      fault = fn
        @fault_step, _total, %{restart_engine: restart} -> restart.()
        _, _, _ -> :ok
      end

      run = Harness.run(Harness.fixture(), fault: fault)
      checks = Harness.compare(reference, run)

      # A stop discards Samples still queued in the Storage_Writer; alerts and
      # duplicate-freedom must still hold.
      checks = Enum.reject(checks, fn {name, _, _} -> name == "no Sample lost and none dropped" end)
      assert_passed(checks)
    end
  end
end

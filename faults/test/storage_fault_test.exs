defmodule AutoangelFaults.StorageFaultTest do
  @moduledoc """
  Task 29.2 (Requirements 65.8, 73.2 to 73.6): TimescaleDB stopped and restarted,
  slow writes, Console absent, Storage_Writer killed. Needs `scripts/fault_env.sh up`.
  """
  use ExUnit.Case, async: false

  alias AutoangelFaults.{Harness, Toxiproxy}

  @moduletag :fault
  @moduletag timeout: 600_000

  setup_all do
    if Toxiproxy.available?() do
      {:ok, reference: Harness.run(Harness.fixture())}
    else
      IO.puts("[fault] skipped: run `eval \"$(scripts/fault_env.sh up)\"` first")
      {:ok, skip: true}
    end
  end

  defp verdict(checks), do: {Harness.passed?(checks), Harness.report(checks)}

  defp assert_passed(checks) do
    {ok?, text} = verdict(checks)
    assert ok?, "\n" <> text
  end

  test "database stopped and restarted", %{reference: reference} = ctx do
    unless ctx[:skip] do
      fault = fn
        150, _total, _ctx -> Toxiproxy.down()
        400, _total, _ctx -> Process.sleep(2_000) && Toxiproxy.up()
        _, _, _ -> :ok
      end

      run = Harness.run(Harness.fixture(), fault: fault)
      checks = Harness.compare(reference, run)
      assert_passed(checks)
      # The fault really hit the Storage_Writer: some batches had to be retried.
      assert run.writer.retryable > 0
    end
  end

  test "slow database writes", %{reference: reference} = ctx do
    unless ctx[:skip] do
      fault = fn
        150, _total, _ctx -> Toxiproxy.slow(1_500)
        400, _total, _ctx -> Toxiproxy.reset()
        _, _, _ -> :ok
      end

      run = Harness.run(Harness.fixture(), fault: fault)
      assert_passed(Harness.compare(reference, run))
    end
  end

  test "Console absent: alerts link to an unreachable Console and are unaffected",
       %{reference: reference} = ctx do
    # The Console is a separate release that only reads the database; the Engine
    # never calls it. This run points the alert link at a dead address.
    unless ctx[:skip] do
      run =
        Harness.run(Harness.fixture(),
          toml: "[alerts]\nconsole_base_url = \"http://127.0.0.1:9\"\n"
        )

      assert_passed(Harness.compare(reference, run))
    end
  end

  test "Storage_Writer killed", %{reference: reference} = ctx do
    unless ctx[:skip] do
      fault = fn
        300, _total, _ctx ->
          writer = Process.whereis(Autoangel.Storage.Writer)
          ref = Process.monitor(writer)
          Process.exit(writer, :kill)
          receive do: ({:DOWN, ^ref, _, _, _} -> :ok)
          wait_for_new_writer(writer)

        _, _, _ ->
          :ok
      end

      run = Harness.run(Harness.fixture(), fault: fault)

      # Samples queued at the moment of the kill are lost with the process; the
      # Engine keeps ingesting and alerting, and stores everything after it.
      assert run.found == reference.found
      assert run.db.samples > 0
      assert run.db.samples == run.db.distinct_samples
      assert run.db.samples <= run.pushed
    end
  end

  defp wait_for_new_writer(old, tries \\ 100) do
    case Process.whereis(Autoangel.Storage.Writer) do
      pid when is_pid(pid) and pid != old ->
        :ok

      _ when tries > 0 ->
        Process.sleep(100)
        wait_for_new_writer(old, tries - 1)

      _ ->
        flunk("Storage_Writer did not restart")
    end
  end
end

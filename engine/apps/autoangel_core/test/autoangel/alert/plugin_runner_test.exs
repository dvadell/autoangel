defmodule Autoangel.Alert.PluginRunnerTest do
  use ExUnit.Case, async: true

  defmodule SuccessfulPlugin do
    @behaviour Autoangel.Plugin
    def validate_config(_), do: :ok

    def deliver(alert, config) do
      send(config[:parent], {:delivered, alert})
      :ok
    end
  end

  defmodule RetryPlugin do
    @behaviour Autoangel.Plugin
    def validate_config(_), do: :ok

    def deliver(_alert, config) do
      attempt = Agent.get_and_update(config[:attempts], fn n -> {n + 1, n + 1} end)
      send(config[:parent], {:attempt, attempt})
      if attempt == 1, do: {:retryable, :temporary}, else: :ok
    end
  end

  defmodule BlockingPlugin do
    @behaviour Autoangel.Plugin
    def validate_config(_), do: :ok

    def deliver(_alert, _config),
      do:
        (
          Process.sleep(5_000)
          :ok
        )
  end

  defmodule PermanentPlugin do
    @behaviour Autoangel.Plugin
    def validate_config(_), do: :ok
    def deliver(_alert, _config), do: {:permanent, :rejected}
  end

  setup do
    {:ok, task_supervisor} = Task.Supervisor.start_link()
    %{task_supervisor: task_supervisor}
  end

  test "delivers asynchronously and records success", %{task_supervisor: task_supervisor} do
    alert = %{anomaly_id: "a-1"}

    {:ok, runner} =
      Autoangel.Alert.PluginRunner.start_link(
        plugin: SuccessfulPlugin,
        config: %{parent: self()},
        task_supervisor: task_supervisor,
        timeout: 100
      )

    assert :ok = Autoangel.Alert.PluginRunner.enqueue(runner, alert)
    assert_receive {:delivered, ^alert}, 500
    assert %{success: 1, queue_depth: 0} = Autoangel.Alert.PluginRunner.metrics(runner)
  end

  test "retries retryable delivery and records final success", %{task_supervisor: task_supervisor} do
    {:ok, attempts} = Agent.start_link(fn -> 0 end)

    plugin = RetryPlugin

    # The plugin receives a stable attempt count through its configuration.
    config = %{parent: self(), attempts: attempts}

    {:ok, runner} =
      Autoangel.Alert.PluginRunner.start_link(
        plugin: plugin,
        config: config,
        task_supervisor: task_supervisor,
        timeout: 100,
        max_attempts: 2
      )

    assert :ok = Autoangel.Alert.PluginRunner.enqueue(runner, %{anomaly_id: "a-2"})
    assert_receive {:attempt, 1}, 500
    assert_receive {:attempt, 2}, 3_000
    assert %{retryable: 1, success: 1} = Autoangel.Alert.PluginRunner.metrics(runner)
    Agent.stop(attempts)
  end

  test "records permanent failures without crashing", %{task_supervisor: task_supervisor} do
    {:ok, runner} =
      Autoangel.Alert.PluginRunner.start_link(
        plugin: PermanentPlugin,
        config: %{},
        task_supervisor: task_supervisor,
        timeout: 50,
        max_attempts: 1
      )

    assert :ok = Autoangel.Alert.PluginRunner.enqueue(runner, %{anomaly_id: "permanent"})

    assert_eventually(
      fn -> Autoangel.Alert.PluginRunner.failures(runner) end,
      fn failures -> match?([%{anomaly: "permanent", error_class: :permanent}], failures) end
    )
  end

  test "abandons timed out delivery and records timeout", %{task_supervisor: task_supervisor} do
    {:ok, runner} =
      Autoangel.Alert.PluginRunner.start_link(
        plugin: BlockingPlugin,
        config: %{},
        task_supervisor: task_supervisor,
        timeout: 10,
        max_attempts: 1
      )

    assert :ok = Autoangel.Alert.PluginRunner.enqueue(runner, %{anomaly_id: "timeout"})

    assert_eventually(
      fn -> Autoangel.Alert.PluginRunner.failures(runner) end,
      fn failures -> match?([%{anomaly: "timeout", error_class: :timeout}], failures) end
    )
  end

  defp assert_eventually(fun, predicate, attempts \\ 50)
  defp assert_eventually(_fun, _predicate, 0), do: flunk("condition was not met")

  defp assert_eventually(fun, predicate, attempts) do
    value = fun.()

    if predicate.(value),
      do: value,
      else:
        (
          Process.sleep(10)
          assert_eventually(fun, predicate, attempts - 1)
        )
  end

  test "rejects work beyond the bounded queue", %{task_supervisor: task_supervisor} do
    plugin = BlockingPlugin

    {:ok, runner} =
      Autoangel.Alert.PluginRunner.start_link(
        plugin: plugin,
        config: %{parent: self()},
        task_supervisor: task_supervisor,
        queue_size: 1,
        timeout: 1_000
      )

    assert :ok = Autoangel.Alert.PluginRunner.enqueue(runner, %{id: 1})
    assert :ok = Autoangel.Alert.PluginRunner.enqueue(runner, %{id: 2})
    assert {:error, :full} = Autoangel.Alert.PluginRunner.enqueue(runner, %{id: 3})
  end
end

defmodule Autoangel.Alert.PluginRunner do
  @moduledoc "Isolated, bounded and retrying delivery worker for one plugin."

  use GenServer
  @default_queue_size 1_000
  @default_timeout 10_000
  @default_attempts 5
  @default_breaker_threshold 5
  @default_cooloff 300_000

  defstruct plugin: nil,
            config: %{},
            queue: :queue.new(),
            queue_size: @default_queue_size,
            timeout: @default_timeout,
            max_attempts: @default_attempts,
            breaker_threshold: @default_breaker_threshold,
            cooloff: @default_cooloff,
            task_supervisor: nil,
            observer: nil,
            current: nil,
            current_task: nil,
            timer: nil,
            breaker: :closed,
            consecutive_failures: 0,
            failures: [],
            metrics: %{success: 0, retryable: 0, permanent: 0, timeout: 0, skipped: 0, dropped: 0}

  @type option ::
          {:plugin, module()}
          | {:config, map()}
          | {:name, GenServer.server()}
          | {:task_supervisor, GenServer.server()}
          | {:queue_size, pos_integer()}
          | {:timeout, pos_integer()}
          | {:max_attempts, pos_integer()}
          | {:breaker_threshold, non_neg_integer()}
          | {:cooloff, pos_integer()}

  @spec start_link([option()]) :: GenServer.on_start()
  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))

  @spec enqueue(GenServer.server(), term()) :: :ok | {:error, :full}
  def enqueue(server, alert), do: GenServer.call(server, {:enqueue, alert})

  @spec metrics(GenServer.server()) :: map()
  def metrics(server), do: GenServer.call(server, :metrics)

  @spec failures(GenServer.server()) :: [map()]
  def failures(server), do: GenServer.call(server, :failures)

  @spec breaker(GenServer.server()) :: :closed | :half_open | {:open, integer()}
  def breaker(server), do: GenServer.call(server, :breaker)

  @impl true
  def init(opts) do
    plugin = Keyword.fetch!(opts, :plugin)

    state = %__MODULE__{
      plugin: plugin,
      config: Keyword.get(opts, :config, %{}),
      queue_size: Keyword.get(opts, :queue_size, @default_queue_size),
      timeout: Keyword.get(opts, :timeout, @default_timeout),
      max_attempts: Keyword.get(opts, :max_attempts, @default_attempts),
      breaker_threshold: Keyword.get(opts, :breaker_threshold, @default_breaker_threshold),
      cooloff: Keyword.get(opts, :cooloff, @default_cooloff),
      task_supervisor: Keyword.fetch!(opts, :task_supervisor),
      observer: Keyword.get(opts, :observer)
    }

    {:ok, state}
  end

  @impl GenServer
  def format_status(_reason, [_pdict, state]) do
    %{state | config: :redacted, current: :redacted, current_task: nil}
  end

  @impl true
  def handle_call({:enqueue, alert}, _from, %{queue: queue, queue_size: limit} = state) do
    if :queue.len(queue) >= limit do
      {:reply, {:error, :full}, put_metric(state, :dropped)}
    else
      {:reply, :ok, state |> put_metric(:enqueued) |> enqueue_and_start(alert)}
    end
  end

  def handle_call(:metrics, _from, state),
    do: {:reply, Map.put(state.metrics, :queue_depth, :queue.len(state.queue)), state}

  def handle_call(:failures, _from, state), do: {:reply, Enum.reverse(state.failures), state}
  def handle_call(:breaker, _from, state), do: {:reply, state.breaker, state}

  @impl true
  def handle_info({:retry, alert, attempt}, state) do
    {:noreply, start_attempt(%{state | current: {alert, attempt}, timer: nil})}
  end

  def handle_info({:attempt_timeout, ref}, %{current: {_, _, ref}} = state) do
    Process.demonitor(ref, [:flush])
    Process.exit(state.current_task, :kill)
    {:noreply, attempt_failed(state, {:timeout, :delivery_timeout})}
  end

  def handle_info({:attempt_timeout, _ref}, state), do: {:noreply, state}

  def handle_info({ref, result}, %{current: {alert, attempt, ref}} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, handle_result(state, alert, attempt, result)}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{current: {alert, attempt, ref}} = state) do
    {:noreply, handle_result(state, alert, attempt, {:retryable, {:task_exit, reason}})}
  end

  def handle_info(:cooloff_elapsed, %{breaker: {:open, _}} = state),
    do: {:noreply, %{state | breaker: :half_open}}

  def handle_info(:cooloff_elapsed, state), do: {:noreply, state}
  def handle_info(_message, state), do: {:noreply, state}

  defp enqueue_and_start(%{current: nil} = state, alert),
    do: start_or_skip(%{state | queue: :queue.in(alert, state.queue)})

  defp enqueue_and_start(state, alert), do: %{state | queue: :queue.in(alert, state.queue)}

  defp start_or_skip(%{breaker: {:open, _}, queue: queue} = state) do
    case :queue.out(queue) do
      {{:value, alert}, remaining} ->
        state
        |> put_metric(:skipped)
        |> failure(alert, :circuit_open)
        |> maybe_start_next(%{queue: remaining})

      {:empty, _} ->
        state
    end
  end

  defp start_or_skip(%{breaker: :half_open, queue: queue} = state) do
    case :queue.out(queue) do
      {{:value, alert}, remaining} ->
        start_attempt(%{state | queue: remaining, current: {alert, 1}})

      {:empty, _} ->
        state
    end
  end

  defp start_or_skip(state) do
    case :queue.out(state.queue) do
      {{:value, alert}, queue} -> start_attempt(%{state | queue: queue, current: {alert, 1}})
      {:empty, _} -> state
    end
  end

  defp start_attempt(%{current: {alert, attempt}} = state) do
    task =
      Task.Supervisor.async_nolink(state.task_supervisor, fn ->
        try do
          state.plugin.deliver(alert, state.config)
        rescue
          exception -> {:retryable, {:plugin_exception, exception.__struct__}}
        catch
          kind, _reason -> {:retryable, {:plugin_throw, kind}}
        end
      end)

    timer = Process.send_after(self(), {:attempt_timeout, task.ref}, state.timeout)

    put_in(state.current, {alert, attempt, task.ref})
    |> Map.put(:current_task, task.pid)
    |> Map.put(:timer, timer)
  end

  defp handle_result(state, _alert, _attempt, :ok) do
    cancel_timer(state.timer)

    state = %{
      state
      | current: nil,
        current_task: nil,
        timer: nil,
        breaker: :closed,
        consecutive_failures: 0
    }

    state |> put_metric(:success) |> notify_observer(:success) |> start_or_skip()
  end

  defp handle_result(state, _alert, _attempt, {:permanent, reason}),
    do: finish_failure(put_metric(state, :permanent), elem(state.current, 0), :permanent, reason)

  defp handle_result(state, _alert, _attempt, {:retryable, reason}),
    do: attempt_failed(state, {:retryable, reason})

  defp handle_result(state, _alert, _attempt, other),
    do: attempt_failed(state, {:retryable, {:invalid_result, other}})

  defp attempt_failed(%{current: {alert, attempt, _ref}} = state, {kind, reason}) do
    cancel_timer(state.timer)
    state = put_metric(%{state | current: nil, current_task: nil, timer: nil}, kind)

    if attempt < state.max_attempts and state.breaker != :half_open do
      delay = backoff(attempt)

      %{
        state
        | timer: Process.send_after(self(), {:retry, alert, attempt + 1}, delay),
          current: {alert, attempt + 1}
      }
    else
      finish_failure(state, alert, kind, reason)
    end
  end

  defp finish_failure(state, alert, kind, reason) do
    cancel_timer(state.timer)
    state = failure(state, alert, {kind, reason})
    consecutive = state.consecutive_failures + 1

    state = %{
      state
      | current: nil,
        current_task: nil,
        timer: nil,
        consecutive_failures: consecutive
    }

    state =
      if consecutive > state.breaker_threshold or state.breaker == :half_open,
        do: open_breaker(state),
        else: state

    state = notify_observer(state, :failure)
    start_or_skip(state)
  end

  defp open_breaker(state) do
    now = System.monotonic_time(:millisecond)
    Process.send_after(self(), :cooloff_elapsed, state.cooloff)
    %{state | breaker: {:open, now}}
  end

  defp maybe_start_next(state, updates), do: start_or_skip(Map.merge(state, updates))

  defp failure(state, alert, error) do
    anomaly =
      if is_map(alert),
        do: Map.get(alert, :anomaly_id) || Map.get(alert, :anomaly) || Map.get(alert, :id),
        else: nil

    record = %{
      plugin: state.plugin,
      anomaly: anomaly,
      error_class: error_class(error),
      error: sanitize_error(error),
      at: DateTime.utc_now()
    }

    %{state | failures: [record | Enum.take(state.failures, 99)]}
  end

  defp sanitize_error({kind, reason}) when kind in [:retryable, :permanent, :timeout],
    do: {kind, sanitize_reason(reason)}

  defp sanitize_error(error), do: sanitize_reason(error)

  defp sanitize_reason(value) when is_atom(value) or is_number(value) or is_boolean(value),
    do: value

  defp sanitize_reason(_value), do: :redacted

  defp error_class({:timeout, _}), do: :timeout
  defp error_class({kind, _}) when kind in [:retryable, :permanent], do: kind
  defp error_class(:circuit_open), do: :circuit_open
  defp error_class(_), do: :failure

  defp backoff(attempt) do
    base = trunc(:math.pow(2, attempt - 1) * 1_000)
    jitter = :rand.uniform(max(1, div(base, 5) * 2 + 1)) - div(base, 5) - 1
    max(base + jitter, base - div(base, 5) + attempt)
  end

  defp put_metric(state, key) do
    if key in [:success, :retryable, :permanent],
      do: Autoangel.Metrics.plugin_delivery(state.plugin, key)

    %{state | metrics: Map.update(state.metrics, key, 1, &(&1 + 1))}
  end

  defp notify_observer(%{observer: observer, plugin: plugin} = state, outcome)
       when is_pid(observer) do
    send(observer, {:plugin_delivery, plugin, outcome})
    state
  end

  defp notify_observer(%{observer: observer, plugin: plugin} = state, outcome)
       when is_function(observer, 2) do
    try do
      observer.(plugin, outcome)
    rescue
      _error -> :ok
    end

    state
  end

  defp notify_observer(state, _outcome), do: state

  defp cancel_timer(nil), do: :ok
  defp cancel_timer(timer), do: Process.cancel_timer(timer)
end

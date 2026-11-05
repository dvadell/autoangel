defmodule Autoangel.Scrape do
  @moduledoc "Supervised scraping of statically configured Prometheus targets."
  use Supervisor
  require Logger

  alias Autoangel.Exposition
  alias Autoangel.Scrape.Target

  @type target :: map()

  @doc "Starts one target process for every valid target configuration."
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc "Returns child specs only when at least one target is valid."
  def child_specs(opts \\ []) do
    targets = Keyword.get(opts, :targets, Keyword.get(opts, :scrape_targets, []))

    if Enum.any?(targets, &match?({:ok, _}, Target.validate(&1))) do
      [{__MODULE__, opts}]
    else
      []
    end
  end

  @impl Supervisor
  def init(opts) do
    table = Keyword.get(opts, :status_table, :autoangel_scrape_status)
    ensure_status_table(table)
    targets = Keyword.get(opts, :targets, Keyword.get(opts, :scrape_targets, []))
    target_opts = Keyword.drop(opts, [:name, :targets, :scrape_targets, :status_table])

    children =
      targets
      |> Enum.with_index()
      |> Enum.flat_map(fn {target, index} ->
        case Target.validate(target, index) do
          {:ok, target} ->
            [
              Supervisor.child_spec(
                {Target, Keyword.merge(target_opts, target: target, status_table: table)},
                id: {Target, target.id}
              )
            ]

          {:error, reason} ->
            Logger.error(
              "invalid scrape target #{inspect(target_name(target, index))}: #{reason}; skipping"
            )

            []
        end
      end)

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp ensure_status_table(table) do
    case :ets.whereis(table) do
      :undefined -> :ets.new(table, [:named_table, :public, :set])
      _tid -> table
    end
  end

  defp target_name(target, index), do: Map.get(target, :id, Map.get(target, "id", index))
end

defmodule Autoangel.Scrape.Processor do
  @moduledoc "Routes parsed exposition through the same registry and workers as push ingestion."

  alias Autoangel.Exposition

  @spec process([Exposition.Family.t()], keyword()) :: {:ok, map()} | {:error, term()}
  def process(families, opts \\ [])

  def process(families, opts) when is_list(families) do
    received_at = Keyword.get(opts, :received_at, System.system_time(:millisecond))
    source = Keyword.get(opts, :source, :push)

    samples =
      Enum.flat_map(families, fn family ->
        Enum.map(family.samples, &%{&1 | source: source})
      end)

    case Autoangel.Ingest.process_samples(samples, Keyword.put(opts, :receive_time, received_at)) do
      {:ok, counts} -> {:ok, counts}
      {:error, :overloaded} -> {:error, :overloaded}
    end
  end

  def process(_, _opts), do: {:error, :invalid_families}
end

defmodule Autoangel.Scrape.Target do
  @moduledoc "One independently scheduled, single-flight scrape target."
  use GenServer
  require Logger

  alias Autoangel.Exposition
  alias Autoangel.Scrape.Processor

  defstruct [
    :target,
    :id,
    :status_table,
    :fetcher,
    :processor_opts,
    :interval,
    :timeout,
    in_flight: nil,
    skipped: 0
  ]

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []),
    do: GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name))

  @spec validate(map(), non_neg_integer()) :: {:ok, map()} | {:error, String.t()}
  def validate(target, index \\ 0)

  def validate(target, index) when is_map(target) do
    url = Map.get(target, :url, Map.get(target, "url"))
    interval = Map.get(target, :interval, Map.get(target, "interval"))
    timeout = Map.get(target, :timeout, Map.get(target, "timeout"))
    id = Map.get(target, :id, Map.get(target, "id", url || index))

    cond do
      not is_binary(url) or url == "" -> {:error, "url is missing"}
      not positive_number?(interval) -> {:error, "interval is invalid"}
      not positive_number?(timeout) -> {:error, "timeout is invalid"}
      timeout > interval -> {:error, "timeout exceeds interval"}
      true -> {:ok, %{id: id, url: url, interval: interval, timeout: timeout}}
    end
  end

  def validate(_, _), do: {:error, "target must be a map"}

  def failing?(table, id) do
    case :ets.lookup(table, id) do
      [{^id, value}] when is_boolean(value) -> value
      [{^id, %{failing: value}}] -> value
      _ -> false
    end
  end

  @impl GenServer
  def init(opts) do
    target = Keyword.fetch!(opts, :target)
    interval = target.interval

    state = %__MODULE__{
      target: target,
      id: target.id,
      status_table: Keyword.fetch!(opts, :status_table),
      fetcher: Keyword.get(opts, :fetcher, &fetch/2),
      processor_opts: Keyword.get(opts, :processor_opts, []),
      interval: interval,
      timeout: target.timeout
    }

    put_initial_status(state)
    {:ok, schedule(state)}
  end

  @impl GenServer
  def handle_info(:tick, %{in_flight: nil} = state) do
    task = Task.async(fn -> state.fetcher.(state.target, receive_timeout: state.timeout) end)
    {:noreply, %{schedule(state) | in_flight: task.ref}}
  end

  def handle_info(:tick, state) do
    Logger.debug("scrape tick skipped for target #{inspect(state.id)}: fetch in flight")
    {:noreply, %{schedule(state) | skipped: state.skipped + 1}}
  end

  def handle_info({ref, result}, %{in_flight: ref} = state) do
    Process.demonitor(ref, [:flush])
    {:noreply, %{handle_result(state, result) | in_flight: nil}}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{in_flight: ref} = state) do
    {:noreply, %{mark_failure(state, "request failed: #{inspect(reason)}") | in_flight: nil}}
  end

  defp schedule(state) do
    Process.send_after(self(), :tick, interval_ms(state.interval))
    state
  end

  defp handle_result(state, {:ok, status, body}) when status in 200..299 and is_binary(body) do
    case Exposition.parse(body) do
      {:ok, families} ->
        Processor.process(
          families,
          Keyword.merge(state.processor_opts,
            received_at: System.system_time(:millisecond),
            source: {:scrape, state.id}
          )
        )

        put_status(state, false)
        state

      {:error, %{line: line}} ->
        mark_failure(
          state,
          "invalid exposition on target #{inspect(state.id)}, first invalid line #{line}"
        )
    end
  end

  defp handle_result(state, {:ok, status, _body}),
    do: mark_failure(state, "target #{inspect(state.id)} returned HTTP #{status}")

  defp handle_result(state, {:error, reason}),
    do: mark_failure(state, "scrape failed for target #{inspect(state.id)}: #{inspect(reason)}")

  defp handle_result(state, _), do: mark_failure(state, "scrape returned an invalid response")

  defp mark_failure(state, message) do
    Logger.warning(message)
    put_status(state, true)
    state
  end

  defp put_initial_status(state),
    do: :ets.insert_new(state.status_table, {state.id, false})

  defp put_status(state, failing), do: :ets.insert(state.status_table, {state.id, failing})

  defp fetch(target, opts) do
    :inets.start()

    case :httpc.request(
           :get,
           {String.to_charlist(target.url), []},
           [
             {:timeout, interval_ms(opts[:receive_timeout])},
             {:connect_timeout, interval_ms(opts[:receive_timeout])}
           ],
           [{:body_format, :binary}]
         ) do
      {:ok, {{_version, status, _reason}, _headers, body}} -> {:ok, status, body}
      {:error, reason} -> {:error, reason}
    end
  end

  defp interval_ms(value) when is_float(value), do: trunc(value * 1_000)
  defp interval_ms(value), do: value * 1_000
  defp positive_number?(value), do: (is_integer(value) or is_float(value)) and value > 0
end

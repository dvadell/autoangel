defmodule Autoangel.Ingest.Metrics do
  @moduledoc "Counters for ingestion admission outcomes."
  @key {__MODULE__, :load_shed}
  def increment_load_shed(count) when is_integer(count) and count >= 0 do
    :atomics.add(counter(), 1, count)
    :ok
  end

  def load_shed, do: :atomics.get(counter(), 1)

  defp counter do
    case :persistent_term.get(@key, nil) do
      nil ->
        value = :atomics.new(1, signed: true)
        :persistent_term.put(@key, value)
        value

      value ->
        value
    end
  end
end

defmodule Autoangel.Ingest do
  # credo:disable-for-this-file Credo.Check.Refactor.Nesting
  @moduledoc "Prometheus push endpoint admission and dispatch pipeline."
  import Plug.Conn
  alias Autoangel.Exposition
  alias Autoangel.Exposition.Sample
  alias Autoangel.{Ingest.Metrics, SeriesRegistry, Workers}
  @default_max_body_size 10 * 1024 * 1024

  def init(opts), do: opts

  def call(%Plug.Conn{method: "POST", request_path: "/api/v1/push"} = conn, opts) do
    config = Keyword.get(opts, :config, Application.get_env(:autoangel_core, :config, %{}))

    max_size =
      Keyword.get(
        opts,
        :max_body_size,
        get_in(config, [:ingest, :max_body_size]) || @default_max_body_size
      )

    with {:ok, body, conn} <- read_body_limited(conn, max_size),
         {:ok, families} <- Exposition.parse(body),
         {:ok, result} <- process_samples(flatten(families), opts) do
      send_json(conn, 202, result)
    else
      {:error, :too_large, conn} ->
        send_resp(conn, 413, "request body too large")

      {:error, %{line: line, reason: reason}} ->
        send_resp(conn, 400, "invalid exposition at line #{line}: #{reason}")

      {:error, :overloaded} ->
        send_resp(conn, 503, "engine overloaded")
    end
  end

  def call(%Plug.Conn{method: "POST"} = conn, _opts), do: send_resp(conn, 404, "not found")
  def call(conn, _opts), do: send_resp(conn, 404, "not found")

  @spec process_samples([Sample.t()], keyword()) :: {:ok, map()} | {:error, :overloaded}
  def process_samples(samples, opts \\ []) when is_list(samples) do
    receive_time = Keyword.get(opts, :receive_time, System.system_time(:millisecond))
    samples = Enum.map(samples, &stamp(&1, receive_time))
    grouped = Enum.group_by(samples, &worker_index(&1, opts))

    case reserve_all(grouped, opts) do
      {:ok, reservations} ->
        case safely_resolve(samples, opts) do
          {:ok, {accepted, rejected, accepted_by_worker, rejected_by_worker}} ->
            release_rejected(rejected_by_worker, reservations)
            enqueue_workers(accepted_by_worker, opts)
            enqueue_storage(accepted, opts)
            if accepted != [], do: notify_self_monitor(opts, :accepted_sample)
            Autoangel.Metrics.increment(:ingest_samples_accepted, length(accepted))
            {:ok, %{accepted: length(accepted), rejected_cardinality: rejected}}

          {:error, _reason} ->
            release_all(reservations)
            Metrics.increment_load_shed(length(samples))
            Autoangel.Metrics.increment(:samples_load_shed, length(samples))
            {:error, :overloaded}
        end

      :error ->
        Metrics.increment_load_shed(length(samples))
        Autoangel.Metrics.increment(:samples_load_shed, length(samples))
        {:error, :overloaded}
    end
  end

  defp safely_resolve(samples, opts) do
    {:ok, resolve_samples(samples, opts)}
  rescue
    _exception -> {:error, :admission_failed}
  catch
    _kind, _reason -> {:error, :admission_failed}
  end

  defp read_body_limited(conn, max_size) do
    case Plug.Conn.read_body(conn, length: max_size + 1) do
      {:ok, body, read_conn} when byte_size(body) <= max_size -> {:ok, body, read_conn}
      {:ok, _body, read_conn} -> {:error, :too_large, read_conn}
      {:more, body, next_conn} when byte_size(body) > max_size -> {:error, :too_large, next_conn}
      {:more, body, next_conn} -> read_more(next_conn, body, max_size)
      {:error, _reason} -> {:error, :too_large, conn}
    end
  end

  defp read_more(conn, body, max_size) do
    case Plug.Conn.read_body(conn, length: max_size + 1) do
      {:ok, chunk, next_conn} when byte_size(body) + byte_size(chunk) <= max_size ->
        {:ok, body <> chunk, next_conn}

      {:ok, _chunk, next_conn} ->
        {:error, :too_large, next_conn}

      {:more, chunk, next_conn} when byte_size(body) + byte_size(chunk) > max_size ->
        {:error, :too_large, next_conn}

      {:more, chunk, next_conn} ->
        read_more(next_conn, body <> chunk, max_size)

      {:error, _reason} ->
        {:error, :too_large, conn}
    end
  end

  defp reserve_all(grouped, opts) do
    capacity = Keyword.get(opts, :queue_capacity, 20_000)

    Enum.reduce_while(grouped, {:ok, []}, fn {index, samples}, {:ok, reserved} ->
      pid = worker(index, opts)

      if is_pid(pid) and Workers.Worker.try_reserve(index, length(samples), capacity) == :ok do
        {:cont, {:ok, [{index, pid, length(samples)} | reserved]}}
      else
        Enum.each(reserved, fn {reserved_index, _reserved_pid, count} ->
          Workers.Worker.release_reserved(reserved_index, count)
        end)

        {:halt, :error}
      end
    end)
  end

  defp resolve_samples(samples, opts) do
    Enum.reduce(samples, {[], 0, %{}, %{}}, fn sample,
                                               {accepted, rejected, by_worker, rejected_by_worker} ->
      key = SeriesRegistry.canonical_key(sample.name, sample.labels)
      index = worker_index(sample, opts)

      case SeriesRegistry.resolve_or_create(key, sample.type_hint) do
        {:rejected, _limit} ->
          {accepted, rejected + 1, by_worker, Map.update(rejected_by_worker, index, 1, &(&1 + 1))}

        {_status, reference} ->
          sample = %{sample | series_kind: reference.kind}

          {[sample | accepted], rejected,
           Map.update(by_worker, reference.worker, [sample], &[sample | &1]), rejected_by_worker}
      end
    end)
    |> then(fn {accepted, rejected, workers, rejected_workers} ->
      {Enum.reverse(accepted), rejected, workers, rejected_workers}
    end)
  end

  defp release_rejected(rejected_by_worker, reservations) do
    Enum.each(reservations, fn {index, _pid, count} ->
      amount = min(Map.get(rejected_by_worker, index, 0), count)
      if amount > 0, do: Workers.Worker.release_reserved(index, amount)
    end)
  end

  defp release_all(reservations) do
    Enum.each(reservations, fn {index, _pid, count} ->
      Workers.Worker.release_reserved(index, count)
    end)
  end

  defp enqueue_workers(by_worker, opts) do
    Enum.each(by_worker, fn {index, samples} ->
      case worker(index, opts) do
        pid when is_pid(pid) -> Workers.Worker.enqueue(pid, Enum.reverse(samples))
        _ -> :ok
      end
    end)
  end

  defp enqueue_storage(samples, opts) do
    case Keyword.get(opts, :storage_writer) do
      nil ->
        :ok

      pid when is_pid(pid) ->
        GenServer.cast(pid, {:samples, samples})

      module when is_atom(module) ->
        if Process.whereis(module), do: send(module, {:samples, samples}), else: :ok

      _ ->
        :ok
    end
  end

  defp notify_self_monitor(opts, event) do
    case Keyword.get(opts, :self_monitor) do
      pid when is_pid(pid) -> send(pid, event)
      name when is_atom(name) -> if Process.whereis(name), do: send(name, event), else: :ok
      _ -> :ok
    end
  end

  defp worker(index, opts), do: Keyword.get(opts, :worker_fun, &Workers.worker/1).(index)

  defp worker_index(sample, opts),
    do:
      SeriesRegistry.worker_index(
        SeriesRegistry.canonical_key(sample.name, sample.labels),
        Keyword.get(opts, :pool_size, System.schedulers_online())
      )

  defp flatten(families), do: Enum.flat_map(families, & &1.samples)

  defp stamp(%Sample{timestamp: nil} = sample, time),
    do: %{sample | timestamp: time, received_at: time}

  defp stamp(%Sample{} = sample, time), do: %{sample | received_at: time}

  defp send_json(conn, status, %{accepted: accepted, rejected_cardinality: rejected}) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, ~s({"accepted":#{accepted},"rejected_cardinality":#{rejected}}))
  end
end

defmodule Autoangel.HTTP.Ingest do
  @moduledoc "Authenticated and rate-limited Engine ingest Plug."
  alias Autoangel.{Auth, Ingest, Ingest.RateLimit}
  def init(opts), do: opts

  def call(conn, opts) do
    conn = Auth.call(conn, Keyword.put(opts, :role, :ingest))

    if conn.halted,
      do: conn,
      else:
        RateLimit.call(conn, opts)
        |> then(&if(&1.halted, do: &1, else: Ingest.call(&1, opts)))
  end
end

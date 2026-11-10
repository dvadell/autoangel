defmodule Autoangel.HTTP.Control do
  @moduledoc """
  Authenticated Control_API: health, readiness, Prometheus metrics and the
  Feedback_Action endpoint `POST /api/v1/feedback` (Requirement 30).
  """

  import Plug.Conn
  alias Autoangel.Alert.PluginRunner
  alias Autoangel.{Auth, Boot, Exposition, Metrics}

  @spec init(keyword()) :: keyword()
  def init(opts), do: Auth.init(Keyword.put(opts, :role, :control_api))

  @spec call(Plug.Conn.t(), keyword()) :: Plug.Conn.t()
  def call(conn, opts) do
    conn = Auth.call(conn, Keyword.put(opts, :role, :control_api))

    if conn.halted do
      conn
    else
      route(conn, opts)
    end
  end

  defp route(%Plug.Conn{method: "GET", request_path: "/healthz"} = conn, _opts),
    do: send_resp(conn, 200, "ok\n")

  defp route(%Plug.Conn{method: "GET", request_path: "/readyz"} = conn, _opts) do
    if Boot.ready?(), do: send_resp(conn, 200, "ready\n"), else: send_resp(conn, 503, "not ready\n")
  end

  defp route(%Plug.Conn{method: "GET", request_path: "/metrics"} = conn, opts) do
    metrics = metrics_families(opts)

    conn
    |> put_resp_content_type("text/plain; version=0.0.4")
    |> send_resp(200, IO.iodata_to_binary(Exposition.print(metrics)))
  end

  defp route(%Plug.Conn{method: "POST", request_path: "/api/v1/feedback"} = conn, opts) do
    if Boot.ready?(),
      do: feedback(conn, opts),
      else: json(conn, 503, %{"error" => "engine starting"})
  end

  defp route(conn, _opts), do: send_resp(conn, 404, "not found")

  @max_feedback_body 64 * 1024

  # Body: {"action": "mute" | "this was normal", "target": {"series": {...}} |
  # {"incident_id": "..."}}. Responses: 200 applied, 400 naming the invalid
  # field, 500 when the State_Store write fails, 503 while the Engine starts.
  defp feedback(conn, opts) do
    with {:ok, body, conn} <- read_feedback_body(conn),
         {:ok, %{} = request} <- decode(body) do
      apply_feedback(conn, opts, request)
    else
      {:too_large, conn} -> json(conn, 413, %{"error" => "body too large"})
      {:error, conn} -> json(conn, 400, invalid("body"))
      _ -> json(conn, 400, invalid("body"))
    end
  end

  defp apply_feedback(conn, opts, request) do
    server = Keyword.get(opts, :feedback, Autoangel.Feedback)
    action = Map.get(request, "action")
    target = Map.get(request, "target")

    case call_feedback(server, action, target) do
      {:ok, _change} -> json(conn, 200, %{"applied" => action, "target" => target})
      {:error, :invalid, field} -> json(conn, 400, invalid(Atom.to_string(field)))
      {:error, :store_failed} -> json(conn, 500, %{"error" => "store_failed"})
      :unavailable -> json(conn, 503, %{"error" => "engine starting"})
    end
  end

  defp call_feedback(server, action, target) do
    Autoangel.Feedback.apply(server, action, target)
  catch
    :exit, _reason -> :unavailable
  end

  defp read_feedback_body(conn) do
    case Plug.Conn.read_body(conn, length: @max_feedback_body) do
      {:ok, body, conn} -> {:ok, body, conn}
      {:more, _partial, conn} -> {:too_large, conn}
      {:error, _reason} -> {:error, conn}
    end
  end

  defp decode(body) do
    case JSON.decode(body) do
      {:ok, value} -> {:ok, value}
      {:error, _} -> :error
    end
  end

  defp invalid(field), do: %{"error" => "invalid_field", "field" => field}

  defp json(conn, status, body) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, JSON.encode!(body))
  end

  defp metrics_families(opts) do
    Metrics.ensure_started()
    base = Metrics.families()
    base = maybe_add_ingest_rate(base, opts)
    base = maybe_add_series(base, opts)
    base = maybe_add_storage(base, opts)
    base = maybe_add_plugins(base, opts)
    group_families(base)
  end

  defp group_families(families) do
    families
    |> Enum.reduce(%{}, fn family, acc ->
      Map.update(acc, family.name, family, fn existing ->
        %{existing | samples: merge_samples(existing.samples, family.samples)}
      end)
    end)
    |> Map.values()
    |> Enum.sort_by(& &1.name)
  end

  defp merge_samples(existing, additions) do
    seen = MapSet.new(existing, &{&1.name, &1.labels})

    Enum.reduce(additions, {existing, seen}, fn sample, {samples, keys} ->
      key = {sample.name, sample.labels}

      if MapSet.member?(keys, key),
        do: {samples, keys},
        else: {samples ++ [sample], MapSet.put(keys, key)}
    end)
    |> elem(0)
  end

  defp maybe_add_ingest_rate(families, _opts) do
    add_gauge(
      families,
      "autoangel_ingest_rate_samples_per_second",
      Metrics.rate(:ingest_samples_accepted)
    )
  end

  defp maybe_add_series(families, _opts) do
    default = safe_call(fn -> Autoangel.SeriesRegistry.metrics() end) || %{}
    value = Metrics.gauge(:series_count)
    value = if value > 0, do: value, else: get_in_safe(default, [:series], 0)
    add_gauge(families, "autoangel_series", value)
  end

  defp maybe_add_storage(families, _opts) do
    families
    |> add_gauge("autoangel_storage_queue_depth", Metrics.gauge(:storage_queue_depth))
    |> add_counter(
      "autoangel_storage_operations_failed_total",
      Metrics.counter({:storage, :retryable}) + Metrics.counter({:storage, :permanent})
    )
    |> add_counter("autoangel_storage_writes_total", Metrics.counter({:storage, :successful}), %{
      "outcome" => "success"
    })
    |> add_counter("autoangel_storage_writes_total", Metrics.counter({:storage, :retryable}), %{
      "outcome" => "retryable"
    })
    |> add_counter("autoangel_storage_writes_total", Metrics.counter({:storage, :permanent}), %{
      "outcome" => "permanent"
    })
    |> add_counter("autoangel_samples_dropped_total", Metrics.counter(:samples_dropped))
  end

  defp maybe_add_plugins(families, opts) do
    runners = Keyword.get(opts, :plugin_runners, %{})
    configured = Keyword.get(opts, :plugins, %{})

    configured
    |> plugin_names()
    |> Enum.reduce(runners, fn plugin, acc -> Map.put_new(acc, plugin, nil) end)
    |> Enum.reduce(families, fn {plugin, runner}, acc ->
      stats = if runner, do: plugin_stats(runner), else: %{}
      add_plugin_outcomes(acc, plugin, stats || %{})
    end)
  end

  defp plugin_names(plugins) when is_map(plugins), do: Map.keys(plugins)
  defp plugin_names(plugins) when is_list(plugins), do: plugins
  defp plugin_names(_plugins), do: []

  defp plugin_stats(runner), do: safe_call(fn -> PluginRunner.metrics(runner) end)

  defp add_plugin_outcomes(families, plugin, stats) do
    Enum.reduce([:success, :retryable, :permanent], families, fn outcome, result ->
      add_counter(result, "autoangel_plugin_deliveries_total", Map.get(stats, outcome, 0), %{
        "plugin" => plugin_name(plugin),
        "outcome" => Atom.to_string(outcome)
      })
    end)
  end

  defp add_gauge(families, name, value), do: add_metric(families, name, :gauge, value, %{})

  defp add_counter(families, name, value, labels \\ %{}),
    do: add_metric(families, name, :counter, value, labels)

  defp add_metric(families, name, type, value, labels) do
    families ++
      [
        %Exposition.Family{
          name: name,
          type: type,
          samples: [%Exposition.Sample{name: name, labels: labels, value: value * 1.0}]
        }
      ]
  end

  defp get_in_safe(map, keys, default), do: get_in(map, keys) || default

  defp safe_call(fun) do
    fun.()
  rescue
    _ -> nil
  catch
    _, _ -> nil
  end

  defp plugin_name(plugin) when is_atom(plugin) do
    name = Atom.to_string(plugin)

    if String.starts_with?(name, "Elixir."),
      do: plugin |> Module.split() |> List.last() |> Macro.underscore(),
      else: name
  end

  defp plugin_name(plugin), do: to_string(plugin)
end

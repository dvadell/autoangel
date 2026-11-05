defmodule Autoangel.StorageWriterTestBackend do
  @behaviour Autoangel.Storage

  def init(config), do: {:ok, config[:recipient]}
  def append_samples(pid, samples), do: send(pid, {:written, samples}) && :ok
  def write_anomalies(_pid, _items), do: :ok
  def write_series(_pid, _items), do: :ok
  def write_bands(_pid, _items), do: :ok
  def read_range(_pid, _ids, _from, _to), do: {:ok, []}
  def read_aggregates(_pid, _ids, _from, _to, _resolution), do: {:ok, []}
end

defmodule Autoangel.StorageWriterTest do
  use ExUnit.Case, async: true

  alias Autoangel.Storage.Writer

  test "gates samples until migration succeeds and writes FIFO batches" do
    {:ok, writer} =
      Writer.start_link(
        backend: Autoangel.StorageWriterTestBackend,
        backend_config: [recipient: self()],
        batch_size: 2
      )

    assert :ok = Writer.enqueue_samples(writer, [1])
    assert Writer.migration_succeeded(writer) == :ok
    assert_receive {:written, [1]}, 100
    assert :ok = Writer.enqueue_samples(writer, [2, 3])
    assert_receive {:written, [2, 3]}, 100
    assert %{successful: 2, queued: 0, ready?: true} = Writer.stats(writer)
  end

  test "drops the oldest sample when capacity is full" do
    {:ok, writer} =
      Writer.start_link(
        backend: Autoangel.StorageWriterTestBackend,
        backend_config: [recipient: self()],
        capacity: 2
      )

    Writer.migration_succeeded(writer)
    assert :ok = Writer.enqueue_samples(writer, [:a, :b, :c])
    assert_receive {:written, [:b, :c]}, 100
    assert %{dropped: 1} = Writer.stats(writer)
  end

  test "restarts writer through its own supervisor" do
    name = String.to_atom("storage-supervisor-#{System.unique_integer([:positive])}")

    writer_name = String.to_atom("storage-writer-#{System.unique_integer([:positive])}")

    {:ok, supervisor} =
      Autoangel.Storage.Supervisor.start_link(
        name: name,
        writer_name: writer_name,
        backend: Autoangel.StorageWriterTestBackend,
        backend_config: [recipient: self()]
      )

    writer = Process.whereis(writer_name)
    ref = Process.monitor(writer)
    Process.exit(writer, :kill)
    assert_receive {:DOWN, ^ref, :process, ^writer, _}
    Process.sleep(20)
    assert is_pid(Process.whereis(writer_name))
    Supervisor.stop(supervisor)
  end
end

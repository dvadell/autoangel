defmodule Autoangel.StorageTestBackend do
  @behaviour Autoangel.Storage

  @impl true
  def init(_config), do: {:ok, :handle}

  @impl true
  def append_samples(_handle, _samples), do: :ok

  @impl true
  def write_anomalies(_handle, _anomalies), do: {:retryable, :busy}

  @impl true
  def write_series(_handle, _series), do: {:permanent, :invalid}

  @impl true
  def write_bands(_handle, _bands), do: :ok

  @impl true
  def read_range(_handle, _series_ids, _from, _to), do: {:ok, []}

  @impl true
  def read_aggregates(_handle, _series_ids, _from, _to, _resolution), do: {:ok, []}
end

defmodule Autoangel.StorageTestIncompleteBackend do
  def init(_config), do: {:ok, :handle}
end

defmodule Autoangel.StorageTest do
  use ExUnit.Case, async: true

  alias Autoangel.Storage

  test "selects no backend when storage is absent or explicitly disabled" do
    assert {:ok, :none} = Storage.select_backend(%{})
    assert {:ok, :none} = Storage.select_backend(%{storage: %{backend: :none}})
    assert {:ok, :none} = Storage.select_backend(%{storage: %{backend: "none"}})
    assert {:ok, :none} = Storage.select_backend(%{storage: [backend: :none]})
    assert {:error, :invalid_storage_configuration} = Storage.select_backend(%{storage: "none"})
  end

  test "selects a configured module after validating every callback" do
    assert {:ok, Autoangel.StorageTestBackend} =
             Storage.select_backend(%{storage: %{backend: Autoangel.StorageTestBackend}})
  end

  test "rejects more than one configured backend" do
    assert {:error, :multiple_backends} =
             Storage.select_backend(%{
               storage: %{
                 backend: :none,
                 backends: [Autoangel.StorageTestBackend, Autoangel.StorageTestBackend]
               }
             })
  end

  test "rejects a backend with missing callbacks" do
    assert {:error, {:missing_callbacks, Autoangel.StorageTestIncompleteBackend, missing}} =
             Storage.select_backend(%{storage: %{backend: Autoangel.StorageTestIncompleteBackend}})

    assert {:write_bands, 2} in missing
    assert {:read_aggregates, 5} in missing
  end

  test "preserves the three backend outcomes" do
    assert :ok == Autoangel.StorageTestBackend.append_samples(:handle, [])
    assert {:retryable, :busy} == Autoangel.StorageTestBackend.write_anomalies(:handle, [])
    assert {:permanent, :invalid} == Autoangel.StorageTestBackend.write_series(:handle, [])
  end
end

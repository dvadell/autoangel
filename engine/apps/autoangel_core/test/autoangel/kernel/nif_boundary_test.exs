defmodule Autoangel.Kernel.NifBoundaryTest do
  # Dynamic dispatch is required because this harness covers every loaded NIF.
  # credo:disable-for-this-file Credo.Check.Refactor.Apply
  use ExUnit.Case, async: true

  @moduledoc """
  Boundary coverage for NIFs actually loaded by the Engine.

  M0 promoted no numeric kernel, so this suite deliberately treats an empty
  loaded-NIF inventory as the expected result. Future NIFs are discovered from
  the `:autoangel_core` application module list and must provide the boundary
  callbacks below.
  """

  @max_batch 2

  defp loaded_nifs do
    modules =
      case :application.get_key(:autoangel_core, :modules) do
        {:ok, modules} -> modules
        _ -> []
      end

    Enum.filter(modules, fn module ->
      name = Atom.to_string(module)
      String.starts_with?(name, "Elixir.Autoangel.Kernel.") and String.ends_with?(name, ".Nif")
    end)
  end

  test "the loaded NIF inventory is explicit" do
    assert loaded_nifs() == []
  end

  test "each loaded NIF accepts valid input and returns an Elixir term" do
    for module <- loaded_nifs() do
      assert function_exported?(module, :scaffold, 0),
             "#{inspect(module)} must expose the valid-input boundary callback"

      assert apply(module, :scaffold, []) == "ok"
    end
  end

  test "invalid boundary inputs are guarded when a NIF is loaded" do
    for module <- loaded_nifs() do
      assert function_exported?(module, :boundary_probe, 2),
             "#{inspect(module)} must expose malformed, oversized, and wrong-type boundary coverage"

      assert {:error, _reason} = boundary_call(module, :malformed_state)
      assert {:error, _reason} = boundary_call(module, :oversized_batch, @max_batch + 1)
      assert {:error, _reason} = boundary_call(module, :wrong_type)
    end
  end

  test "a NIF panic is isolated when the test-panic NIF is loaded" do
    for module <- loaded_nifs() do
      assert function_exported?(module, :panic_sentinel, 0),
             "#{inspect(module)} must expose the test-panic sentinel when loaded for boundary tests"

      assert {:error, _reason} = catch_nif_call(module, :panic_sentinel, [])
      assert Process.alive?(self())
      assert is_binary(apply(module, :scaffold, []))
    end
  end

  test "golden comparison is intentionally empty without promoted numeric kernels" do
    assert loaded_nifs() == []
    assert File.dir?(native_fixture_dir())
    assert native_tolerance_policy() == :not_applicable
  end

  defp boundary_call(module, :malformed_state), do: apply(module, :boundary_probe, [<<0>>, []])

  defp boundary_call(module, :oversized_batch, size),
    do: apply(module, :boundary_probe, [valid_state(), List.duplicate(valid_sample(), size)])

  defp boundary_call(module, :wrong_type),
    do: apply(module, :boundary_probe, [:not_a_state, :not_a_batch])

  defp catch_nif_call(module, function, args) do
    apply(module, function, args)
  catch
    :error, reason -> {:error, reason}
    :exit, reason -> {:error, reason}
  end

  defp valid_state, do: <<"AANG1">>
  defp valid_sample, do: {1_000, 1.0, :gauge}

  defp native_fixture_dir do
    Path.expand("../../../../../native/autoangel_kernels/tests/data", __DIR__)
  end

  defp native_tolerance_policy do
    if loaded_nifs() == [], do: :not_applicable, else: :documented_per_kernel
  end
end

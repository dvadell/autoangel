defmodule AutoangelTsdb.SchemaVersionCheckTest do
  use ExUnit.Case, async: true

  alias AutoangelTsdb.Migrations
  alias Mix.Tasks.Autoangel.SchemaVersionCheck

  test "the Engine uses the Schema_Version declared in the shared library" do
    assert Migrations.version() == AutoangelSchema.version()
    assert Migrations.version() in AutoangelSchema.supported_versions()
  end

  test "the declared version equals the newest migration" do
    assert :ok = Migrations.check_version()
  end

  test "a mismatch reports both values" do
    assert {:error, %{declared: 4, newest_migration: 3}} = Migrations.check_version(4, 3)
  end

  test "the mix task passes on the current tree" do
    assert SchemaVersionCheck.check() == :ok
  end
end

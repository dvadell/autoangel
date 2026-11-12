defmodule Mix.Tasks.Autoangel.SchemaVersionCheck do
  @shortdoc "Fails when the shared Schema_Version differs from the newest migration"
  @moduledoc """
  CI check: the Schema_Version declared in `AutoangelSchema` must equal the
  version of the newest migration in `AutoangelTsdb.Migrations`. On a mismatch
  it prints both values and exits non-zero.

      mix autoangel.schema_version_check
  """
  use Mix.Task

  @impl Mix.Task
  def run(_args) do
    Mix.Task.run("compile")

    case AutoangelTsdb.Migrations.check_version() do
      :ok ->
        Mix.shell().info("schema version check: ok (#{AutoangelTsdb.Migrations.version()})")

      {:error, %{declared: declared, newest_migration: newest}} ->
        Mix.raise(
          "schema version check failed: AutoangelSchema declares #{declared}, " <>
            "newest migration is #{newest}"
        )
    end
  end
end

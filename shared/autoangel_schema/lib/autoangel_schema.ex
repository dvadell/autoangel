defmodule AutoangelSchema do
  @moduledoc """
  Shared schema contract between the Engine (writer) and the Console (reader).

  The Schema_Version is the version of the tables the Engine migrates (see
  `AutoangelTsdb.Migrations`). The Console refuses to start against a version
  it does not list in `supported_versions/0`.
  """

  @schema_version 3
  @supported_versions [3]

  @doc "The Schema_Version this library describes."
  @spec version() :: 3
  def version, do: @schema_version

  @doc "Every Schema_Version a Console built with this library can read."
  @spec supported_versions() :: [3, ...]
  def supported_versions, do: @supported_versions
end

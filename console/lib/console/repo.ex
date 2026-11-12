defmodule Console.Repo do
  @moduledoc """
  The Console's only database handle. It is read-only twice over: Ecto refuses
  writes (`read_only: true`) and every connection asks PostgreSQL for
  `default_transaction_read_only`. A database role without write grants (task 28)
  is the third layer.
  """
  use Ecto.Repo,
    otp_app: :autoangel_console,
    adapter: Ecto.Adapters.Postgres,
    read_only: true

  @doc "Repo options for a database URL; nil keeps the Repo idle."
  @spec options(String.t()) :: keyword()
  def options(url) do
    [
      url: url,
      pool_size: 10,
      parameters: [default_transaction_read_only: "on"],
      queue_target: 500,
      queue_interval: 1_000
    ]
  end
end

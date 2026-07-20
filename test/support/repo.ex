defmodule Capstan.Test.Repo do
  use Ecto.Repo,
    otp_app: :capstan,
    adapter: Application.compile_env(:capstan, :test_adapter, Ecto.Adapters.SQLite3)
end

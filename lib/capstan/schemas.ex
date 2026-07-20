defmodule Capstan.Step do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  schema "capstan_steps" do
    field :job_id, :integer
    field :name, :string
    field :attempt, :integer, default: 0
    field :result, :binary
    field :inserted_at, :utc_datetime_usec
  end
end

defmodule Capstan.Signal do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  schema "capstan_signals" do
    field :scope, :string
    field :name, :string
    field :payload, :map
    field :inserted_at, :utc_datetime_usec
  end
end

defmodule Capstan.RateWindow do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  schema "capstan_rate" do
    field :queue, :string
    field :resource, :string, default: ""
    field :window_start, :integer
    field :count, :integer, default: 0
  end
end

defmodule Capstan.CronEntry do
  @moduledoc false
  use Ecto.Schema

  @primary_key false
  schema "capstan_crons" do
    field :name, :string
    field :expression, :string
    field :worker, :string
    field :args, :map, default: %{}
    field :opts, :map, default: %{}
    field :timezone, :string
    field :paused, :boolean, default: false
    field :last_enqueued_at, :utc_datetime_usec
    field :inserted_at, :utc_datetime_usec
    field :updated_at, :utc_datetime_usec
  end
end

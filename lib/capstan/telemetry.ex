defmodule Capstan.Telemetry do
  @moduledoc """
  Telemetry events emitted by Capstan:

    * `[:capstan, :job, :start]` — measurements: `%{system_time}`;
      metadata: `%{job, name}`
    * `[:capstan, :job, :stop]` — measurements: `%{duration}` (native units);
      metadata: `%{job, name, state}`
    * `[:capstan, :job, :exception]` — metadata:
      `%{job, name, kind, reason, stacktrace}`

  Attach the bundled logger for structured one-line logs:

      Capstan.Telemetry.attach_default_logger(:info)
  """

  require Logger

  @events [
    [:capstan, :job, :stop],
    [:capstan, :job, :exception]
  ]

  @doc "Attach a one-line logger for job completions and crashes."
  def attach_default_logger(level \\ :info) do
    :telemetry.attach_many("capstan-default-logger", @events, &__MODULE__.handle_event/4, %{
      level: level
    })
  end

  def detach_default_logger do
    :telemetry.detach("capstan-default-logger")
  end

  @doc false
  def handle_event([:capstan, :job, :stop], measurements, meta, %{level: level}) do
    duration_ms =
      System.convert_time_unit(measurements[:duration] || 0, :native, :millisecond)

    Logger.log(
      level,
      "capstan job=#{meta.job.id} worker=#{meta.job.kind} queue=#{meta.job.queue} " <>
        "state=#{meta.state} attempt=#{meta.job.attempt} duration=#{duration_ms}ms"
    )
  end

  def handle_event([:capstan, :job, :exception], _measurements, meta, _config) do
    Logger.error(
      "capstan job=#{meta.job.id} worker=#{meta.job.kind} crashed: " <>
        Exception.format_banner(meta.kind, meta.reason, meta[:stacktrace] || [])
    )
  end
end

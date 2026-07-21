defmodule Belay.Telemetry do
  @moduledoc """
  Telemetry events emitted by Belay:

    * `[:belay, :job, :start]` — measurements: `%{system_time}`;
      metadata: `%{job, name}`
    * `[:belay, :job, :stop]` — measurements: `%{duration}` (native units);
      metadata: `%{job, name, state}`
    * `[:belay, :job, :exception]` — metadata:
      `%{job, name, kind, reason, stacktrace}`

  Attach the bundled logger for structured one-line logs:

      Belay.Telemetry.attach_default_logger(:info)
  """

  require Logger

  @events [
    [:belay, :job, :stop],
    [:belay, :job, :exception]
  ]

  @doc "Attach a one-line logger for job completions and crashes."
  def attach_default_logger(level \\ :info) do
    :telemetry.attach_many("belay-default-logger", @events, &__MODULE__.handle_event/4, %{
      level: level
    })
  end

  def detach_default_logger do
    :telemetry.detach("belay-default-logger")
  end

  @doc false
  def handle_event([:belay, :job, :stop], measurements, meta, %{level: level}) do
    duration_ms =
      System.convert_time_unit(measurements[:duration] || 0, :native, :millisecond)

    Logger.log(
      level,
      "belay job=#{meta.job.id} worker=#{meta.job.kind} queue=#{meta.job.queue} " <>
        "state=#{meta.state} attempt=#{meta.job.attempt} duration=#{duration_ms}ms"
    )
  end

  def handle_event([:belay, :job, :exception], _measurements, meta, _config) do
    Logger.error(
      "belay job=#{meta.job.id} worker=#{meta.job.kind} crashed: " <>
        Exception.format_banner(meta.kind, meta.reason, meta[:stacktrace] || [])
    )
  end
end

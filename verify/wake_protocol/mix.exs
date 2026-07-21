defmodule WakeProtocol.MixProject do
  use Mix.Project

  # Standalone harness so Belay's own dependency tree stays untouched:
  # this project models the parent-wake protocol abstractly and never
  # imports Belay code.
  def project do
    [
      app: :wake_protocol,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: false,
      deps: [{:lockstep, "~> 0.1.0"}]
    ]
  end

  def application, do: [extra_applications: []]
end

defmodule Capstan.MixProject do
  use Mix.Project

  @version "0.2.0"
  @source_url "https://github.com/bariserdem/capstan"

  def project do
    [
      app: :capstan,
      version: @version,
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "Capstan",
      description:
        "A standalone, agent-native durable job engine for Elixir on Postgres: " <>
          "memoized steps with cost budgets, signals, workflows, leases, and leaderless scheduling.",
      package: package(),
      docs: [main: "Capstan", source_url: @source_url]
    ]
  end

  def application do
    [extra_applications: [:logger, :crypto]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:postgrex, "~> 0.17"},
      {:jason, "~> 1.4"},
      {:telemetry, "~> 1.2"},
      {:ex_doc, "~> 0.31", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      licenses: ["Apache-2.0"],
      links: %{"GitHub" => @source_url}
    ]
  end
end

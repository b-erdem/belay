defmodule Capstan.MixProject do
  use Mix.Project

  @version "1.0.0-rc.4"
  @source_url "https://github.com/b-erdem/capstan"

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
        "An agent-native durable job engine for Elixir on Postgres: memoized steps " <>
          "with cost budgets, signals, dynamic child jobs, event streams, replay " <>
          "debugging, token-aware rate limits, and an MCP server — leaderless, no Ecto.",
      package: package(),
      docs: docs()
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: @source_url,
      extras: [
        "README.md",
        "guides/getting-started.md",
        "guides/durable-steps.md",
        "guides/agents.md",
        "guides/operations.md",
        "guides/testing.md",
        "guides/comparison.md",
        "DESIGN.md",
        "SCHEMA.md",
        "docs/ZERO_TO_ONE.md",
        "CONTRIBUTING.md",
        "CHANGELOG.md"
      ],
      groups_for_extras: [Guides: ~r/guides\/.*/]
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
      links: %{"GitHub" => @source_url},
      files: ~w(lib guides mix.exs README.md DESIGN.md SCHEMA.md CHANGELOG.md LICENSE)
    ]
  end
end

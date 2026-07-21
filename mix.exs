defmodule Belay.MixProject do
  use Mix.Project

  @version "1.0.0-rc.5"
  @source_url "https://github.com/b-erdem/belay"

  def project do
    [
      app: :belay,
      version: @version,
      elixir: "~> 1.15",
      elixirc_paths: elixirc_paths(Mix.env()),
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      name: "Belay",
      description:
        "Durable execution for Elixir on Postgres: step-resumable jobs, workflows, " <>
          "budgets, signals, and replay — without a separate workflow server.",
      package: package(),
      docs: docs()
    ]
  end

  defp docs do
    [
      main: "readme",
      source_url: @source_url,
      assets: %{"assets" => "assets"},
      extras: [
        "README.md",
        "guides/getting-started.md",
        "guides/migrating-from-oban.md",
        "guides/durable-steps.md",
        "guides/agents.md",
        "guides/operations.md",
        "guides/testing.md",
        "guides/comparison.md",
        {"verify/README.md", filename: "formal-verification", title: "Formal verification"},
        "DESIGN.md",
        "SCHEMA.md",
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
      # The formal-verification harness (verify/) lives in the repo, not the
      # published package — it is evidence, not runtime code, and shipping a
      # nested mix project inside a library tarball confuses tooling.
      files: ~w(lib guides assets mix.exs README.md DESIGN.md SCHEMA.md CHANGELOG.md
           CONTRIBUTING.md SECURITY.md LICENSE)
    ]
  end
end

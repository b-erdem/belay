defmodule Capstan.MixProject do
  use Mix.Project

  @version "0.1.0"
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
      description: "Agent-native pro toolkit for Oban: durable steps, signals, workflows, batches, chains, relay, and a smart engine with global and rate limits.",
      package: package(),
      docs: [main: "Capstan", source_url: @source_url]
    ]
  end

  def application do
    [extra_applications: [:logger]]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(_), do: ["lib"]

  defp deps do
    [
      {:oban, "~> 2.19"},
      {:ecto_sql, "~> 3.10"},
      {:jason, "~> 1.4"},
      {:ecto_sqlite3, "~> 0.16", only: [:dev, :test]},
      {:postgrex, "~> 0.17", only: [:dev, :test]},
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

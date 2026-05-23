defmodule Kylix.MixProject do
  use Mix.Project

  def project do
    [
      app: :kylix,
      version: "0.1.0",
      elixir: "~> 1.18",
      dialyzer: [plt_add_apps: [:mix, :ex_unit]],
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger, :observer, :wx, :runtime_tools],
      # :observer is used for monitoring the application
      # :logger is used for logging
      # :kylix is the main application module
      mod: {Kylix.Application, []}
    ]
  end

  defp deps do
    [
      # use for json serialization
      {:jason, "~> 1.4"},
      # use for api server
      {:plug_cowboy, "~> 2.6"},
      # use for graphql server
      {:absinthe, "~> 1.7.8"},
      # use for graphql server
      {:absinthe_plug, "~> 1.5.8"},
      # use for rdf graph
      {:rdf, "~> 2.0"},
      # use for sparql
      {:sparql, "~> 0.3.10"},
      # use for encryption
      {:ex_crypto, "~> 0.10"},
      # use for monitoring
      {:telemetry, "~> 1.2"},
      # use for monitoring
      {:telemetry_metrics, "~> 0.6"},
      # use for code analysis
      {:credo, "~> 1.7", only: [:dev, :test]},
      # use for static analysis
      {:dialyxir, "~> 1.3", only: [:dev], runtime: false},
      # use for documentation
      {:ex_doc, "~> 0.29", only: :dev, runtime: false},
      # use for mocking
      {:meck, "~> 1.0.0", only: :test},
      # use for parsing
      {:nimble_parsec, "~> 1.4.2"}
    ]
  end
end

defmodule Kylix.MixProject do
  use Mix.Project

  def project do
    [
      app: :kylix,
      version: "0.1.0",
      elixir: "~> 1.18",
      dialyzer: [plt_add_apps: [:mix, :ex_unit]],
      start_permanent: Mix.env() == :prod,
      deps: deps(),
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
      {:jason, "~> 1.4"},               # use for json serialization
      {:plug_cowboy, "~> 2.6"},         # use for api server
      {:absinthe, "~> 1.7.8"},          # use for graphql server
      {:absinthe_plug, "~> 1.5.8"},     # use for graphql server
      {:rdf, "~> 2.0"},                 # use for rdf graph
      {:sparql, "~> 0.3.10"},           # use for sparql
      {:ex_crypto, "~> 0.10"},          # use for encryption
      {:telemetry, "~> 1.2"},           # use for monitoring
      {:telemetry_metrics, "~> 0.6"},   # use for monitoring
      {:credo, "~> 1.7", only: [:dev, :test]},              #use for code analysis
      {:dialyxir, "~> 1.3", only: [:dev], runtime: false},  #use for static analysis
      {:ex_doc, "~> 0.29", only: :dev, runtime: false},     #use for documentation
      {:meck, "~> 1.0.0", only: :test},                    #use for mocking
      {:nimble_parsec, "~> 1.4.2"}       # use for parsing
    ]
  end
end

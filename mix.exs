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
      extra_applications: [:logger],
      mod: {Kylix.Application, []}
    ]
  end

  defp deps do
    [
      {:jason, "~> 1.4"},
      {:plug_cowboy, "~> 2.6"},
      {:absinthe, "~> 1.7.8"},
      {:absinthe_plug, "~> 1.5.8"},
      {:rdf, "~> 2.0"},
      {:sparql, "~> 0.3.10"},
      {:ex_crypto, "~> 0.10"},
      {:telemetry, "~> 1.2"},
      {:telemetry_metrics, "~> 0.6"},
      {:credo, "~> 1.7", only: [:dev, :test]},
      {:dialyxir, "~> 1.3", only: [:dev], runtime: false},
      {:ex_doc, "~> 0.29", only: :dev, runtime: false}
    ]
  end
end

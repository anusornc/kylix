defmodule Kylix.Eval.Suite do
  @moduledoc """
  Lineage-suite query files and expected bindings under `eval/`.
  """

  @queries [
    "generated-by",
    "used",
    "derived-from",
    "derived-from-chain",
    "attributed-to",
    "activity-outputs",
    "count"
  ]

  def names, do: @queries

  def query(name) when is_binary(name) do
    File.read!(path(["eval", "queries", "#{name}.sparql"]))
  end

  def expected(name) when is_binary(name) do
    ["eval", "expected", "#{name}.json"]
    |> path()
    |> File.read!()
    |> Jason.decode!()
  end

  defp path(segments), do: Path.expand(Path.join(segments))
end

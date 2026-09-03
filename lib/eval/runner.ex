defmodule Kylix.Eval.Runner do
  @moduledoc """
  Loads Fig1 through the public record API, runs the lineage suite, and diffs
  expected bindings.
  """

  alias Kylix.Query.SparqlEngine

  @doc """
  Records Fig1, executes each lineage query, and diffs `eval/expected/*.json`.

  Returns `{:ok, rows}` when every query matches, otherwise `{:error, rows}`.
  """
  def run(opts) when is_list(opts) do
    with {:ok, 15} <- Kylix.Eval.Fig1.record(opts) do
      rows = Enum.map(Kylix.Eval.Suite.names(), &diff_query/1)
      IO.puts(format_table(rows))

      if Enum.all?(rows, & &1.match?) do
        {:ok, rows}
      else
        {:error, rows}
      end
    end
  end

  defp diff_query(name) do
    query = Kylix.Eval.Suite.query(name)
    expected = Kylix.Eval.Suite.expected(name)
    {:ok, kylix} = SparqlEngine.execute(query)

    %{name: name, kylix: kylix, expected: expected, match?: kylix == expected}
  end

  defp format_table(rows) do
    header = "| Query | Kylix | Fabric (on-chain) |"
    sep = "| --- | --- | --- |"

    body =
      Enum.map(rows, fn row ->
        "| #{row.name} | #{format_kylix(row.kylix)} | not answerable |"
      end)

    Enum.join([header, sep | body], "\n")
  end

  defp format_kylix(bindings) do
    Enum.map_join(bindings, "; ", fn row ->
      row
      |> Enum.sort_by(fn {k, _v} -> k end)
      |> Enum.map_join(", ", fn {_k, v} -> to_string(v) end)
    end)
  end
end

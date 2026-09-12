defmodule Kylix.Query.SparqlEngine.Join do
  @moduledoc false

  alias Kylix.Query.SparqlEngine.Count

  def execute(query) do
    rows =
      query.patterns
      |> bgp()
      |> Count.apply_aggregations(query.aggregates, query.group_by)

    project(rows, query.variables)
  end

  defp bgp(patterns) do
    Enum.reduce(patterns, [%{}], fn pattern, solutions ->
      Enum.flat_map(solutions, fn solution ->
        {s, p, o} = {
          bound(pattern.s, solution),
          bound(pattern.p, solution),
          bound(pattern.o, solution)
        }

        case Kylix.Storage.query({s, p, o}) do
          {:ok, results} ->
            results
            |> Enum.map(&triple_data/1)
            |> Enum.reject(&is_nil/1)
            |> Enum.map(&merge_triple(solution, pattern, &1))
            |> Enum.reject(&is_nil/1)

          _ ->
            []
        end
      end)
    end)
  end

  defp bound("?" <> var, solution), do: Map.get(solution, var)
  defp bound(literal, _solution), do: literal

  defp triple_data({_id, data, _edges}) when is_map(data) do
    if Map.has_key?(data, :subject) and Map.has_key?(data, :predicate) and
         Map.has_key?(data, :object) do
      data
    else
      nil
    end
  end

  defp triple_data(_), do: nil

  defp merge_triple(solution, pattern, data) do
    [
      {:s, data.subject},
      {:p, data.predicate},
      {:o, data.object}
    ]
    |> Enum.reduce_while(solution, fn {key, value}, acc ->
      case Map.fetch!(pattern, key) do
        "?" <> var ->
          case Map.fetch(acc, var) do
            :error -> {:cont, Map.put(acc, var, value)}
            {:ok, ^value} -> {:cont, acc}
            {:ok, _} -> {:halt, nil}
          end

        _ ->
          {:cont, acc}
      end
    end)
  end

  defp project(rows, variables) do
    {:ok,
     Enum.map(rows, fn row ->
       Map.new(variables, fn var -> {var, Map.get(row, var)} end)
     end)}
  end
end

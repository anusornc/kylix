defmodule Kylix.Query.SparqlEngine.Count do
  @moduledoc false

  def apply_aggregations(rows, [], _group_by), do: rows

  def apply_aggregations(rows, aggregates, group_by) do
    groups =
      if group_by == [] do
        [rows]
      else
        rows
        |> Enum.group_by(fn row -> Enum.map(group_by, &Map.get(row, &1)) end)
        |> Map.values()
      end

    Enum.map(groups, fn group_rows ->
      base =
        case {group_by, group_rows} do
          {[], _} -> %{}
          {_vars, []} -> %{}
          {vars, [first | _]} -> Map.new(vars, fn v -> {v, Map.get(first, v)} end)
        end

      Enum.reduce(aggregates, base, fn agg, acc ->
        n =
          group_rows
          |> Enum.map(&Map.get(&1, agg.variable))
          |> Enum.reject(&is_nil/1)
          |> length()

        Map.put(acc, agg.alias, n)
      end)
    end)
  end
end

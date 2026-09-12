defmodule Kylix.Query.SparqlEngine.Parse do
  @moduledoc false

  import NimbleParsec

  def add_prov_prefix(id), do: "prov:" <> id

  whitespace = ascii_string([?\s, ?\n, ?\r, ?\t], min: 1)
  optional_whitespace = ascii_string([?\s, ?\n, ?\r, ?\t], min: 0)

  ident = ascii_string([?a..?z, ?A..?Z, ?0..?9, ?_], min: 1)

  variable =
    ignore(string("?"))
    |> concat(ident)
    |> unwrap_and_tag(:variable)

  quoted =
    ignore(string("\""))
    |> ascii_string([not: ?"], min: 1)
    |> ignore(string("\""))
    |> unwrap_and_tag(:literal)

  prov =
    ignore(string("prov:"))
    |> concat(ident)
    |> map({__MODULE__, :add_prov_prefix, []})
    |> unwrap_and_tag(:prov)

  node = choice([variable, quoted, prov])

  triple_pattern =
    node
    |> ignore(whitespace)
    |> concat(node)
    |> ignore(whitespace)
    |> concat(node)
    |> ignore(optional_whitespace)
    |> optional(ignore(string(".")))
    |> ignore(optional_whitespace)
    |> tag(:triple)

  aggregate =
    ignore(string("("))
    |> ignore(string("COUNT"))
    |> ignore(string("("))
    |> concat(variable)
    |> ignore(string(")"))
    |> ignore(whitespace)
    |> ignore(string("AS"))
    |> ignore(whitespace)
    |> concat(variable)
    |> ignore(string(")"))
    |> tag(:aggregate)

  select_item =
    choice([aggregate, variable])
    |> ignore(optional_whitespace)

  select_clause =
    ignore(string("SELECT"))
    |> ignore(whitespace)
    |> times(select_item, min: 1)
    |> tag(:select)

  where_clause =
    ignore(string("WHERE"))
    |> ignore(whitespace)
    |> ignore(string("{"))
    |> ignore(optional_whitespace)
    |> times(triple_pattern, min: 1)
    |> ignore(optional_whitespace)
    |> ignore(string("}"))
    |> tag(:where)

  group_by_clause =
    ignore(string("GROUP BY"))
    |> ignore(whitespace)
    |> times(
      ignore(optional_whitespace) |> concat(variable) |> ignore(optional_whitespace),
      min: 1
    )
    |> tag(:group_by)

  lineage_query =
    select_clause
    |> ignore(optional_whitespace)
    |> concat(where_clause)
    |> ignore(optional_whitespace)
    |> optional(concat(group_by_clause, ignore(optional_whitespace)))
    |> eos()

  defparsecp(:parse_query, lineage_query)

  def parse(query) do
    normalized = query |> String.trim() |> String.replace(~r/\s+/, " ")

    case parse_query(normalized) do
      {:ok, parsed, "", _, _, _} ->
        {:ok, convert(parsed)}

      {:ok, _, rest, _, _, _} ->
        {:error, "Failed to parse entire query. Stopped at: #{rest}"}

      {:error, reason, rest, _, _, _} ->
        {:error, "Parse error: #{reason}, at: #{rest}"}
    end
  end

  defp convert(parsed) do
    {variables, aggregates} = process_select(Keyword.get(parsed, :select, []))
    patterns = process_where(Keyword.get(parsed, :where, []))

    %{
      variables: variables,
      patterns: patterns,
      aggregates: aggregates,
      group_by: process_group_by(Keyword.get(parsed, :group_by))
    }
  end

  defp process_select(items) do
    Enum.reduce(items, {[], []}, fn
      {:variable, var}, {vars, aggs} ->
        {vars ++ [var], aggs}

      {:aggregate, [{:variable, var}, {:variable, alias_var}]}, {vars, aggs} ->
        agg = %{function: :count, variable: var, alias: alias_var}
        {vars ++ [alias_var], aggs ++ [agg]}

      _, acc ->
        acc
    end)
  end

  defp process_where(items) do
    Enum.flat_map(items, fn
      {:triple, [s, p, o]} ->
        [%{s: process_node(s), p: process_node(p), o: process_node(o)}]

      _ ->
        []
    end)
  end

  defp process_group_by(nil), do: []
  defp process_group_by(items), do: Enum.map(items, fn {:variable, var} -> var end)

  defp process_node({:variable, var}), do: "?" <> var
  defp process_node({:literal, value}), do: value
  defp process_node({:prov, prov}), do: prov
  defp process_node(_), do: nil
end

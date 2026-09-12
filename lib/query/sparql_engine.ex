defmodule Kylix.Query.SparqlEngine do
  @moduledoc """
  Lineage suite: ask provenance questions at execute/1.
  """

  import NimbleParsec

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

  def execute(query) do
    try do
      query = query |> ensure_utf8_encoding() |> String.trim()

      case lineage_subset(query) do
        {:error, reason} ->
          {:error, reason}

        :ok ->
          case parse_lineage(query) do
            {:ok, parsed} -> evaluate(parsed)
            {:error, reason} -> {:error, reason}
          end
      end
    rescue
      e ->
        {:error, "Query execution error: #{Exception.message(e)}"}
    end
  end

  defp lineage_subset(query) do
    cond do
      Regex.match?(~r/\bSELECT\s+\*/i, query) ->
        {:error, "SELECT * is not in the lineage suite"}

      match =
          Regex.run(
            ~r/\b(PREFIX|BASE|CONSTRUCT|DESCRIBE|ASK|OPTIONAL|UNION|FILTER|HAVING|DELETE|INSERT|DROP|LOAD|CLEAR|ORDER\s+BY|LIMIT|OFFSET|SUM|AVG|MIN|MAX|GROUP_CONCAT|DISTINCT)\b/i,
            query
          ) ->
        {:error, "#{hd(match)} is not in the lineage suite"}

      Regex.match?(~r/\w+:\w+\s*[+*]|\w+:\w+\s*\/\s*\w+:/, query) ->
        {:error, "property paths are not in the lineage suite"}

      not Regex.match?(~r/\ASELECT\b/, query) ->
        {:error, "not in the lineage suite"}

      true ->
        :ok
    end
  end

  defp parse_lineage(query) do
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
  defp process_node({:prov, local}), do: "prov:" <> local
  defp process_node(_), do: nil

  defp evaluate(query) do
    rows =
      query.patterns
      |> bgp()
      |> apply_aggregations(query.aggregates, query.group_by)

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

  defp apply_aggregations(rows, [], _group_by), do: rows

  defp apply_aggregations(rows, aggregates, group_by) do
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

  defp project(rows, variables) do
    {:ok,
     Enum.map(rows, fn row ->
       Map.new(variables, fn var -> {var, Map.get(row, var)} end)
     end)}
  end

  defp ensure_utf8_encoding(input) when is_binary(input) do
    case :unicode.characters_to_binary(input, :utf8, :utf8) do
      converted when is_binary(converted) ->
        String.replace(converted, ~r/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/, "")

      _ ->
        String.replace(input, ~r/[^\x20-\x7E\n\r\t]/, "")
    end
  end

  defp ensure_utf8_encoding(input), do: "#{input}"
end

defmodule Kylix.Query.SparqlAggregator do
  @moduledoc """
  Handles SPARQL aggregation functions for Kylix SPARQL queries.

  Supports standard SPARQL aggregation functions like COUNT, SUM, AVG, MIN, MAX, and GROUP_CONCAT
  with both regular and DISTINCT variants.
  """

  import NimbleParsec
  require Logger

  # Basic whitespace parsers
  _whitespace = ascii_string([?\s, ?\t, ?\n, ?\r], min: 1)
  optional_whitespace = ascii_string([?\s, ?\t, ?\n, ?\r], min: 0)

  # Simple parsers
  variable =
    string("?")
    |> ascii_string([?a..?z, ?A..?Z, ?0..?9, ?_], min: 1)
    |> reduce({Enum, :join, []})

  # Parse function name (COUNT, SUM, etc.)
  function =
    choice([
      string("COUNT") |> replace(:count),
      string("SUM") |> replace(:sum),
      string("AVG") |> replace(:avg),
      string("MIN") |> replace(:min),
      string("MAX") |> replace(:max),
      string("GROUP_CONCAT") |> replace(:group_concat)
    ])

  # Parse DISTINCT keyword
  distinct = string("DISTINCT") |> replace(true)

  # Parse separator for GROUP_CONCAT
  separator =
    optional_whitespace
    |> string("SEPARATOR")
    |> ignore(optional_whitespace)
    |> choice([
      ignore(string("'"))
      |> utf8_string([not: ?'], min: 0)
      |> ignore(string("'")),
      ignore(string("\""))
      |> utf8_string([not: ?"], min: 0)
      |> ignore(string("\""))
    ])

  # Parse AS clause
  as_clause =
    optional_whitespace
    |> string("AS")
    |> ignore(optional_whitespace)
    |> concat(variable)

  # Main parser for aggregate expressions
  defparsec(
    :parse_aggregate_expr,
    optional(string("("))  # Make the outer parenthesis optional for flexibility
    |> unwrap_and_tag(function, :function)
    |> ignore(string("("))
    |> optional(
      unwrap_and_tag(distinct, :distinct)
      |> ignore(optional_whitespace)
    )
    |> unwrap_and_tag(variable, :variable)
    |> optional(
      unwrap_and_tag(separator, :separator)
    )
    |> ignore(string(")"))
    |> optional(
      unwrap_and_tag(as_clause, :alias)
    )
    |> optional(string(")"))  # Make the closing parenthesis optional as well
  )

  @doc """
  Parses an aggregate expression using NimbleParsec.

  ## Examples

      iex> Kylix.Query.SparqlAggregator.parse_aggregate_expression("COUNT(?s)")
      {:ok, %{function: :count, variable: "s", distinct: false, alias: "count_s"}}

      iex> Kylix.Query.SparqlAggregator.parse_aggregate_expression("COUNT(DISTINCT ?s)")
      {:ok, %{function: :count, variable: "s", distinct: true, alias: "count_distinct_s"}}

      iex> Kylix.Query.SparqlAggregator.parse_aggregate_expression("COUNT(?s AS ?totalCount)")
      {:ok, %{function: :count, variable: "s", distinct: false, alias: "totalCount"}}

      iex> Kylix.Query.SparqlAggregator.parse_aggregate_expression("GROUP_CONCAT(?name SEPARATOR \"; \")")
      {:ok, %{function: :group_concat, variable: "name", distinct: false, options: %{separator: "; "}, alias: "group_concat_name"}}
  """
  def parse_aggregate_expression(expr) when is_binary(expr) do
    # Log the input expression for debugging
    Logger.debug("Parsing aggregate expression: #{expr}")

    case parse_aggregate_expr(expr) do
      {:ok, parsed, "", _, _, _} ->
        result = %{
          function: Keyword.get(parsed, :function),
          variable: String.replace(Keyword.get(parsed, :variable), "?", ""),
          distinct: Keyword.get(parsed, :distinct, false)
        }

        # Handle alias if provided, otherwise generate default alias
        result =
          case Keyword.get(parsed, :alias) do
            nil ->
              alias_name = case result.function do
                :count ->
                  prefix = if result.distinct, do: "count_distinct_", else: "count_"
                  prefix <> result.variable
                :group_concat -> "group_concat_" <> result.variable
                other -> "#{other}_#{result.variable}"
              end
              Map.put(result, :alias, alias_name)
            alias_value ->
              Map.put(result, :alias, String.replace(alias_value, "?", ""))
          end

        # Handle separator for group_concat
        result =
          if result.function == :group_concat do
            separator = Keyword.get(parsed, :separator, ",")
            Map.put(result, :options, %{separator: separator})
          else
            result
          end

        {:ok, result}

      {:ok, _result, rest, _, _, _} ->
        Logger.warning("Failed to parse entire aggregate expression. Remaining: #{rest}")
        {:error, "Failed to parse entire expression: #{expr}"}

      {:error, reason, _rest, _context, _line, _column} ->
        Logger.error("Error parsing aggregate expression: #{reason}")
        {:error, "Parse error: #{reason}"}
    end
  end

  @doc """
  Applies aggregation to query results according to aggregate specifications.

  ## Parameters

  - results: The raw query results
  - aggregates: List of aggregate specifications
  - group_by: List of variables to group by
  - var_positions: Optional mapping of variables to positions (s, p, o)

  ## Returns

  Aggregated result set
  """
  def apply_aggregations(results, aggregates, group_by \\ [], var_positions \\ %{}) do
    # Log inputs for debugging
    Logger.debug("Apply aggregations with:")
    Logger.debug("Results: #{inspect(results)}")
    Logger.debug("Aggregates: #{inspect(aggregates)}")
    Logger.debug("Group by: #{inspect(group_by)}")
    Logger.debug("Variable positions: #{inspect(var_positions)}")

    if Enum.empty?(aggregates) do
      # No aggregations to apply
      results
    else
      # Group results if needed
      grouped_results = if Enum.empty?(group_by) do
        # No GROUP BY, treat all results as one group
        [{"__all__", results}]
      else
        # Group by the specified variables
        groups = Enum.group_by(results, fn result ->
          Enum.map(group_by, fn var ->
            # Use variable positions to get the correct value
            position = Map.get(var_positions, var)
            value = case position do
              "s" -> Map.get(result, "s")
              "p" -> Map.get(result, "p")
              "o" -> Map.get(result, "o")
              _ -> Map.get(result, var)  # Fall back to direct lookup
            end
            value || Map.get(result, var)  # Additional fallback
          end)
        end)

        # Debug grouped results
        Logger.debug("Grouped results: #{inspect(groups)}")
        Map.to_list(groups)
      end

      # Apply aggregations to each group
      aggregated = Enum.map(grouped_results, fn {group_key, group_results} ->
        # Start with a result containing the group_by values
        base_result = if group_key == "__all__" do
          %{}
        else
          if is_list(group_key) do
            Enum.zip(group_by, group_key)
            |> Enum.into(%{})
          else
            %{Enum.at(group_by, 0) => group_key}
          end
        end

        # Add each aggregate result to the base result
        Enum.reduce(aggregates, base_result, fn agg, result ->
          # Use variable position for the aggregate variable if available
          agg_variable = agg.variable
          position = Map.get(var_positions, agg_variable)

          # If we have a position mapping, update the aggregate to use the right field
          updated_agg = if position do
            field = case position do
              "s" -> "s"
              "p" -> "p"
              "o" -> "o"
              _ -> agg_variable
            end
            Map.put(agg, :field, field)
          else
            agg
          end

          agg_value = compute_aggregate(updated_agg, group_results)

          # Store the aggregate value both in the alias name and in a special key
          result
          |> Map.put(agg.alias, agg_value)
          |> Map.put("count_#{agg.variable}", agg_value)
        end)
      end)

      # Debug the final aggregated results
      Logger.debug("Aggregated results: #{inspect(aggregated)}")
      aggregated
    end
  end

  @doc """
  Computes a single aggregate function over a group of results.
  """
  def compute_aggregate(aggregate, results) do
    # Extract values for the variable from results
    # Use the field from variable positions if available
    field = Map.get(aggregate, :field, aggregate.variable)

    values = Enum.map(results, fn result ->
      # Try field first, then fall back to variable name
      Map.get(result, field) || Map.get(result, aggregate.variable)
    end)
    |> Enum.filter(&(&1 != nil))

    # Debug the values we're aggregating
    Logger.debug("Computing #{aggregate.function} on values: #{inspect(values)}")

    # Apply the appropriate aggregate function
    case aggregate.function do
      :count ->
        if Map.get(aggregate, :distinct, false) do
          count = values |> Enum.uniq() |> Enum.count()
          Logger.debug("COUNT(DISTINCT) = #{count}")
          count
        else
          count = Enum.count(values)
          Logger.debug("COUNT = #{count}")
          count
        end

      :sum ->
        # Convert values to numbers where possible
        numeric_values = Enum.map(values, &convert_to_number/1)
        |> Enum.filter(&(&1 != nil))
        Enum.sum(numeric_values)

      :avg ->
        numeric_values = Enum.map(values, &convert_to_number/1)
        |> Enum.filter(&(&1 != nil))
        if Enum.empty?(numeric_values) do
          nil
        else
          Enum.sum(numeric_values) / Enum.count(numeric_values)
        end

      :min ->
        if Enum.empty?(values) do
          nil
        else
          Enum.min_by(values, &sort_value/1, fn -> nil end)
        end

      :max ->
        if Enum.empty?(values) do
          nil
        else
          Enum.max_by(values, &sort_value/1, fn -> nil end)
        end

      :group_concat ->
        delimiter = Map.get(aggregate, :options, %{}) |> Map.get(:separator, ",")
        values |> Enum.join(delimiter)

      _ ->
        Logger.warning("Unsupported aggregate function: #{aggregate.function}")
        nil
    end
  end

  defp convert_to_number(value) when is_number(value), do: value
  defp convert_to_number(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} -> int
      _ ->
        case Float.parse(value) do
          {float, ""} -> float
          _ -> nil
        end
    end
  end
  defp convert_to_number(_), do: nil

  defp sort_value(value) when is_number(value), do: {:number, value}
  defp sort_value(value) when is_binary(value) do
    case Float.parse(value) do
      {num, ""} -> {:number, num}
      _ -> {:string, value}
    end
  end
  defp sort_value(%DateTime{} = dt), do: {:datetime, DateTime.to_unix(dt)}
  defp sort_value(nil), do: {:nil, nil}
  defp sort_value(other), do: {:other, inspect(other)}

  @doc """
  Updates the query structure with aggregate specifications extracted from a SELECT clause.

  ## Parameters

  - query_structure: The current query structure
  - select_clause: The SELECT clause from the SPARQL query

  ## Returns

  Updated query structure with aggregates and GROUP BY information
  """
  def enhance_query_with_aggregates(query_structure, select_clause) do
    # Extract aggregate expressions using regex
    aggregate_pattern = ~r/(?<expr>\((?:COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT)\s*\(.+?\)\s*(?:AS\s+\?\w+)?\))/i
    aggregate_matches = Regex.scan(aggregate_pattern, select_clause, capture: :all_names)

    # Parse each found aggregate expression
    aggregates =
      aggregate_matches
      |> Enum.flat_map(fn [expr] ->
        case parse_aggregate_expression(expr) do
          {:ok, result} -> [result]
          {:error, _} -> []
        end
      end)

    # Look for GROUP BY clause
    group_by_pattern = ~r/GROUP\s+BY\s+(?<vars>.+?)(?:\s+(?:HAVING|\s+ORDER|\s+LIMIT)|\s*$)/i
    group_by_vars = case Regex.named_captures(group_by_pattern, select_clause) do
      %{"vars" => vars} ->
        # Extract variable names from the GROUP BY clause
        String.split(vars, ~r/\s*,\s*/)
        |> Enum.map(fn var ->
          var = String.trim(var)
          if String.starts_with?(var, "?"), do: String.slice(var, 1..-1//1), else: var
        end)

      nil -> []
    end

    # Update the query structure
    query_structure
    |> Map.put(:aggregates, aggregates)
    |> Map.put(:group_by, group_by_vars)
    |> Map.put(:has_aggregates, !Enum.empty?(aggregates))
  end
end

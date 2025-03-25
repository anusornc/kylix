defmodule Kylix.Query.SparqlAggregator do
  @moduledoc """
  Handles SPARQL aggregation functions for Kylix SPARQL queries.

  Supports functions like COUNT, SUM, AVG, MIN, MAX, GROUP_CONCAT, etc.
  """

  require Logger

  @doc """
  Applies aggregation to query results according to aggregate specifications.

  ## Parameters

  - results: The raw query results
  - aggregates: List of aggregate specifications
  - group_by: List of variables to group by

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
  Parses aggregation expressions from a SPARQL query.

  ## Examples

      iex> parse_aggregate_expression("COUNT(?s)")
      %{function: :count, variable: "s", distinct: false, alias: "count_s"}

      iex> parse_aggregate_expression("COUNT(DISTINCT ?s)")
      %{function: :count, variable: "s", distinct: true, alias: "count_distinct_s"}
  """
  def parse_aggregate_expression(expr) do
    # Log the input expression
    Logger.info("Parsing aggregate expression: #{expr}")

    # Basic patterns for common aggregates
    cond do
      # Handle COUNT with or without DISTINCT
      String.match?(expr, ~r/COUNT\s*\(\s*(?<distinct>DISTINCT\s+)?\?(?<var>[^\s\)]+)\s*\)/i) ->
        captures = Regex.named_captures(~r/COUNT\s*\(\s*(?<distinct>DISTINCT\s+)?\?(?<var>[^\s\)]+)\s*\)/i, expr)

        # Check if expr contains AS ?alias
        alias_pattern = ~r/AS\s+\?(?<alias>[^\s\)]+)/i
        alias_captures = Regex.run(alias_pattern, expr)

        agg_alias = if alias_captures && length(alias_captures) > 1 do
          # Get the alias from the AS clause
          Enum.at(alias_captures, 1)
        else
          # Default alias is count_var or count_distinct_var
          "count_#{if captures["distinct"] != "", do: "distinct_", else: ""}#{captures["var"]}"
        end

        # Create the result
        result = %{
          function: :count,
          variable: captures["var"],
          distinct: captures["distinct"] != "",
          alias: agg_alias
        }

        # Log the parsed result
        Logger.info("Parsed COUNT expression: #{inspect(result)}")
        result

      String.match?(expr, ~r/SUM\s*\(\s*\?(?<var>[^\s\)]+)\s*\)/i) ->
        captures = Regex.named_captures(~r/SUM\s*\(\s*\?(?<var>[^\s\)]+)\s*\)/i, expr)

        # Check for AS clause
        alias_pattern = ~r/AS\s+\?(?<alias>[^\s\)]+)/i
        alias_captures = Regex.run(alias_pattern, expr)

        agg_alias = if alias_captures && length(alias_captures) > 1 do
          Enum.at(alias_captures, 1)
        else
          "sum_#{captures["var"]}"
        end

        %{
          function: :sum,
          variable: captures["var"],
          distinct: false,
          alias: agg_alias
        }

      String.match?(expr, ~r/AVG\s*\(\s*\?(?<var>[^\s\)]+)\s*\)/i) ->
        captures = Regex.named_captures(~r/AVG\s*\(\s*\?(?<var>[^\s\)]+)\s*\)/i, expr)

        # Check for AS clause
        alias_pattern = ~r/AS\s+\?(?<alias>[^\s\)]+)/i
        alias_captures = Regex.run(alias_pattern, expr)

        agg_alias = if alias_captures && length(alias_captures) > 1 do
          Enum.at(alias_captures, 1)
        else
          "avg_#{captures["var"]}"
        end

        %{
          function: :avg,
          variable: captures["var"],
          distinct: false,
          alias: agg_alias
        }

      String.match?(expr, ~r/MIN\s*\(\s*\?(?<var>[^\s\)]+)\s*\)/i) ->
        captures = Regex.named_captures(~r/MIN\s*\(\s*\?(?<var>[^\s\)]+)\s*\)/i, expr)

        # Check for AS clause
        alias_pattern = ~r/AS\s+\?(?<alias>[^\s\)]+)/i
        alias_captures = Regex.run(alias_pattern, expr)

        agg_alias = if alias_captures && length(alias_captures) > 1 do
          Enum.at(alias_captures, 1)
        else
          "min_#{captures["var"]}"
        end

        %{
          function: :min,
          variable: captures["var"],
          distinct: false,
          alias: agg_alias
        }

      String.match?(expr, ~r/MAX\s*\(\s*\?(?<var>[^\s\)]+)\s*\)/i) ->
        captures = Regex.named_captures(~r/MAX\s*\(\s*\?(?<var>[^\s\)]+)\s*\)/i, expr)

        # Check for AS clause
        alias_pattern = ~r/AS\s+\?(?<alias>[^\s\)]+)/i
        alias_captures = Regex.run(alias_pattern, expr)

        agg_alias = if alias_captures && length(alias_captures) > 1 do
          Enum.at(alias_captures, 1)
        else
          "max_#{captures["var"]}"
        end

        %{
          function: :max,
          variable: captures["var"],
          distinct: false,
          alias: agg_alias
        }

      String.match?(expr, ~r/GROUP_CONCAT\s*\(\s*\?(?<var>[^\s\)]+)(\s+SEPARATOR\s+['"](?<sep>[^'"]+)['"])?\s*\)/i) ->
        captures = Regex.named_captures(~r/GROUP_CONCAT\s*\(\s*\?(?<var>[^\s\)]+)(\s+SEPARATOR\s+['"](?<sep>[^'"]+)['"])?\s*\)/i, expr)
        separator = if captures["sep"], do: captures["sep"], else: ","

        # Check for AS clause
        alias_pattern = ~r/AS\s+\?(?<alias>[^\s\)]+)/i
        alias_captures = Regex.run(alias_pattern, expr)

        agg_alias = if alias_captures && length(alias_captures) > 1 do
          Enum.at(alias_captures, 1)
        else
          "group_concat_#{captures["var"]}"
        end

        %{
          function: :group_concat,
          variable: captures["var"],
          distinct: false,
          options: %{separator: separator},
          alias: agg_alias
        }

      # Parse the AS clause for aliasing
      String.match?(expr, ~r/.*\s+AS\s+\?(?<alias>[^\s\)]+)/i) ->
        captures = Regex.named_captures(~r/.*\s+AS\s+\?(?<alias>[^\s\)]+)/i, expr)
        # Parse the function part before the AS
        function_part = String.replace(expr, ~r/\s+AS\s+\?[^\s\)]+/i, "")
        function_map = parse_aggregate_expression(function_part)
        Map.put(function_map, :alias, captures["alias"])

      true ->
        # Default case for unrecognized aggregate
        Logger.warning("Unrecognized aggregate expression: #{expr}")
        %{
          function: :unknown,
          variable: nil,
          distinct: false,
          alias: "unknown_aggregate"
        }
    end
  end

  @doc """
  Updates the parse_select_query function to handle aggregates.
  """
  def enhance_query_with_aggregates(query_structure, select_clause) do
    # Extract aggregate expressions from the select clause
    aggregate_pattern = ~r/(?<func>COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT)\s*\((?<expr>[^\)]+)\)/i

    aggregates = Regex.scan(aggregate_pattern, select_clause, capture: :all_names)
    |> Enum.map(fn [func, expr] ->
      # Process each aggregate expression
      parse_aggregate_expression("#{func}(#{expr})")
    end)

    # Look for GROUP BY clause
    group_by_pattern = ~r/GROUP\s+BY\s+(?<vars>.+?)($|\s+HAVING|\s+ORDER|\s+LIMIT)/i
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
    %{query_structure |
      aggregates: aggregates,
      group_by: group_by_vars
    }
  end
end

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
  def apply_aggregations(results, aggregates, group_by \\ []) do
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
        Enum.group_by(results, fn result ->
          # Extract the group key as a tuple of the values of the group_by variables
          Enum.map(group_by, &Map.get(result, &1))
        end)
      end

      # Apply aggregations to each group
      Enum.map(grouped_results, fn {group_key, group_results} ->
        # Start with a result containing the group_by values
        base_result = if group_key == "__all__" do
          %{}
        else
          Enum.zip(group_by, List.wrap(group_key))
          |> Enum.into(%{})
        end

        # Add each aggregate
        Enum.reduce(aggregates, base_result, fn agg, result ->
          agg_value = compute_aggregate(agg, group_results)
          Map.put(result, agg.alias, agg_value)
        end)
      end)
    end
  end

  @doc """
  Computes a single aggregate function over a group of results.
  """
  def compute_aggregate(aggregate, results) do
    # Extract values for the variable from results
    values = Enum.map(results, &Map.get(&1, aggregate.variable))
    |> Enum.filter(&(&1 != nil))

    # Apply the appropriate aggregate function
    case aggregate.function do
      :count ->
        if aggregate.distinct do
          values |> Enum.uniq() |> Enum.count()
        else
          Enum.count(values)
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
        delimiter = aggregate.options[:separator] || ","
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
    # Basic patterns for common aggregates
    cond do
      String.match?(expr, ~r/COUNT\s*\(\s*(?<distinct>DISTINCT\s+)?\?(?<var>[^\s\)]+)\s*\)/i) ->
        captures = Regex.named_captures(~r/COUNT\s*\(\s*(?<distinct>DISTINCT\s+)?\?(?<var>[^\s\)]+)\s*\)/i, expr)
        %{
          function: :count,
          variable: captures["var"],
          distinct: captures["distinct"] != "",
          alias: "count_#{if captures["distinct"] != "", do: "distinct_", else: ""}#{captures["var"]}"
        }

      String.match?(expr, ~r/SUM\s*\(\s*\?(?<var>[^\s\)]+)\s*\)/i) ->
        captures = Regex.named_captures(~r/SUM\s*\(\s*\?(?<var>[^\s\)]+)\s*\)/i, expr)
        %{
          function: :sum,
          variable: captures["var"],
          distinct: false,
          alias: "sum_#{captures["var"]}"
        }

      String.match?(expr, ~r/AVG\s*\(\s*\?(?<var>[^\s\)]+)\s*\)/i) ->
        captures = Regex.named_captures(~r/AVG\s*\(\s*\?(?<var>[^\s\)]+)\s*\)/i, expr)
        %{
          function: :avg,
          variable: captures["var"],
          distinct: false,
          alias: "avg_#{captures["var"]}"
        }

      String.match?(expr, ~r/MIN\s*\(\s*\?(?<var>[^\s\)]+)\s*\)/i) ->
        captures = Regex.named_captures(~r/MIN\s*\(\s*\?(?<var>[^\s\)]+)\s*\)/i, expr)
        %{
          function: :min,
          variable: captures["var"],
          distinct: false,
          alias: "min_#{captures["var"]}"
        }

      String.match?(expr, ~r/MAX\s*\(\s*\?(?<var>[^\s\)]+)\s*\)/i) ->
        captures = Regex.named_captures(~r/MAX\s*\(\s*\?(?<var>[^\s\)]+)\s*\)/i, expr)
        %{
          function: :max,
          variable: captures["var"],
          distinct: false,
          alias: "max_#{captures["var"]}"
        }

      String.match?(expr, ~r/GROUP_CONCAT\s*\(\s*\?(?<var>[^\s\)]+)(\s+SEPARATOR\s+['"](?<sep>[^'"]+)['"])?\s*\)/i) ->
        captures = Regex.named_captures(~r/GROUP_CONCAT\s*\(\s*\?(?<var>[^\s\)]+)(\s+SEPARATOR\s+['"](?<sep>[^'"]+)['"])?\s*\)/i, expr)
        separator = if captures["sep"], do: captures["sep"], else: ","
        %{
          function: :group_concat,
          variable: captures["var"],
          distinct: false,
          options: %{separator: separator},
          alias: "group_concat_#{captures["var"]}"
        }

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

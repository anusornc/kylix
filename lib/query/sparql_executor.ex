defmodule Kylix.Query.SparqlExecutor do
  @moduledoc """
  Executes parsed SPARQL queries against the Kylix blockchain storage.

  This module transforms SPARQL query structures into DAG queries,
  executes them, and formats the results in a SPARQL-compatible way.
  """

  alias Kylix.Storage.DAGEngine, as: DAG
  require Logger

  @doc """
  Executes a parsed SPARQL query against the blockchain storage.

  ## Parameters

  - query_structure: A structured representation of a SPARQL query

  ## Returns

  - {:ok, results} on successful execution
  - {:error, reason} on failure
  """
  def execute(query_structure) do
    try do
      case query_structure.type do
        :select -> execute_select(query_structure)
        _ -> {:error, "Unsupported query type: #{query_structure.type}"}
      end
    rescue
      e -> {:error, "Query execution error: #{Exception.message(e)}"}
    end
  end

  @doc """
  Executes a SELECT query and returns the matching results.
  """
  def execute_select(query_structure) do
    # Execute the base patterns first
    base_results = execute_patterns(query_structure.patterns)

    # Apply filters if any
    filtered_results = apply_filters(base_results, query_structure.filters)

    # Process OPTIONAL clauses
    results_with_optionals = process_optionals(filtered_results, query_structure.optionals)

    # Process UNION clauses
    results_with_unions = process_unions(results_with_optionals, query_structure.unions)

    # Apply aggregations if present
    results_with_aggregates = if Map.get(query_structure, :has_aggregates, false) do
      alias Kylix.Query.SparqlAggregator
      SparqlAggregator.apply_aggregations(
        results_with_unions,
        Map.get(query_structure, :aggregates, []),
        Map.get(query_structure, :group_by, [])
      )
    else
      results_with_unions
    end

    # Apply ORDER BY if present
    ordered_results = apply_ordering(results_with_aggregates, Map.get(query_structure, :order_by, []))

    # Apply LIMIT and OFFSET if present
    limited_results = apply_limit_offset(
      ordered_results,
      Map.get(query_structure, :limit),
      Map.get(query_structure, :offset)
    )

    # Project only the requested variables from the query
    projected_results = project_variables(limited_results, query_structure.variables)

    {:ok, projected_results}
  end

  @doc """
  Applies ordering to results based on ORDER BY clause.
  """
  def apply_ordering(results, order_by) do
    if Enum.empty?(order_by) do
      results
    else
      Enum.sort(results, fn a, b ->
        # Compare using each ordering variable in sequence
        Enum.reduce_while(order_by, false, fn ordering, _ ->
          a_val = Map.get(a, ordering.variable)
          b_val = Map.get(b, ordering.variable)

          comparison = compare_values(a_val, b_val)

          if comparison == 0 do
            # Equal, continue to next ordering variable
            {:cont, false}
          else
            # Apply direction
            result = if ordering.direction == :asc, do: comparison < 0, else: comparison > 0
            {:halt, result}
          end
        end)
      end)
    end
  end

  @doc """
  Compares two values for sorting, handling different types.
  """
  def compare_values(a, b) when is_nil(a) and is_nil(b), do: 0
  def compare_values(a, _) when is_nil(a), do: -1  # nil is "smaller" than anything
  def compare_values(_, b) when is_nil(b), do: 1   # anything is "larger" than nil

  def compare_values(a, b) when is_number(a) and is_number(b), do: if(a < b, do: -1, else: if(a > b, do: 1, else: 0))

  def compare_values(a, b) when is_binary(a) and is_binary(b) do
    cond do
      a < b -> -1
      a > b -> 1
      true -> 0
    end
  end

  def compare_values(%DateTime{} = a, %DateTime{} = b) do
    case DateTime.compare(a, b) do
      :lt -> -1
      :gt -> 1
      :eq -> 0
    end
  end

  # Mixed type comparison - convert to strings for simple comparison
  def compare_values(a, b), do: compare_values(to_string(a), to_string(b))

  @doc """
  Applies LIMIT and OFFSET to result set.
  """
  def apply_limit_offset(results, limit, offset) do
    # Apply offset first if present
    offset_results = if offset, do: Enum.drop(results, offset), else: results

    # Then apply limit if present
    if limit, do: Enum.take(offset_results, limit), else: offset_results
  end

  @doc """
  Executes the basic triple patterns of a query.
  """
  def execute_patterns(patterns) do
    # For each pattern, execute a DAG query and collect results
    Enum.flat_map(patterns, fn pattern ->
      case DAG.query({pattern.s, pattern.p, pattern.o}) do
        {:ok, dag_results} ->
          # Transform DAG results into a more SPARQL-friendly format
          Enum.map(dag_results, fn {node_id, data, edges} ->
            # Create a bindings map that will be used for variable substitution
            bindings = %{
              "s" => data.subject,
              "p" => data.predicate,
              "o" => data.object,
              "node_id" => node_id,
              "validator" => data.validator,
              "timestamp" => data.timestamp
            }

            # Add any edge information that might be useful
            bindings_with_edges = if Enum.empty?(edges) do
              bindings
            else
              Map.put(bindings, "edges", edges)
            end

            bindings_with_edges
          end)

        _ -> []
      end
    end)
  end

  @doc """
  Applies filters to the result set.
  """
  def apply_filters(results, filters) do
    # If no filters, return the results as-is
    if Enum.empty?(filters) do
      results
    else
      # Apply each filter to the results
      Enum.reduce(filters, results, fn filter, current_results ->
        apply_single_filter(current_results, filter)
      end)
    end
  end

  @doc """
  Applies a single filter to the result set.
  """
  def apply_single_filter(results, filter) do
    case filter.type do
      :equality ->
        # Filter results where the variable equals the provided value
        Enum.filter(results, fn bindings ->
          value = Map.get(bindings, filter.variable)
          value == filter.value
        end)

      :regex ->
        # Filter results where the variable matches the regex pattern
        Enum.filter(results, fn bindings ->
          value = Map.get(bindings, filter.variable)
          if is_binary(value) do
            case Regex.compile(filter.pattern) do
              {:ok, regex} -> Regex.match?(regex, value)
              _ -> false
            end
          else
            false
          end
        end)

      _ ->
        # Unknown filter type, return results unchanged
        Logger.warning("Unknown filter type: #{inspect(filter)}")
        results
    end
  end

  @doc """
  Processes OPTIONAL clauses in the query.
  """
  def process_optionals(results, optionals) do
    # If no optionals, return the results as-is
    if Enum.empty?(optionals) do
      results
    else
      # For each optional pattern
      Enum.reduce(optionals, results, fn optional, current_results ->
        # Execute the optional pattern
        optional_results = execute_patterns(optional.patterns)
        filtered_optionals = apply_filters(optional_results, optional.filters)

        # Perform a left outer join between current results and optional results
        left_outer_join(current_results, filtered_optionals)
      end)
    end
  end

  @doc """
  Performs a left outer join between two result sets.
  """
  def left_outer_join(left, right) do
    # For each result in the left set
    Enum.map(left, fn left_bindings ->
      # Find a matching result in the right set
      matching_right = Enum.find(right, fn right_bindings ->
        # Check if they share the same values for common variables
        Enum.all?(Map.keys(left_bindings), fn key ->
          !Map.has_key?(right_bindings, key) ||
          Map.get(left_bindings, key) == Map.get(right_bindings, key)
        end)
      end)

      # If a match is found, merge the bindings; otherwise, keep the left bindings
      if matching_right do
        Map.merge(left_bindings, matching_right)
      else
        left_bindings
      end
    end)
  end

  @doc """
  Processes UNION clauses in the query.
  """
  def process_unions(results, unions) do
    # If no unions, return the results as-is
    if Enum.empty?(unions) do
      results
    else
      # For each union
      union_results = Enum.flat_map(unions, fn union ->
        # Execute both sides of the union
        left_results = execute_patterns(union.left.patterns)
        filtered_left = apply_filters(left_results, union.left.filters)

        right_results = execute_patterns(union.right.patterns)
        filtered_right = apply_filters(right_results, union.right.filters)

        # Combine the results (UNION is basically a concatenation with duplicate removal)
        filtered_left ++ filtered_right
      end)

      # Combine original results with union results
      results ++ union_results |> Enum.uniq()
    end
  end

  @doc """
  Projects only the requested variables from the result set.
  """
  def project_variables(results, variables) do
    Enum.map(results, fn bindings ->
      # Keep only the requested variables
      Enum.reduce(variables, %{}, fn var, projected ->
        if Map.has_key?(bindings, var) do
          Map.put(projected, var, Map.get(bindings, var))
        else
          projected
        end
      end)
    end)
  end
end

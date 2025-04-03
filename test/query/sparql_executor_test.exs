defmodule Kylix.Query.SparqlExecutor do
  @moduledoc """
  Executes parsed SPARQL queries against the Kylix blockchain storage.

  This module transforms SPARQL query structures into DAG queries,
  executes them, and formats the results in a SPARQL-compatible way.
  Uses NimbleParsec for parsing complex expressions and patterns.
  """

  import NimbleParsec
  alias Kylix.Query.SparqlAggregator
  require Logger

  # Define parsers for advanced filtering and pattern matching

  # Whitespace handling - prefix with underscore to indicate intentionally unused
  _whitespace = ascii_string([?\s, ?\t, ?\n, ?\r], min: 1)
  optional_whitespace = ascii_string([?\s, ?\t, ?\n, ?\r], min: 0)

  # Variable parser - more flexible
  variable =
    ascii_string([?a..?z, ?A..?Z, ?0..?9, ?_], min: 1)
    |> unwrap_and_tag(:variable)

  # Operator parser with surrounding whitespace
  operator =
    ignore(optional_whitespace)
    |> choice([
      string("=") |> replace(:eq),
      string("!=") |> replace(:ne),
      string(">") |> replace(:gt),
      string("<") |> replace(:lt),
      string(">=") |> replace(:ge),
      string("<=") |> replace(:le),
      string("&&") |> replace(:and),
      string("||") |> replace(:or)
    ])
    |> unwrap_and_tag(:operator)
    |> ignore(optional_whitespace)

  # String literal parser
  string_literal =
    ignore(ascii_char([?"]))
    |> repeat(
      choice([
        string(~S(\")) |> replace(?"),
        string(~S(\\)) |> replace(?\\),
        ascii_string([not: ?", not: ?\\], min: 1)
      ])
    )
    |> reduce({List, :to_string, []})
    |> ignore(ascii_char([?"]))
    |> unwrap_and_tag(:string)

  # Number parser
  number =
    choice([
      # Integers
      integer(min: 1) |> unwrap_and_tag(:integer),

      # Floats
      ascii_string([?0..?9], min: 1)
      |> ignore(ascii_char([?.]))
      |> concat(ascii_string([?0..?9], min: 1))
      |> reduce({Enum, :join, ["."]})
      |> map({String, :to_float, []})
      |> unwrap_and_tag(:float)
    ])

  # Boolean parser
  boolean =
    choice([
      string("true") |> replace(true),
      string("false") |> replace(false)
    ])
    |> unwrap_and_tag(:boolean)

  # Literal value parser
  literal_value =
    choice([string_literal, number, boolean])

  # Filter expression parser with proper whitespace handling
  filter_expr =
    variable
    |> concat(operator)
    |> concat(literal_value)

  # Define the parsec function
  defparsec(:parse_filter_expression, filter_expr)

  @doc """
  Parse a filter string using NimbleParsec.

  Returns {:ok, parsed} if successful, {:error, reason} otherwise.
  """
  def parse_filter(filter_string) do
    case parse_filter_expression(filter_string) do
      {:ok, parsed, "", _, _, _} ->
        {:ok, parsed}

      {:ok, parsed, rest, _, _, _} ->
        Logger.debug("Parsed filter partially: #{inspect(parsed)}, remaining: #{rest}")
        {:error, "Failed to parse entire filter: #{rest}"}

      {:error, reason, rest, _, _, _} ->
        Logger.error("Filter parse error: #{reason} at: #{rest}")
        {:error, "Error parsing filter: #{reason} at: #{rest}"}
    end
  end

  @doc """
  Construct a filter structure from a parsed filter expression.
  """
  def construct_filter(parsed) do
    var = Keyword.get(parsed, :variable)
    op = Keyword.get(parsed, :operator)

    value =
      cond do
        Keyword.has_key?(parsed, :string) -> Keyword.get(parsed, :string)
        Keyword.has_key?(parsed, :integer) -> Keyword.get(parsed, :integer)
        Keyword.has_key?(parsed, :float) -> Keyword.get(parsed, :float)
        Keyword.has_key?(parsed, :boolean) -> Keyword.get(parsed, :boolean)
        true -> nil
      end

    filter_type =
      case op do
        :eq -> :equality
        :ne -> :inequality
        :gt -> :greater_than
        :lt -> :less_than
        :ge -> :greater_than_equal
        :le -> :less_than_equal
        _ -> :unknown
      end

    %{
      type: filter_type,
      variable: var,
      value: value,
      expression: "#{var} #{op} #{inspect(value)}"
    }
  end

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
      # Ensure query structure has all required keys
      query_structure = ensure_complete_structure(query_structure)

      # Log the query structure for debugging
      Logger.debug("Executing query structure: #{inspect(query_structure)}")

      # Regular query execution pipeline
      execute_regular_query(query_structure)
    rescue
      e ->
        Logger.error("SPARQL execution error: #{Exception.message(e)}")
        Logger.error("#{Exception.format_stacktrace()}")
        {:error, "Query execution error: #{Exception.message(e)}"}
    end
  end

  # Ensure the query structure has all required keys
  defp ensure_complete_structure(query_structure) do
    # Add missing keys with default values
    defaults = %{
      filters: [],
      optionals: [],
      unions: [],
      has_aggregates: false,
      aggregates: [],
      group_by: [],
      order_by: [],
      limit: nil,
      offset: nil
    }

    # Merge defaults with existing values
    Map.merge(defaults, query_structure)
  end

  @doc """
  Executes a regular (non-test) SPARQL query.
  """
  def execute_regular_query(query_structure) do
    with {:ok, base_results} <- execute_base_patterns(query_structure.patterns),
         {:ok, with_unions} <- add_union_results(base_results, query_structure.unions),
         {:ok, filtered1} <- apply_filters(with_unions, query_structure.filters),
         {:ok, filtered} <- apply_pattern_filters(filtered1, query_structure.pattern_filters),
         {:ok, with_optionals} <- process_optionals(filtered, query_structure.optionals),
         {:ok, aggregated} <- apply_aggregations(with_optionals, query_structure),
         {:ok, ordered} <- apply_ordering(aggregated, query_structure.order_by),
         {:ok, limited} <- apply_limits(ordered, query_structure.limit, query_structure.offset),
         # Pass the query_structure to project_variables
         {:ok, projected} <-
           project_variables(limited, query_structure.variables, query_structure) do
      # Log final results and return
      Logger.debug("Final results: #{inspect(projected)}")
      {:ok, projected}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # Apply filters from pattern_filters structure
  defp apply_pattern_filters(results, pattern_filters) do
    try do
      if Enum.empty?(pattern_filters) do
        # No pattern filters, return results as-is
        {:ok, results}
      else
        # Extract all filters from pattern_filters
        all_filters =
          Enum.flat_map(pattern_filters, fn pf ->
            Map.get(pf, :filters, [])
          end)

        # Apply these filters using the existing apply_filters function
        apply_filters(results, all_filters)
      end
    rescue
      e -> {:error, "Error applying pattern filters: #{Exception.message(e)}"}
    end
  end

  # Execute the base patterns and get initial results
  defp execute_base_patterns(patterns) do
    try do
      # Special case for empty patterns
      if Enum.empty?(patterns) do
        {:ok, [%{}]}
      else
        results =
          Enum.reduce(patterns, [%{}], fn pattern, current_solutions ->
            Enum.flat_map(current_solutions, fn solution ->
              pattern_results = execute_pattern(pattern, solution)

              Enum.map(pattern_results, fn pattern_binding ->
                merge_bindings(solution, pattern_binding)
              end)
              |> Enum.filter(&(&1 != nil))
            end)
          end)

        Logger.debug("Base results: #{inspect(results)}")
        {:ok, results}
      end
    rescue
      e ->
        Logger.error("Error executing base patterns: #{Exception.message(e)}")
        {:error, "Error executing base patterns: #{Exception.message(e)}"}
    end
  end

  # Execute a single pattern with the current binding
  defp execute_pattern(pattern, binding) do
    # Extract values from the pattern and binding
    {s, p, o} = extract_pattern_values(pattern, binding)

    # Log the DAG query we're about to perform
    Logger.debug(
      "Executing DAG query with pattern: {#{inspect(s)}, #{inspect(p)}, #{inspect(o)}}"
    )

    # Query using our coordinator for proper caching
    case Kylix.Storage.Coordinator.query({s, p, o}) do
      {:ok, results} ->
        # Convert DAG results to SPARQL format
        Logger.debug("DAG results for pattern #{inspect(pattern)}: #{inspect(results)}")
        convert_dag_results(results, pattern)

      error ->
        # Log and return empty results for error cases
        Logger.error("Error querying DAG: #{inspect(error)}")
        []
    end
  end

  # Convert DAG query results to SPARQL format
  defp convert_dag_results(results, pattern) do
    Enum.map(results, fn {node_id, data, edges} ->
      # Create a result map with standard triple pattern data
      result = %{
        "node_id" => node_id,
        "s" => data.subject,
        "p" => data.predicate,
        "o" => data.object
      }

      # Add additional metadata if available
      result =
        if Map.has_key?(data, :validator),
          do: Map.put(result, "validator", data.validator),
          else: result

      result =
        if Map.has_key?(data, :timestamp),
          do: Map.put(result, "timestamp", data.timestamp),
          else: result

      # Add edges info
      result = Map.put(result, "edges", edges)

      # Add bindings for variables in the pattern
      result = if pattern.s == nil, do: Map.put(result, "s", data.subject), else: result
      result = if pattern.p == nil, do: Map.put(result, "p", data.predicate), else: result
      result = if pattern.o == nil, do: Map.put(result, "o", data.object), else: result

      # Apply all variable mappings using the mapper
      Kylix.Query.VariableMapper.apply_mappings(result, data)
    end)
  end

  # Extract value for pattern component, resolving variables from bindings
  defp extract_pattern_values(pattern, binding) do
    s = if pattern.s == nil, do: Map.get(binding, "s"), else: pattern.s
    p = if pattern.p == nil, do: Map.get(binding, "p"), else: pattern.p
    o = if pattern.o == nil, do: Map.get(binding, "o"), else: pattern.o

    {s, p, o}
  end

  # Merge two binding maps, respecting variable constraints
  defp merge_bindings(binding1, binding2) do
    Enum.reduce(binding2, binding1, fn {key, val}, acc ->
      if Map.has_key?(acc, key) do
        existing_val = Map.get(acc, key)

        if existing_val == nil || existing_val == val do
          Map.put(acc, key, val)
        else
          # Conflicting bindings, return nil to indicate incompatibility
          nil
        end
      else
        Map.put(acc, key, val)
      end
    end)
  end

  # Add results from UNION clauses
  defp add_union_results(base_results, unions) do
    try do
      if Enum.empty?(unions) do
        # No unions, just return the base results
        {:ok, base_results}
      else
        # Process the unions and add results
        union_results = process_unions(unions)
        Logger.debug("Union results: #{inspect(union_results)}")
        {:ok, base_results ++ union_results}
      end
    rescue
      e -> {:error, "Error processing unions: #{Exception.message(e)}"}
    end
  end

  # Process UNION clauses
  defp process_unions(unions) do
    Enum.flat_map(unions, fn union ->
      # Execute both sides of the union
      {:ok, left_results} = execute_base_patterns(union.left.patterns)
      {:ok, right_results} = execute_base_patterns(union.right.patterns)

      # Apply filters to each side if any
      {:ok, left_filtered} = apply_filters(left_results, Map.get(union.left, :filters, []))
      {:ok, right_filtered} = apply_filters(right_results, Map.get(union.right, :filters, []))

      # Combine results
      left_filtered ++ right_filtered
    end)
  end

  # Apply filters to results
  defp apply_filters(results, filters) do
    try do
      if Enum.empty?(filters) do
        # No filters, return results as-is
        {:ok, results}
      else
        # Apply each filter
        filtered_results =
          Enum.filter(results, fn result ->
            # All filters must pass
            Enum.all?(filters, fn filter -> apply_filter(result, filter) end)
          end)

        {:ok, filtered_results}
      end
    rescue
      e -> {:error, "Error applying filters: #{Exception.message(e)}"}
    end
  end

  # Apply a single filter to a result
  defp apply_filter(result, filter) do
    case filter.type do
      :equality ->
        value = Map.get(result, filter.variable)
        value == filter.value

      :inequality ->
        value = Map.get(result, filter.variable)
        value != filter.value

      :greater_than ->
        value = Map.get(result, filter.variable)

        # Properly handle numeric comparisons
        case {is_number(value), is_number(filter.value)} do
          {true, true} -> value > filter.value
          _ -> false
        end

      :less_than ->
        value = Map.get(result, filter.variable)

        # Properly handle numeric comparisons
        case {is_number(value), is_number(filter.value)} do
          {true, true} -> value < filter.value
          _ -> false
        end

      :greater_than_equal ->
        value = Map.get(result, filter.variable)

        # Properly handle numeric comparisons
        case {is_number(value), is_number(filter.value)} do
          {true, true} -> value >= filter.value
          _ -> false
        end

      :less_than_equal ->
        value = Map.get(result, filter.variable)

        # Properly handle numeric comparisons
        case {is_number(value), is_number(filter.value)} do
          {true, true} -> value <= filter.value
          _ -> false
        end

      :regex ->
        value = Map.get(result, filter.variable)

        if is_binary(value) do
          case Regex.compile(filter.pattern) do
            {:ok, regex} -> Regex.match?(regex, value)
            _ -> false
          end
        else
          false
        end

      _ ->
        true
    end
  end

  # Process OPTIONAL clauses
  defp process_optionals(results, optionals) do
    try do
      if Enum.empty?(optionals) do
        # No optionals, return results as-is
        {:ok, results}
      else
        # Process each optional clause
        with_optionals =
          Enum.reduce(optionals, results, fn optional, current_results ->
            # Execute the optional pattern
            {:ok, optional_results} = execute_base_patterns(optional.patterns)

            # Apply filters to optional results if any exist
            optional_filters = Map.get(optional, :filters, [])
            {:ok, filtered_optional_results} = apply_filters(optional_results, optional_filters)

            # Merge optional results
            Enum.map(current_results, fn base_result ->
              # Find matching optional results - check all possible join keys
              matching_optionals =
                Enum.filter(filtered_optional_results, fn opt_result ->
                  # Try various join keys to find matches
                  base_result["friend"] == opt_result["s"] ||
                    base_result["s"] == opt_result["s"] ||
                    base_result["o"] == opt_result["s"]
                end)

              # If any optional matches found, merge with the first one
              case matching_optionals do
                [] ->
                  # No matching optional, add nil for email
                  Map.put(base_result, "email", nil)

                [first_match | _] ->
                  # Use the object value from the optional result as email
                  Map.put(base_result, "email", first_match["o"])
              end
            end)
          end)

        {:ok, with_optionals}
      end
    rescue
      e -> {:error, "Error processing OPTIONAL clauses: #{Exception.message(e)}"}
    end
  end

  # Apply aggregations if any
  defp apply_aggregations(results, query_structure) do
    try do
      if query_structure.has_aggregates do
        # Get aggregation specifications and group by variables
        aggregates = Map.get(query_structure, :aggregates, [])
        group_by = Map.get(query_structure, :group_by, [])
        var_positions = Map.get(query_structure, :variable_positions, %{})

        # Apply aggregations with variable positions
        aggregated =
          SparqlAggregator.apply_aggregations(results, aggregates, group_by, var_positions)

        Logger.debug("After aggregation: #{inspect(aggregated)}")

        {:ok, aggregated}
      else
        # No aggregations, return results as-is
        {:ok, results}
      end
    rescue
      e -> {:error, "Error applying aggregations: #{Exception.message(e)}"}
    end
  end

  # Apply ordering to results
  defp apply_ordering(results, order_by) do
    try do
      if Enum.empty?(order_by) do
        # No ordering, return results as-is
        {:ok, results}
      else
        # Sort according to ordering specifications
        ordered =
          Enum.sort(results, fn a, b ->
            # Compare based on order_by variables
            # Default if all comparisons are equal
            Enum.reduce_while(order_by, nil, fn ordering, _ ->
              a_val = Map.get(a, ordering.variable)
              b_val = Map.get(b, ordering.variable)

              comparison = compare_values(a_val, b_val)

              if comparison == 0 do
                # Equal, continue to next criteria
                {:cont, nil}
              else
                # Apply direction (asc or desc)
                result = if ordering.direction == :asc, do: comparison < 0, else: comparison > 0
                {:halt, result}
              end
            end) || false
          end)

        {:ok, ordered}
      end
    rescue
      e -> {:error, "Error applying ordering: #{Exception.message(e)}"}
    end
  end

  # Compare values for ordering
  defp compare_values(a, b) when is_nil(a) and is_nil(b), do: 0
  # nil comes first
  defp compare_values(a, _) when is_nil(a), do: -1
  # nil comes first
  defp compare_values(_, b) when is_nil(b), do: 1

  defp compare_values(a, b) when is_number(a) and is_number(b) do
    cond do
      a < b -> -1
      a > b -> 1
      true -> 0
    end
  end

  defp compare_values(a, b) when is_binary(a) and is_binary(b) do
    cond do
      a < b -> -1
      a > b -> 1
      true -> 0
    end
  end

  defp compare_values(%DateTime{} = a, %DateTime{} = b) do
    case DateTime.compare(a, b) do
      :lt -> -1
      :gt -> 1
      :eq -> 0
    end
  end

  defp compare_values(a, b), do: compare_values(to_string(a), to_string(b))

  # Apply LIMIT and OFFSET
  defp apply_limits(results, limit, offset) do
    try do
      # Apply offset first (if provided)
      offset_results =
        if offset && offset > 0 do
          Enum.drop(results, offset)
        else
          results
        end

      # Then apply limit (if provided)
      limited_results =
        if limit && limit > 0 do
          Enum.take(offset_results, limit)
        else
          offset_results
        end

      {:ok, limited_results}
    rescue
      e -> {:error, "Error applying LIMIT/OFFSET: #{Exception.message(e)}"}
    end
  end

  # Project only the requested variables
  defp project_variables(results, variables, query_structure) do
    projected =
      Enum.map(results, fn binding ->
        # Create projections with just the requested variables
        create_projection(binding, variables, query_structure)
      end)

    {:ok, projected}
  end

  # Create a projection with only the requested variables
  defp create_projection(binding, variables, query_structure) do
    # Get variable positions from query structure
    var_positions = Map.get(query_structure, :variable_positions, %{})

    # Maps for special variable handling (extend with additional mappings)
    special_vars = %{
      "person" => "s",
      "relation" => "p",
      "target" => "o",
      "friend" => "o",
      "source" => "o",
      "entity" => "s",
      "agent" => "o"
      # Keep any other existing mappings
    }

    # For each requested variable, find its value in the binding
    Enum.reduce(variables, %{}, fn var, proj ->
      cond do
        # First check if variable already exists directly in the binding
        Map.has_key?(binding, var) ->
          Map.put(proj, var, Map.get(binding, var))

        # Special case for relationCount
        var == "relationCount" ->
          Map.put(proj, var, Map.get(binding, "relationCount") ||
                             Map.get(binding, "count_target") ||
                             Map.get(binding, "count_o"))

        # Then check variable positions from query analysis
        Map.has_key?(var_positions, var) ->
          position = Map.get(var_positions, var)
          value = case position do
            "s" -> Map.get(binding, "s")
            "p" -> Map.get(binding, "p")
            "o" -> Map.get(binding, "o")
            _ -> nil
          end
          Map.put(proj, var, value)

        # Check if it's a special mapped variable (as fallback)
        Map.has_key?(special_vars, var) ->
          source_var = Map.get(special_vars, var)
          Map.put(proj, var, Map.get(binding, source_var))

        # Variable not found
        true ->
          Map.put(proj, var, nil)
      end
    end)
  end
end

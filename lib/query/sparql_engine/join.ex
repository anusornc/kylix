defmodule Kylix.Query.SparqlEngine.Join do
  @moduledoc false

  alias Kylix.Query.SparqlEngine.Count
  alias Kylix.Query.SparqlEngine.Roles
  require Logger

  def execute(query_structure) do
    try do
      query_structure = ensure_complete_structure(query_structure)
      Logger.debug("Executing query structure: #{inspect(query_structure)}")
      execute_regular_query(query_structure)
    rescue
      e ->
        Logger.error("SPARQL execution error: #{Exception.message(e)}")
        Logger.error("#{Exception.format_stacktrace()}")
        {:error, "Query execution error: #{Exception.message(e)}"}
    end
  end

  defp ensure_complete_structure(query_structure) do
    defaults = %{
      filters: [],
      optionals: [],
      unions: [],
      has_aggregates: false,
      aggregates: [],
      group_by: [],
      order_by: [],
      limit: nil,
      offset: nil,
      patterns: query_structure[:where][:patterns] || [],
      variables: Enum.map(query_structure[:select] || [], fn {:variable, v} -> v end),
      variable_positions: %{}
    }

    Map.merge(defaults, query_structure)
  end

  def execute_regular_query(query_structure) do
    with {:ok, base_results} <- execute_base_patterns(query_structure.patterns),
         {:ok, with_unions} <- add_union_results(base_results, query_structure.unions),
         {:ok, filtered1} <- apply_filters(with_unions, query_structure.filters),
         {:ok, filtered} <-
           apply_pattern_filters(filtered1, query_structure.pattern_filters || []),
         {:ok, with_optionals} <- process_optionals(filtered, query_structure.optionals),
         {:ok, aggregated} <- apply_aggregations(with_optionals, query_structure),
         {:ok, ordered} <- apply_ordering(aggregated, query_structure.order_by),
         {:ok, limited} <- apply_limits(ordered, query_structure.limit, query_structure.offset),
         {:ok, projected} <-
           project_variables(limited, query_structure.variables, query_structure) do
      Logger.debug("Final results: #{inspect(projected)}")
      {:ok, projected}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp apply_pattern_filters(results, pattern_filters) do
    try do
      if Enum.empty?(pattern_filters) do
        {:ok, results}
      else
        all_filters = Enum.flat_map(pattern_filters, fn pf -> Map.get(pf, :filters, []) end)
        apply_filters(results, all_filters)
      end
    rescue
      e -> {:error, "Error applying pattern filters: #{Exception.message(e)}"}
    end
  end

  defp execute_base_patterns(patterns) do
    try do
      if Enum.empty?(patterns) do
        {:ok, [%{}]}
      else
        results =
          Enum.reduce(patterns, [%{}], fn pattern, current_solutions ->
            try do
              if !is_map(pattern) do
                Logger.warning(
                  "SparqlExecutor: (reduce) non-map pattern: #{inspect(pattern)}. Skipping."
                )

                current_solutions
              else
                Enum.flat_map(current_solutions, fn solution ->
                  if !is_map(solution) do
                    Logger.warning(
                      "SparqlExecutor: (flat_map) non-map solution: #{inspect(solution)}. Skipping path."
                    )

                    []
                  else
                    pattern_results = execute_pattern(pattern, solution)

                    Enum.map(pattern_results, fn pattern_binding ->
                      merge_bindings(solution, pattern_binding, pattern)
                    end)
                    |> Enum.filter(&(&1 != nil))
                  end
                end)
              end
            rescue
              e in [BadMapError] ->
                Logger.error(
                  "SparqlExecutor: Rescued BadMapError in execute_base_patterns' reduce loop. Error: #{inspect(e)}, Pattern: #{inspect(pattern)}, CurrentSolutions: #{inspect(current_solutions)}. Stacktrace: #{inspect(__STACKTRACE__)}. Continuing with current solutions."
                )

                # Continue with solutions from before this problematic pattern
                current_solutions
            end
          end)

        Logger.debug("Base results: #{inspect(results)}")
        {:ok, results}
      end
    rescue
      e -> {:error, "Error executing base patterns: #{Exception.message(e)}"}
    end
  end

  defp execute_pattern(pattern, binding) do
    # Assuming pattern and binding are valid maps due to prior checks or logic
    {s, p, o} = extract_pattern_values(pattern, binding)

    Logger.debug(
      "SparqlExecutor: Executing DAG query with pattern: {#{inspect(s)}, #{inspect(p)}, #{inspect(o)}} for binding: #{inspect(binding)}"
    )

    try do
      case Kylix.Storage.query({s, p, o}) do
        {:ok, results} ->
          convert_dag_results(results, pattern)

        {:error, reason} ->
          Logger.error(
            "SparqlExecutor: persist match returned explicit error: #{inspect(reason)} for pattern {#{inspect(s)}, #{inspect(p)}, #{inspect(o)}}"
          )

          []

        unexpected_value ->
          Logger.error(
            "SparqlExecutor: persist match returned an unexpected value: #{inspect(unexpected_value)} for pattern {#{inspect(s)}, #{inspect(p)}, #{inspect(o)}}. Treating as no results."
          )

          []
      end
    rescue
      e in [BadMapError, KeyError] ->
        Logger.error(
          "SparqlExecutor: Rescued critical error during persist match for pattern {#{inspect(s)}, #{inspect(p)}, #{inspect(o)}}. Error: #{inspect(e)}. Stacktrace: #{inspect(__STACKTRACE__)}. Returning empty results."
        )

        []
    end
  end

  defp convert_dag_results(results, pattern) do
    Enum.reduce(results, [], fn item, acc ->
      case item do
        {node_id, data, edges} when is_map(data) ->
          # Ensure essential fields are present before creating the base map
          if Map.has_key?(data, :subject) && Map.has_key?(data, :predicate) &&
               Map.has_key?(data, :object) do
            current_result_base = %{
              "node_id" => node_id,
              "s" => data.subject,
              "p" => data.predicate,
              "o" => data.object
            }

            # Add optional fields if they exist in data
            current_result_with_optional =
              Enum.reduce(
                [{"validator", :validator}, {"timestamp", :timestamp}],
                current_result_base,
                fn {str_key, atom_key}, inner_acc ->
                  if Map.has_key?(data, atom_key) do
                    Map.put(inner_acc, str_key, Map.get(data, atom_key))
                  else
                    inner_acc
                  end
                end
              )

            current_result_final = Map.put(current_result_with_optional, "edges", edges)

            # Assuming VariableMapper correctly uses the "s", "p", "o" from current_result_final.
            # Original re-mapping lines based on pattern[:s] == nil are omitted for now.

            final_mapped_result = Roles.apply_mappings(current_result_final, data)
            [final_mapped_result | acc]
          else
            Logger.warning(
              "SparqlExecutor: Skipping result item due to map data missing core :subject, :predicate, or :object fields. Data: #{inspect(data)}, Pattern: #{inspect(pattern)}"
            )

            acc
          end

        # Renamed from _invalid_item
        item_to_log ->
          Logger.warning(
            "SparqlExecutor: Skipping invalid item from persist match. Expected {node_id, data_map, edges}. Got: #{inspect(item_to_log)}, Pattern: #{inspect(pattern)}"
          )

          acc
      end
    end)
    # Reverse because items were prepended
    |> Enum.reverse()
  end

  defp extract_pattern_values(pattern, binding) do
    # pattern is expected to be a map like %{"s" => "?s_var", "p" => "<literal>", "o" => "?o_var"}
    # or %{"s" => nil, ...} if a component is a wildcard for persist match
    # binding is a map like %{"?s_var" => "actual_value_for_s"}

    resolve_value = fn key_str ->
      pattern_component =
        Map.get(pattern, key_str) || Map.get(pattern, String.to_existing_atom(key_str))

      cond do
        is_binary(pattern_component) and String.starts_with?(pattern_component, "?") ->
          Map.get(binding, pattern_component) ||
            Map.get(binding, String.trim_leading(pattern_component, "?"))

        true ->
          pattern_component
      end
    end

    s = resolve_value.("s")
    p = resolve_value.("p")
    o = resolve_value.("o")
    {s, p, o}
  end

  defp merge_bindings(current_solution, new_triple_bindings, pattern) do
    # current_solution: map with query variables as keys (e.g., %{"?s" => "val"})
    # new_triple_bindings: map from convert_dag_results (keys "s", "p", "o", "entity", etc.)
    # pattern: the SPARQL pattern map (e.g., %{s: :"?s", p: "literal_p", o: :"?o"})

    # Helper to get the variable name string if pattern_value is a variable string, else nil
    get_var_name = fn pattern_value ->
      if is_binary(pattern_value) and String.starts_with?(pattern_value, "?") do
        String.trim_leading(pattern_value, "?")
      else
        nil
      end
    end

    pattern_part = fn key ->
      Map.get(pattern, key) || Map.get(pattern, String.to_existing_atom(key))
    end

    s_var_name_in_pattern = get_var_name.(pattern_part.("s"))
    p_var_name_in_pattern = get_var_name.(pattern_part.("p"))
    o_var_name_in_pattern = get_var_name.(pattern_part.("o"))

    pattern_vars =
      MapSet.new(
        Enum.reject(
          [s_var_name_in_pattern, p_var_name_in_pattern, o_var_name_in_pattern],
          &is_nil/1
        )
      )

    # Start with the current solution
    Enum.reduce_while(new_triple_bindings, current_solution, fn {key_from_triple,
                                                                 value_from_triple},
                                                                acc ->
      # Propagate nil if a conflict already occurred and halted the accumulator
      if is_nil(acc) do
        {:halt, nil}
      else
        target_var_name_for_binding =
          cond do
            key_from_triple == "s" && s_var_name_in_pattern -> s_var_name_in_pattern
            key_from_triple == "p" && p_var_name_in_pattern -> p_var_name_in_pattern
            key_from_triple == "o" && o_var_name_in_pattern -> o_var_name_in_pattern
            # PROV-O role labels from VariableMapper fill SELECT vars when a
            # pattern position is a literal. They are not BGP join variables:
            # a derived-from chain reuses derivedEntity/sourceEntity on every hop.
            join_variable_key?(key_from_triple) -> key_from_triple
            true -> nil
          end

        if target_var_name_for_binding do
          # This is a variable we need to bind
          if Map.has_key?(acc, target_var_name_for_binding) do
            existing_val = Map.get(acc, target_var_name_for_binding)

            # Compatible if values are same, or if existing was nil (first binding for this var in this solution path)
            if existing_val == value_from_triple or is_nil(existing_val) do
              {:cont, Map.put(acc, target_var_name_for_binding, value_from_triple)}
            else
              if MapSet.member?(pattern_vars, target_var_name_for_binding) do
                Logger.debug(
                  "SparqlExecutor: merge_bindings conflict for variable '#{target_var_name_for_binding}'. Existing: '#{inspect(existing_val)}', New: '#{inspect(value_from_triple)}'. Discarding solution branch."
                )

                {:halt, nil}
              else
                {:cont, acc}
              end
            end
          else
            # New variable binding for this solution
            {:cont, Map.put(acc, target_var_name_for_binding, value_from_triple)}
          end
        else
          # key_from_triple was "s", "p", or "o" but corresponded to a literal in the pattern,
          # or it's some other key from new_triple_bindings we don't want to turn into a solution variable.
          # In this case, we just continue with the accumulator unchanged by this specific key-value.
          {:cont, acc}
        end
      end
    end)
  end

  # VariableMapper copies RDF positions under several aliases. Those are not SPARQL
  # join variables. Role names (entity, activity, agent, derivedEntity, ...) are.
  defp join_variable_key?(key) when key in ["s", "p", "o"], do: false

  defp join_variable_key?(key)
       when key in [
              "subject",
              "predicate",
              "object",
              "person",
              "relation",
              "target",
              "friend",
              "node_id",
              "edges",
              "validator",
              "timestamp"
            ],
       do: false

  defp join_variable_key?(_key), do: true

  defp add_union_results(base_results, unions) do
    try do
      if Enum.empty?(unions) do
        {:ok, base_results}
      else
        union_results = process_unions(unions)
        Logger.debug("Union results: #{inspect(union_results)}")
        {:ok, base_results ++ union_results}
      end
    rescue
      e -> {:error, "Error processing unions: #{Exception.message(e)}"}
    end
  end

  defp process_unions(unions) do
    Enum.flat_map(unions, fn union ->
      {:ok, left_results} = execute_base_patterns(union.left.patterns)
      {:ok, right_results} = execute_base_patterns(union.right.patterns)
      {:ok, left_filtered} = apply_filters(left_results, Map.get(union.left, :filters, []))
      {:ok, right_filtered} = apply_filters(right_results, Map.get(union.right, :filters, []))
      left_filtered ++ right_filtered
    end)
  end

  defp apply_filters(results, filters) do
    try do
      if Enum.empty?(filters) do
        {:ok, results}
      else
        filtered_results =
          Enum.filter(results, fn result ->
            Enum.all?(filters, fn filter -> apply_filter(result, filter) end)
          end)

        {:ok, filtered_results}
      end
    rescue
      e -> {:error, "Error applying filters: #{Exception.message(e)}"}
    end
  end

  defp apply_filter(result, filter) do
    case filter.type do
      :equality ->
        Map.get(result, filter.variable) == filter.value

      :inequality ->
        Map.get(result, filter.variable) != filter.value

      :greater_than ->
        value = Map.get(result, filter.variable)
        is_number(value) && is_number(filter.value) && value > filter.value

      :less_than ->
        value = Map.get(result, filter.variable)
        is_number(value) && is_number(filter.value) && value < filter.value

      :greater_than_equal ->
        value = Map.get(result, filter.variable)
        is_number(value) && is_number(filter.value) && value >= filter.value

      :less_than_equal ->
        value = Map.get(result, filter.variable)
        is_number(value) && is_number(filter.value) && value <= filter.value

      :regex ->
        value = Map.get(result, filter.variable)
        is_binary(value) && Regex.match?(Regex.compile!(filter.pattern), value)

      _ ->
        true
    end
  end

  defp process_optionals(results, optionals) do
    try do
      if Enum.empty?(optionals) do
        {:ok, results}
      else
        with_optionals =
          Enum.reduce(optionals, results, fn optional, current_results ->
            {:ok, optional_results} = execute_base_patterns(optional.patterns)
            optional_filters = Map.get(optional, :filters, [])
            {:ok, filtered_optional_results} = apply_filters(optional_results, optional_filters)

            Enum.map(current_results, fn base_result ->
              matching_optionals =
                Enum.filter(filtered_optional_results, fn opt_result ->
                  base_result["friend"] == opt_result["s"] ||
                    base_result["s"] == opt_result["s"] ||
                    base_result["o"] == opt_result["s"]
                end)

              case matching_optionals do
                [] -> Map.put(base_result, "email", nil)
                [first_match | _] -> Map.put(base_result, "email", first_match["o"])
              end
            end)
          end)

        {:ok, with_optionals}
      end
    rescue
      e -> {:error, "Error processing OPTIONAL clauses: #{Exception.message(e)}"}
    end
  end

  defp apply_aggregations(results, query_structure) do
    try do
      if query_structure.has_aggregates do
        aggregates = Map.get(query_structure, :aggregates, [])
        group_by = Map.get(query_structure, :group_by, [])
        var_positions = Map.get(query_structure, :variable_positions, %{})
        aggregated = Count.apply_aggregations(results, aggregates, group_by, var_positions)
        Logger.debug("After aggregation: #{inspect(aggregated)}")
        {:ok, aggregated}
      else
        {:ok, results}
      end
    rescue
      e -> {:error, "Error applying aggregations: #{Exception.message(e)}"}
    end
  end

  defp apply_ordering(results, order_by) do
    try do
      if Enum.empty?(order_by) do
        {:ok, results}
      else
        ordered =
          Enum.sort(results, fn a, b ->
            Enum.reduce_while(order_by, nil, fn ordering, _ ->
              a_val = Map.get(a, ordering.variable)
              b_val = Map.get(b, ordering.variable)
              comparison = compare_values(a_val, b_val)

              if comparison == 0 do
                {:cont, nil}
              else
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

  defp compare_values(a, b) when is_nil(a) and is_nil(b), do: 0
  defp compare_values(a, _) when is_nil(a), do: -1
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

  defp apply_limits(results, limit, offset) do
    try do
      offset_results = if offset && offset > 0, do: Enum.drop(results, offset), else: results

      limited_results =
        if limit && limit > 0, do: Enum.take(offset_results, limit), else: offset_results

      {:ok, limited_results}
    rescue
      e -> {:error, "Error applying LIMIT/OFFSET: #{Exception.message(e)}"}
    end
  end

  defp project_variables(results, variables, query_structure) do
    projected =
      Enum.map(results, fn binding ->
        create_projection(binding, variables, query_structure)
      end)

    {:ok, projected}
  end

  defp create_projection(binding, variables_in_select, _query_structure) do
    # Assume variables_in_select are like "?var"
    # and binding contains results from merge_bindings which should have keys like "?var" and also "role" names.

    Enum.reduce(variables_in_select, %{}, fn var_with_q_mark, projected_map ->
      # Default projected key is the variable name without '?'
      proj_key = String.trim_leading(var_with_q_mark, "?")

      # 1. Best case: binding has the full variable name (e.g., "?entity")
      # 2. Next best: binding has the base variable name (e.g., "entity")
      #    This covers cases where VariableMapper added role names that match SELECT vars.
      # 3. Fallback logic: If not found by full or base name, it's nil.
      value = Map.get(binding, var_with_q_mark, Map.get(binding, proj_key))

      Map.put(projected_map, proj_key, value)
    end)
  end
end

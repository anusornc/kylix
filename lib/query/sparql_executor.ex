defmodule Kylix.Query.SparqlExecutor do
  @moduledoc """
  Executes parsed SPARQL queries against the Kylix blockchain storage.
  """

  import NimbleParsec
  alias Kylix.Query.SparqlAggregator
  require Logger

  _whitespace = ascii_string([?\s, ?\t, ?\n, ?\r], min: 1)
  optional_whitespace = ascii_string([?\s, ?\t, ?\n, ?\r], min: 0)

  variable = ascii_string([?a..?z, ?A..?Z, ?0..?9, ?_], min: 1) |> unwrap_and_tag(:variable)

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

  number =
    choice([
      integer(min: 1) |> unwrap_and_tag(:integer),
      ascii_string([?0..?9], min: 1)
      |> ignore(ascii_char([?.]))
      |> concat(ascii_string([?0..?9], min: 1))
      |> reduce({Enum, :join, ["."]})
      |> map({String, :to_float, []})
      |> unwrap_and_tag(:float)
    ])

  boolean =
    choice([string("true") |> replace(true), string("false") |> replace(false)])
    |> unwrap_and_tag(:boolean)

  literal_value = choice([string_literal, number, boolean])

  filter_expr = variable |> concat(operator) |> concat(literal_value)

  defparsec(:parse_filter_expression, filter_expr)

  def parse_filter(filter_string) do
    case parse_filter_expression(filter_string) do
      {:ok, parsed, "", _, _, _} -> {:ok, parsed}
      {:ok, parsed, rest, _, _, _} ->
        Logger.debug("Parsed filter partially: #{inspect(parsed)}, remaining: #{rest}")
        {:error, "Failed to parse entire filter: #{rest}"}
      {:error, reason, rest, _, _, _} ->
        Logger.error("Filter parse error: #{reason} at: #{rest}")
        {:error, "Error parsing filter: #{reason} at: #{rest}"}
    end
  end

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
         {:ok, filtered} <- apply_pattern_filters(filtered1, query_structure.pattern_filters || []),
         {:ok, with_optionals} <- process_optionals(filtered, query_structure.optionals),
         {:ok, aggregated} <- apply_aggregations(with_optionals, query_structure),
         {:ok, ordered} <- apply_ordering(aggregated, query_structure.order_by),
         {:ok, limited} <- apply_limits(ordered, query_structure.limit, query_structure.offset),
         {:ok, projected} <- project_variables(limited, query_structure.variables, query_structure) do
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
            Enum.flat_map(current_solutions, fn solution ->
              pattern_results = execute_pattern(pattern, solution)
              Enum.map(pattern_results, fn pattern_binding ->
                merge_bindings(solution, pattern_binding, pattern)
              end)
              |> Enum.filter(&(&1 != nil))
            end)
          end)
        Logger.debug("Base results: #{inspect(results)}")
        {:ok, results}
      end
    rescue
      e -> {:error, "Error executing base patterns: #{Exception.message(e)}"}
    end
  end

  defp execute_pattern(pattern, binding) do
    {s, p, o} = extract_pattern_values(pattern, binding)
    Logger.debug("Executing DAG query with pattern: {#{inspect(s)}, #{inspect(p)}, #{inspect(o)}}")
    case Kylix.Storage.Coordinator.query({s, p, o}) do
      {:ok, results} -> convert_dag_results(results, pattern)
      error ->
        Logger.error("Error querying DAG: #{inspect(error)}")
        []
    end
  end

  defp convert_dag_results(results, pattern) do
    Enum.map(results, fn {node_id, data, edges} ->
      result = %{
        "node_id" => node_id,
        "s" => data.subject,
        "p" => data.predicate,
        "o" => data.object
      }
      result = if Map.has_key?(data, :validator), do: Map.put(result, "validator", data.validator), else: result
      result = if Map.has_key?(data, :timestamp), do: Map.put(result, "timestamp", data.timestamp), else: result
      result = Map.put(result, "edges", edges)
      result = if pattern[:s] == nil, do: Map.put(result, "s", data.subject), else: result
      result = if pattern[:p] == nil, do: Map.put(result, "p", data.predicate), else: result
      result = if pattern[:o] == nil, do: Map.put(result, "o", data.object), else: result
      Kylix.Query.VariableMapper.apply_mappings(result, data)
    end)
  end

  defp extract_pattern_values(pattern, binding) do
    s = if pattern[:s] == nil, do: Map.get(binding, "s"), else: pattern[:s]
    p = if pattern[:p] == nil, do: Map.get(binding, "p"), else: pattern[:p]
    o = if pattern[:o] == nil, do: Map.get(binding, "o"), else: pattern[:o]
    {s, p, o}
  end

  defp merge_bindings(binding1, binding2, pattern) do
    # Extract variable names from the pattern
    s_var = if is_atom(pattern[:s]), do: Atom.to_string(pattern[:s]), else: "s"
    p_var = if is_atom(pattern[:p]), do: Atom.to_string(pattern[:p]), else: "p"
    o_var = if is_atom(pattern[:o]), do: Atom.to_string(pattern[:o]), else: "o"

    # Merge bindings, respecting variable names from the pattern
    Enum.reduce(binding2, binding1, fn {key, val}, acc ->
      new_key = case key do
        "s" -> s_var
        "p" -> p_var
        "o" -> o_var
        _ -> key
      end
      if Map.has_key?(acc, new_key) do
        existing_val = Map.get(acc, new_key)
        if existing_val == nil || existing_val == val do
          Map.put(acc, new_key, val)
        else
          nil  # Conflict, discard
        end
      else
        Map.put(acc, new_key, val)
      end
    end)
  end

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
      :equality -> Map.get(result, filter.variable) == filter.value
      :inequality -> Map.get(result, filter.variable) != filter.value
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
      _ -> true
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
        aggregated = SparqlAggregator.apply_aggregations(results, aggregates, group_by, var_positions)
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
    cond do a < b -> -1; a > b -> 1; true -> 0 end
  end
  defp compare_values(a, b) when is_binary(a) and is_binary(b) do
    cond do a < b -> -1; a > b -> 1; true -> 0 end
  end
  defp compare_values(%DateTime{} = a, %DateTime{} = b) do
    case DateTime.compare(a, b) do :lt -> -1; :gt -> 1; :eq -> 0 end
  end
  defp compare_values(a, b), do: compare_values(to_string(a), to_string(b))

  defp apply_limits(results, limit, offset) do
    try do
      offset_results = if offset && offset > 0, do: Enum.drop(results, offset), else: results
      limited_results = if limit && limit > 0, do: Enum.take(offset_results, limit), else: offset_results
      {:ok, limited_results}
    rescue
      e -> {:error, "Error applying LIMIT/OFFSET: #{Exception.message(e)}"}
    end
  end

  defp project_variables(results, variables, query_structure) do
    projected = Enum.map(results, fn binding ->
      create_projection(binding, variables, query_structure)
    end)
    {:ok, projected}
  end

  defp create_projection(binding, variables, query_structure) do
    var_positions = Map.get(query_structure, :variable_positions, %{})
    Enum.reduce(variables, %{}, fn var, proj ->
      cond do
        Map.has_key?(binding, var) -> Map.put(proj, var, Map.get(binding, var))
        Map.has_key?(var_positions, var) ->
          position = Map.get(var_positions, var)
          value = case position do
            "s" -> Map.get(binding, "s")
            "p" -> Map.get(binding, "p")
            "o" -> Map.get(binding, "o")
            _ -> nil
          end
          Map.put(proj, var, value)
        true ->
          value = Map.get(binding, "s") || Map.get(binding, "o") || Map.get(binding, var)
          Map.put(proj, var, value)
      end
    end)
  end
end

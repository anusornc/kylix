defmodule Kylix.Query.SparqlParser do
  @moduledoc """
  SPARQL Parser for Kylix blockchain queries.

  Parses SPARQL query strings into structured query objects that can be executed
  against the Kylix blockchain storage.
  """

  require Logger

  @doc """
  Parses a SPARQL query string into a structured query representation.

  ## Parameters

  - query: A SPARQL query string

  ## Returns

  - {:ok, query_structure} if parsing is successful
  - {:error, reason} if parsing fails
  """
  def parse(query) do
    try do
      # Normalize the query string
      normalized_query = normalize_query(query)

      # Determine query type and parse accordingly
      parse_by_type(normalized_query)
    rescue
      e -> {:error, "SPARQL parse error: #{Exception.message(e)}"}
    end
  end

  # Normalize query by trimming and normalizing whitespace
  defp normalize_query(query) do
    query
    |> String.trim()
    |> String.replace(~r/\s+/, " ")
  end

  # Parse query based on its type (SELECT, CONSTRUCT, etc.)
  defp parse_by_type(query) do
    cond do
      String.match?(query, ~r/^\s*SELECT\s+/i) ->
        parse_select_query(query)

      String.match?(query, ~r/^\s*CONSTRUCT\s+/i) ->
        {:error, "CONSTRUCT queries are not yet supported"}

      String.match?(query, ~r/^\s*ASK\s+/i) ->
        {:error, "ASK queries are not yet supported"}

      String.match?(query, ~r/^\s*DESCRIBE\s+/i) ->
        {:error, "DESCRIBE queries are not yet supported"}

      true ->
        {:error, "Unsupported query type or invalid SPARQL syntax"}
    end
  end

  @doc """
  Parses a SELECT query into its component parts.
  """
  def parse_select_query(query) do
    with {:ok, parts} <- extract_query_parts(query),
         {:ok, variables} <- parse_variables(parts.variables),
         {:ok, clauses} <- extract_clauses(parts.where),
         {:ok, metadata} <- parse_metadata(parts.rest, parts.variables) do
      # Extract variables_map from clauses
      variables_map = Map.get(clauses, :variables_map, %{})

      # Build the query structure
      query_structure = %{
        # All existing fields
        type: :select,
        variables: variables,
        patterns: clauses.patterns,
        filters: clauses.filters,
        optionals: clauses.optionals,
        unions: clauses.unions,
        has_aggregates: metadata.has_aggregates,
        aggregates: metadata.aggregates,
        group_by: metadata.group_by,
        order_by: metadata.order_by,
        limit: metadata.limit,
        offset: metadata.offset,
        pattern_filters: clauses.pattern_filters,

        # Add the variable positions
        variable_positions: variables_map
      }

      {:ok, query_structure}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  # Extract main parts of the SELECT query with balanced brace handling
  defp extract_query_parts(query) do
    # Find the start position of "WHERE {"
    where_start = Regex.run(~r/WHERE\s*\{/i, query, return: :index)

    if where_start do
      [{start_pos, len}] = where_start
      where_start_pos = start_pos + len

      # Extract variables part
      variables_part = String.slice(query, 0, start_pos)

      variables =
        String.trim(variables_part)
        |> String.replace("SELECT ", "")
        |> String.trim()

      # Get the rest of the query after WHERE {
      rest_of_query = String.slice(query, where_start_pos, String.length(query) - where_start_pos)

      # Extract content between balanced braces
      {where_clause, remaining} = extract_between_braces(rest_of_query)

      Logger.debug("WHERE clause: #{where_clause}")

      {:ok, %{variables: variables, where: where_clause, rest: remaining}}
    else
      {:error, "Invalid SELECT query format: Cannot find WHERE clause"}
    end
  end

  # Extract content between balanced braces
  defp extract_between_braces(str) do
    # Helper function to extract content between balanced braces
    extract_balanced(str, 1, "", "")
  end

  defp extract_balanced("", _, acc, rest), do: {acc, rest}

  defp extract_balanced(<<"{", rest::binary>>, depth, acc, remaining) do
    extract_balanced(rest, depth + 1, acc <> "{", remaining)
  end

  defp extract_balanced(<<"}", rest::binary>>, 1, acc, _) do
    # Found the matching closing brace at the right depth
    {acc, rest}
  end

  defp extract_balanced(<<"}", rest::binary>>, depth, acc, remaining) do
    extract_balanced(rest, depth - 1, acc <> "}", remaining)
  end

  defp extract_balanced(<<char::utf8, rest::binary>>, depth, acc, remaining) do
    extract_balanced(rest, depth, acc <> <<char::utf8>>, remaining)
  end

  # Extract all clauses from the WHERE part
  defp extract_clauses(where_str) do
    # Current implementation with variable positions added
    all_filters = extract_filters(where_str)
    optionals = extract_optionals(where_str)
    unions = extract_unions(where_str)

    clean_str =
      where_str
      |> String.replace(~r/OPTIONAL\s*\{[^{}]+\}/i, "")
      |> String.replace(
        ~r/\{\s*[^{}]*(?:\{[^{}]*\}[^{}]*)*\s*\}\s+UNION\s+\{\s*[^{}]*(?:\{[^{}]*\}[^{}]*)*\s*\}/i,
        ""
      )
      |> String.replace(~r/FILTER\s*\(\s*[^()]*(?:\([^()]*\)[^()]*)*\s*\)/i, "")

    Logger.debug("Clean string for basic patterns: #{clean_str}")

    # Replace this line with the new version that tracks variable positions
    {patterns, variables_map} =
      extract_triple_patterns_with_positions(clean_str, optionals, unions, all_filters)

    pattern_filters = attach_filters_to_patterns(patterns, all_filters)

    {:ok,
     %{
       optionals: optionals,
       unions: unions,
       filters: all_filters,
       patterns: patterns,
       pattern_filters: pattern_filters,
       variables_map: variables_map
     }}
  end

  # Helper to attach filters to patterns
  defp attach_filters_to_patterns(patterns, filters) do
    # Simple approach: attach all filters to the first pattern
    if Enum.empty?(patterns) do
      []
    else
      first_pattern = hd(patterns)
      rest_patterns = tl(patterns)

      [%{pattern: first_pattern, filters: filters}] ++
        Enum.map(rest_patterns, fn pattern -> %{pattern: pattern, filters: []} end)
    end
  end

  # Implement the function to extract patterns with positions
  defp extract_triple_patterns_with_positions(str, _optionals, _unions, _filters) do
    # Initialize an empty variables map
    variables_map = %{}

    # Split the string by period, but keep track of the original positions
    pattern_strings_with_positions =
      str
      |> String.split(".")
      |> Enum.map(&String.trim/1)
      |> Enum.filter(&(String.length(&1) > 0))
      |> Enum.with_index()

    # Process each pattern and accumulate variable positions
    {patterns_with_positions, final_variables_map} =
      Enum.reduce(pattern_strings_with_positions, {[], variables_map}, fn {pattern_str, pos},
                                                                          {acc, vars_map} ->
        {pattern, updated_vars_map} = parse_triple(pattern_str, vars_map)

        if pattern != nil do
          {[{pattern, pos} | acc], updated_vars_map}
        else
          {acc, vars_map}
        end
      end)

    # Sort patterns by original position
    patterns =
      patterns_with_positions
      |> Enum.sort_by(fn {_, pos} -> pos end)
      |> Enum.map(fn {pattern, _} -> pattern end)

    {patterns, final_variables_map}
  end

  # Parse metadata like GROUP BY, ORDER BY, LIMIT, OFFSET
  defp parse_metadata(rest_str, variables_str) do
    has_aggregates = Regex.match?(~r/(COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT)\s*\(/i, variables_str)

    aggregates = if has_aggregates, do: parse_aggregates(variables_str), else: []
    group_by = parse_group_by(rest_str)
    order_by = parse_order_by(rest_str)
    {limit, offset} = parse_limit_offset(rest_str)

    {:ok,
     %{
       has_aggregates: has_aggregates,
       aggregates: aggregates,
       group_by: group_by,
       order_by: order_by,
       limit: limit,
       offset: offset
     }}
  end

  # Build the final query structure with all required keys
  # defp build_query_structure(variables, clauses, metadata, variables_map) do
  #   %{
  #     type: :select,
  #     variables: variables,
  #     patterns: clauses.patterns,
  #     filters: clauses.filters,
  #     optionals: clauses.optionals,
  #     unions: clauses.unions,
  #     has_aggregates: metadata.has_aggregates,
  #     aggregates: metadata.aggregates,
  #     group_by: metadata.group_by,
  #     order_by: metadata.order_by,
  #     limit: metadata.limit,
  #     offset: metadata.offset,
  #     pattern_filters: clauses.pattern_filters,
  #     # Add this field
  #     variable_positions: variables_map
  #   }
  # end

  # Extract OPTIONAL clauses
  defp extract_optionals(where_str) do
    # Pattern to match OPTIONAL clauses
    optional_pattern = ~r/OPTIONAL\s*\{([^{}]+)\}/i

    Regex.scan(optional_pattern, where_str, capture: :all_but_first)
    |> List.flatten()
    |> Enum.map(fn inner_content ->
      # This line now needs to handle the tuple return value
      {patterns, _} = extract_triple_patterns_with_positions(inner_content, [], [], [])
      optional_filters = extract_filters(inner_content)

      %{patterns: patterns, filters: optional_filters}
    end)
  end

  # Extract UNION clauses
  defp extract_unions(where_str) do
    # Pattern to match UNION clauses
    union_pattern = ~r/\{\s*([^{}]*(?:\{[^{}]*\}[^{}]*)*)\s*\}\s+UNION\s+\{\s*([^{}]*(?:\{[^{}]*\}[^{}]*)*)\s*\}/i

    Regex.scan(union_pattern, where_str, capture: :all_but_first)
    |> Enum.map(fn [left_content, right_content] ->
      # These lines now need to handle the tuple return values
      {left_patterns, _} = extract_triple_patterns_with_positions(left_content, [], [], [])
      {right_patterns, _} = extract_triple_patterns_with_positions(right_content, [], [], [])

      # Also extract any filters in each UNION branch
      left_filters = extract_filters(left_content)
      right_filters = extract_filters(right_content)

      %{
        left: %{patterns: left_patterns, filters: left_filters},
        right: %{patterns: right_patterns, filters: right_filters}
      }
    end)
  end

  # Extract FILTER clauses
  defp extract_filters(str) do
    # Pattern to match FILTER expressions
    filter_pattern = ~r/FILTER\s*\(\s*([^()]*(?:\([^()]*\)[^()]*)*)\s*\)/i

    Regex.scan(filter_pattern, str, capture: :all_but_first)
    |> List.flatten()
    |> Enum.map(&parse_filter_expression/1)
  end

  # Parse a filter expression into a structured filter
  defp parse_filter_expression(expr) do
    cond do
      # Handle equality filter
      String.contains?(expr, "=") && !String.contains?(expr, "!=") ->
        [var, value] = String.split(expr, "=", parts: 2)
        var = String.trim(var)
        value = String.trim(value)

        %{
          type: :equality,
          variable: extract_var_name(var),
          value: extract_value(value),
          # Store original expression for reconstruction
          expression: expr
        }

      # Handle inequality filter
      String.contains?(expr, "!=") ->
        [var, value] = String.split(expr, "!=", parts: 2)
        var = String.trim(var)
        value = String.trim(value)

        %{
          type: :inequality,
          variable: extract_var_name(var),
          value: extract_value(value),
          # Store original expression for reconstruction
          expression: expr
        }

      # Handle regex filter
      String.match?(expr, ~r/regex\s*\(/i) ->
        # Extract variable and pattern from regex
        regex_pattern = ~r/regex\s*\(\s*\?([^\s,\)]+)\s*,\s*["']([^"']+)["']\s*\)/i
        captures = Regex.named_captures(regex_pattern, expr)

        if captures do
          %{
            type: :regex,
            variable: captures["1"],
            pattern: captures["2"],
            # Store original expression for reconstruction
            expression: expr
          }
        else
          %{type: :unknown, expression: expr}
        end

      # Unknown filter type
      true ->
        %{type: :unknown, expression: expr}
    end
  end

  # Extract variable name from ?var format
  defp extract_var_name(var_str) do
    if String.starts_with?(var_str, "?") do
      String.slice(var_str, 1, String.length(var_str) - 1)
    else
      var_str
    end
  end

  # Extract value (remove quotes if present)
  defp extract_value(value_str) do
    cond do
      String.starts_with?(value_str, "\"") && String.ends_with?(value_str, "\"") ->
        String.slice(value_str, 1, String.length(value_str) - 2)

      String.starts_with?(value_str, "'") && String.ends_with?(value_str, "'") ->
        String.slice(value_str, 1, String.length(value_str) - 2)

      true ->
        value_str
    end
  end

  # Extract basic triple patterns considering complex structures
  # defp extract_basic_patterns(where_str, optionals, unions, filters) do
  #   # Clean string by removing complex structures
  #   clean_str = remove_complex_structures(where_str, optionals, unions, filters)
  #   Logger.debug("Clean string for basic patterns: #{clean_str}")
  #   extract_triple_patterns(clean_str, optionals, unions, filters)
  # end

  # Extract triple patterns from a string - IMPORTANT: REVERSE the patterns
  # defp extract_triple_patterns(str, _optionals, _unions, _filters) do
  #   # Initialize an empty variables map
  #   variables_map = %{}

  #   # Split the string by period, but keep track of the original positions
  #   pattern_strings_with_positions =
  #     str
  #     |> String.split(".")
  #     |> Enum.map(&String.trim/1)
  #     |> Enum.filter(&(String.length(&1) > 0))
  #     |> Enum.with_index()

  #   # Parse each pattern with its position
  #   {patterns_with_positions, variables_map} =
  #     Enum.reduce(pattern_strings_with_positions, {[], variables_map}, fn {pattern_str, pos},
  #                                                                         {acc, vars_map} ->
  #       {pattern, updated_vars_map} = parse_triple(pattern_str, vars_map)
  #       {[{pattern, pos} | acc], updated_vars_map}
  #     end)

  #   # Sort patterns by original position
  #   patterns =
  #     patterns_with_positions
  #     |> Enum.filter(fn {pattern, _} -> pattern != nil end)
  #     |> Enum.sort_by(fn {_, pos} -> pos end)
  #     |> Enum.map(fn {pattern, _} -> pattern end)

  #   {patterns, variables_map}
  # end

  # Parse a triple string into a pattern structure
  defp parse_triple(triple, variables_map) do
    parts = String.split(triple, ~r/\s+/)

    if length(parts) >= 3 do
      s_part = Enum.at(parts, 0)
      p_part = Enum.at(parts, 1)
      o_part = Enum.at(parts, 2)

      # Track variable positions
      variables_map = update_variable_positions(s_part, "s", variables_map)
      variables_map = update_variable_positions(p_part, "p", variables_map)
      variables_map = update_variable_positions(o_part, "o", variables_map)

      pattern = %{
        s: parse_component(s_part),
        p: parse_component(p_part),
        o: parse_component(o_part)
      }

      # Return the pattern and updated variables map
      {pattern, variables_map}
    else
      {nil, variables_map}
    end
  end

  defp update_variable_positions(part, position, variables_map) do
    if String.starts_with?(part, "?") do
      # Note the //1 syntax for Elixir 1.18
      var_name = String.slice(part, 1..-1//1)
      Map.put(variables_map, var_name, position)
    else
      variables_map
    end
  end

  # Remove complex structures considering the already extracted parts
  # defp clean_string_for_patterns(str, optionals, unions, filters) do
  #   # Remove OPTIONAL blocks
  #   str_without_optionals = Enum.reduce(optionals, str, fn _, acc ->
  #     String.replace(acc, ~r/OPTIONAL\s*\{[^{}]+\}/i, "")
  #   end)

  #   # Remove UNION blocks
  #   str_without_unions = Enum.reduce(unions, str_without_optionals, fn _, acc ->
  #     String.replace(acc, ~r/\{\s*[^{}]*(?:\{[^{}]*\}[^{}]*)*\s*\}\s+UNION\s+\{\s*[^{}]*(?:\{[^{}]*\}[^{}]*)*\s*\}/i, "")
  #   end)

  #   # Remove FILTER expressions
  #   Enum.reduce(filters, str_without_unions, fn _, acc ->
  #     String.replace(acc, ~r/FILTER\s*\(\s*[^()]*(?:\([^()]*\)[^()]*)*\s*\)/i, "")
  #   end)
  # end

  # Parse a triple pattern component (subject, predicate, or object)
  defp parse_component(component) do
    cond do
      # For variable, tests expect nil
      String.starts_with?(component, "?") ->
        nil

      # For quoted literals, remove the quotes
      String.starts_with?(component, "\"") && String.ends_with?(component, "\"") ->
        String.slice(component, 1, String.length(component) - 2)

      String.starts_with?(component, "'") && String.ends_with?(component, "'") ->
        String.slice(component, 1, String.length(component) - 2)

      # Everything else, just return as-is
      true ->
        component
    end
  end

  # Parse variables from SELECT clause
  defp parse_variables(vars_str) do
    # Extract aggregate aliases
    aggregate_aliases =
      Regex.scan(~r/\([^)]+\s+AS\s+\?([^\s\)]+)\)/i, vars_str, capture: :all_but_first)
      |> List.flatten()

    # Extract standalone variables
    standalone_vars =
      Regex.scan(~r/\s\?([^\s\(\)]+)/i, " " <> vars_str, capture: :all_but_first)
      |> List.flatten()
      |> Enum.filter(fn var ->
        # Filter out variables that might be part of expressions
        !String.contains?(vars_str, "(#{var})")
      end)

    # Combine all variables (removing duplicates)
    variables = (aggregate_aliases ++ standalone_vars) |> Enum.uniq()

    {:ok, variables}
  end

  # Parse aggregation expressions with better alias handling
  def parse_aggregates(vars_str) do
    # Look for full expressions like (COUNT(?x) AS ?y)
    full_pattern = ~r/\((\w+)\(\??([^\)]+)\)(?:\s+AS\s+\?([^\s\)]+))?\)/i

    Regex.scan(full_pattern, vars_str)
    |> Enum.map(fn match ->
      case match do
        [_, function, variable, ""] ->
          # No alias specified, use default
          %{
            function: String.downcase(function) |> String.to_atom(),
            variable: variable,
            distinct: false,
            alias: "#{String.downcase(function)}_#{variable}"
          }

        [_, function, variable, alias_name] when alias_name != "" ->
          # With explicit alias
          %{
            function: String.downcase(function) |> String.to_atom(),
            variable: variable,
            distinct: false,
            alias: alias_name
          }

        [_, function, variable | _] ->
          # Default case
          %{
            function: String.downcase(function) |> String.to_atom(),
            variable: variable,
            distinct: false,
            alias: "#{String.downcase(function)}_#{variable}"
          }
      end
    end)
  end

  # Parse GROUP BY clause
  def parse_group_by(query_part) do
    pattern = ~r/GROUP\s+BY\s+(?<vars>[^;)]+?)(\s+ORDER\s+BY|\s+LIMIT|\s+OFFSET|$)/i

    case Regex.named_captures(pattern, query_part) do
      %{"vars" => vars} ->
        vars
        |> String.split(",")
        |> Enum.map(&String.trim/1)
        |> Enum.map(fn var ->
          var = String.trim(var)

          if String.starts_with?(var, "?"),
            do: String.slice(var, 1, String.length(var) - 1),
            else: var
        end)

      nil ->
        []
    end
  end

  # Parse ORDER BY clause
  def parse_order_by(query_part) do
    pattern = ~r/ORDER\s+BY\s+(?<ordering>[^;)]+?)(\s+LIMIT|\s+OFFSET|$)/i

    case Regex.named_captures(pattern, query_part) do
      %{"ordering" => ordering} ->
        ordering
        |> String.split(",")
        |> Enum.map(&String.trim/1)
        |> Enum.map(fn item ->
          cond do
            String.starts_with?(item, "DESC(") ->
              var = Regex.run(~r/DESC\(\s*\?([^\s\)]+)\s*\)/i, item)

              if var && length(var) > 1,
                do: %{variable: Enum.at(var, 1), direction: :desc},
                else: nil

            String.starts_with?(item, "ASC(") ->
              var = Regex.run(~r/ASC\(\s*\?([^\s\)]+)\s*\)/i, item)

              if var && length(var) > 1,
                do: %{variable: Enum.at(var, 1), direction: :asc},
                else: nil

            String.starts_with?(item, "?") ->
              %{variable: String.slice(item, 1, String.length(item) - 1), direction: :asc}

            true ->
              nil
          end
        end)
        |> Enum.filter(&(&1 != nil))

      nil ->
        []
    end
  end

  # Parse LIMIT and OFFSET clauses
  def parse_limit_offset(query_part) do
    # Extract LIMIT
    limit_pattern = ~r/LIMIT\s+(?<limit>\d+)/i

    limit =
      case Regex.named_captures(limit_pattern, query_part) do
        %{"limit" => limit_str} -> String.to_integer(limit_str)
        nil -> nil
      end

    # Extract OFFSET
    offset_pattern = ~r/OFFSET\s+(?<offset>\d+)/i

    offset =
      case Regex.named_captures(offset_pattern, query_part) do
        %{"offset" => offset_str} -> String.to_integer(offset_str)
        nil -> nil
      end

    {limit, offset}
  end
end

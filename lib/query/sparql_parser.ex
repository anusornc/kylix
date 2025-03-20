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
      # Normalize the query string by trimming whitespace and ensuring single spaces
      normalized_query = query
      |> String.trim()
      |> String.replace(~r/\s+/, " ")

      # Check query type (currently only supporting SELECT)
      cond do
        String.starts_with?(normalized_query, "SELECT ") ->
          parse_select_query(normalized_query)

        # Future support for other query types
        String.starts_with?(normalized_query, "CONSTRUCT ") ->
          {:error, "CONSTRUCT queries are not yet supported"}

        String.starts_with?(normalized_query, "ASK ") ->
          {:error, "ASK queries are not yet supported"}

        String.starts_with?(normalized_query, "DESCRIBE ") ->
          {:error, "DESCRIBE queries are not yet supported"}

        true ->
          {:error, "Unsupported query type or invalid SPARQL syntax"}
      end
    rescue
      e -> {:error, "SPARQL parse error: #{Exception.message(e)}"}
    end
  end

  @doc """
  Parses a SELECT query into its component parts.
  """
  def parse_select_query(query) do
    # Extract the SELECT variables section and full query for additional clauses
    select_pattern = ~r/SELECT\s+(?<variables>.+?)\s+WHERE\s+\{(?<where_clause>.+?)\}(?<rest>.*)/is

    case Regex.named_captures(select_pattern, query) do
      %{"variables" => vars, "where_clause" => where, "rest" => rest} ->
        # Get variable names from SELECT clause
        variables = parse_variables(vars)

        # First extract all special blocks
        optionals = extract_optionals(where)
        unions = extract_unions(where)
        filters = extract_filters(where)

        # Get basic patterns from what remains
        basic_patterns = extract_basic_patterns(where, optionals, unions, filters)

        # Check for aggregates
        has_aggregates = Regex.match?(~r/(COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT)\s*\(/i, vars)

        # Parse other clauses
        group_by = parse_group_by(rest)
        order_by = parse_order_by(rest)
        {limit, offset} = parse_limit_offset(rest)

        # Construct the query structure
        query_structure = %{
          type: :select,
          variables: variables,
          patterns: basic_patterns,
          filters: filters,
          optionals: optionals,
          unions: unions,
          has_aggregates: has_aggregates,
          group_by: group_by,
          order_by: order_by,
          limit: limit,
          offset: offset
        }

        # Handle aggregates if present
        query_structure = if has_aggregates do
          #alias Kylix.Query.SparqlAggregator

          # Parse the count expression based on the full variable clause
          # This handles "SELECT ?person (COUNT(?x) AS ?y)" format
          aggregates = parse_aggregates(vars)

          # Add aggregates to query structure
          Map.put(query_structure, :aggregates, aggregates)
        else
          query_structure
        end

        {:ok, query_structure}

      nil ->
        {:error, "Invalid SELECT query format"}
    end
  end

  # Extract OPTIONAL clauses from WHERE
  defp extract_optionals(where_str) do
    # This improved regex works better for nested braces
    pattern = ~r/OPTIONAL\s*\{([^{}]|\{[^{}]*\})*\}/s

    # Find all matches
    Regex.scan(pattern, where_str)
    |> Enum.map(fn [optional_text] ->
      # Extract just the content between OPTIONAL { and }
      content = Regex.run(~r/OPTIONAL\s*\{(.*)\}\s*$/s, optional_text)

      if content && length(content) >= 2 do
        inner_content = Enum.at(content, 1)

        # Parse the contents for patterns and filters
        inner_patterns = extract_basic_patterns(inner_content, [], [], [])
        inner_filters = extract_filters(inner_content)

        %{patterns: inner_patterns, filters: inner_filters}
      else
        %{patterns: [], filters: []}
      end
    end)
  end

  # Extract UNION blocks from WHERE
  defp extract_unions(where_str) do
    # Match expressions like { ... } UNION { ... }
    pattern = ~r/\{\s*([^{}]|\{[^{}]*\})*\}\s+UNION\s+\{\s*([^{}]|\{[^{}]*\})*\}/s

    Regex.scan(pattern, where_str)
    |> Enum.map(fn [union_text] ->
      # Split into left and right parts
      parts = Regex.run(~r/\{\s*(.*?)\s*\}\s+UNION\s+\{\s*(.*?)\s*\}/s, union_text)

      if parts && length(parts) >= 3 do
        left_content = Enum.at(parts, 1)
        right_content = Enum.at(parts, 2)

        # Parse each side
        left_patterns = extract_basic_patterns(left_content, [], [], [])
        left_filters = extract_filters(left_content)

        right_patterns = extract_basic_patterns(right_content, [], [], [])
        right_filters = extract_filters(right_content)

        %{
          left: %{patterns: left_patterns, filters: left_filters},
          right: %{patterns: right_patterns, filters: right_filters}
        }
      else
        %{left: %{patterns: [], filters: []}, right: %{patterns: [], filters: []}}
      end
    end)
  end

  # Extract FILTER expressions from WHERE
  defp extract_filters(where_str) do
    pattern = ~r/FILTER\s*\(\s*([^()]*|\([^()]*\))*\)/s

    Regex.scan(pattern, where_str)
    |> Enum.map(fn [filter_text] ->
      # Extract the actual expression
      content = Regex.run(~r/FILTER\s*\(\s*(.*)\s*\)/s, filter_text)

      if content && length(content) >= 2 do
        expr = Enum.at(content, 1)
        parse_filter_expression(expr)
      else
        %{type: :unknown, expression: filter_text}
      end
    end)
  end

  # Extract basic triple patterns
  defp extract_basic_patterns(where_str, _optionals, _unions, _filters) do
    # First clean the string by removing OPTIONAL, UNION, and FILTER blocks
    clean_str = where_str
    |> remove_optional_blocks()
    |> remove_union_blocks()
    |> remove_filter_blocks()

    # Split by dots and process each triple pattern
    String.split(clean_str, ".")
    |> Enum.map(&String.trim/1)
    |> Enum.filter(fn s -> String.length(s) > 0 end)
    |> Enum.flat_map(fn pattern ->
      # Split into components
      components = pattern
      |> String.split(~r/\s+/)
      |> Enum.map(&String.trim/1)
      |> Enum.filter(fn s -> String.length(s) > 0 end)

      # If we have a valid triple (3+ components), parse it
      if length(components) >= 3 do
        s = parse_component(Enum.at(components, 0))
        p = parse_component(Enum.at(components, 1))
        o = parse_component(Enum.at(components, 2))

        [%{s: s, p: p, o: o}]
      else
        []
      end
    end)
  end

  # Parse a triple pattern component (subject, predicate, or object)
  defp parse_component(component) do
    cond do
      # Variable (starts with ?)
      String.starts_with?(component, "?") ->
        nil

      # Quoted literal
      String.starts_with?(component, "\"") && String.ends_with?(component, "\"") ->
        String.slice(component, 1..-2//1)

      # URI (angle brackets)
      String.starts_with?(component, "<") && String.ends_with?(component, ">") ->
        String.slice(component, 1..-2//1)

      # Prefixed name
      String.contains?(component, ":") ->
        component

      # Other literals
      true ->
        component
    end
  end

  # Remove OPTIONAL blocks for clean triple pattern extraction
  defp remove_optional_blocks(str) do
    pattern = ~r/OPTIONAL\s*\{([^{}]|\{[^{}]*\})*\}/s
    Regex.replace(pattern, str, "")
  end

  # Remove UNION blocks for clean triple pattern extraction
  defp remove_union_blocks(str) do
    pattern = ~r/\{\s*([^{}]|\{[^{}]*\})*\}\s+UNION\s+\{\s*([^{}]|\{[^{}]*\})*\}/s
    Regex.replace(pattern, str, "")
  end

  # Remove FILTER blocks for clean triple pattern extraction
  defp remove_filter_blocks(str) do
    pattern = ~r/FILTER\s*\(\s*([^()]*|\([^()]*\))*\)/s
    Regex.replace(pattern, str, "")
  end

  # Parse a FILTER expression
  defp parse_filter_expression(expr) do
    # Handle inequality filter (must check first since it contains =)
    if String.contains?(expr, "!=") do
      [var, value] = String.split(expr, "!=", parts: 2)
      %{
        type: :inequality,
        variable: extract_var_name(String.trim(var)),
        value: extract_value(String.trim(value))
      }

    # Handle equality filter
    else if String.contains?(expr, "=") do
      [var, value] = String.split(expr, "=", parts: 2)
      %{
        type: :equality,
        variable: extract_var_name(String.trim(var)),
        value: extract_value(String.trim(value))
      }

    # Handle regex
    else if String.match?(expr, ~r/^REGEX\s*\(/i) do
      regex_parts = Regex.named_captures(~r/REGEX\s*\(\s*\?(?<var>[^\s,]+)\s*,\s*"(?<pattern>[^"]+)"/i, expr)

      if regex_parts do
        %{
          type: :regex,
          variable: regex_parts["var"],
          pattern: regex_parts["pattern"]
        }
      else
        %{type: :unknown, expression: expr}
      end

    # Other filter types
    else
      %{type: :unknown, expression: expr}
    end
    end
    end
  end

  # Extract variable name from ?var format
  defp extract_var_name(var_str) do
    if String.starts_with?(var_str, "?") do
      String.slice(var_str, 1..-1//1)
    else
      var_str
    end
  end

  # Extract value from quoted or variable format
  defp extract_value(value_str) do
    cond do
      # Quoted value
      String.starts_with?(value_str, "\"") && String.ends_with?(value_str, "\"") ->
        String.slice(value_str, 1..-2//1)

      # Variable
      String.starts_with?(value_str, "?") ->
        value_str

      # Other value
      true ->
        value_str
    end
  end

  # Parse variables from SELECT clause
  def parse_variables(vars_str) do
    # Handle both ?var format and aggregates like (COUNT(?x) AS ?y)
    # First try to extract just the variable names
    vars_str
    |> String.split(~r/\s+/)
    |> Enum.map(&String.trim/1)
    |> Enum.filter(&(String.length(&1) > 0))
    |> Enum.map(fn var ->
      # Remove the ? prefix if present
      if String.starts_with?(var, "?") do
        String.slice(var, 1..-1//1)
      else
        var
      end
    end)
  end

  # Parse aggregation expressions
  def parse_aggregates(vars_str) do
    # Extract expressions like (COUNT(?x) AS ?y)
    pattern = ~r/\(([^()]+)\)/s

    Regex.scan(pattern, vars_str)
    |> Enum.map(fn [_full, inner] ->
      if String.match?(inner, ~r/^COUNT|SUM|AVG|MIN|MAX/i) do
        # Split by AS if present
        parts = String.split(inner, ~r/\s+AS\s+/i)

        if length(parts) >= 2 do
          # We have an AS clause
          func_expr = Enum.at(parts, 0)
          alias_expr = Enum.at(parts, 1)

          # Parse the function part
          {func, var, distinct} = parse_aggregate_function(func_expr)

          # Extract the alias without ? prefix
          alias_name = extract_var_name(alias_expr)

          %{
            function: func,
            variable: var,
            distinct: distinct,
            alias: alias_name
          }
        else
          # No AS clause, use default alias
          {func, var, distinct} = parse_aggregate_function(inner)

          %{
            function: func,
            variable: var,
            distinct: distinct,
            alias: "#{func}_#{if distinct, do: "distinct_", else: ""}#{var}"
          }
        end
      else
        # Unknown aggregation
        %{
          function: :unknown,
          variable: nil,
          distinct: false,
          alias: "unknown"
        }
      end
    end)
  end

  # Parse aggregate function parts
  defp parse_aggregate_function(expr) do
    # Handle COUNT(DISTINCT ?var) or COUNT(?var)
    count_pattern = ~r/^COUNT\s*\(\s*(?<distinct>DISTINCT\s+)?\?(?<var>[^\s\)]+)\s*\)$/i

    cond do
      String.match?(expr, count_pattern) ->
        parts = Regex.named_captures(count_pattern, expr)
        {:count, parts["var"], parts["distinct"] != ""}

      String.match?(expr, ~r/^SUM\s*\(\s*\?([^\s\)]+)\s*\)$/i) ->
        var = Regex.run(~r/^SUM\s*\(\s*\?([^\s\)]+)\s*\)$/i, expr) |> Enum.at(1)
        {:sum, var, false}

      String.match?(expr, ~r/^AVG\s*\(\s*\?([^\s\)]+)\s*\)$/i) ->
        var = Regex.run(~r/^AVG\s*\(\s*\?([^\s\)]+)\s*\)$/i, expr) |> Enum.at(1)
        {:avg, var, false}

      String.match?(expr, ~r/^MIN\s*\(\s*\?([^\s\)]+)\s*\)$/i) ->
        var = Regex.run(~r/^MIN\s*\(\s*\?([^\s\)]+)\s*\)$/i, expr) |> Enum.at(1)
        {:min, var, false}

      String.match?(expr, ~r/^MAX\s*\(\s*\?([^\s\)]+)\s*\)$/i) ->
        var = Regex.run(~r/^MAX\s*\(\s*\?([^\s\)]+)\s*\)$/i, expr) |> Enum.at(1)
        {:max, var, false}

      true ->
        {:unknown, nil, false}
    end
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
          if String.starts_with?(var, "?"), do: String.slice(var, 1..-1//1), else: var
        end)

      nil -> []
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
              if var && length(var) > 1, do: %{variable: Enum.at(var, 1), direction: :desc}, else: nil

            String.starts_with?(item, "ASC(") ->
              var = Regex.run(~r/ASC\(\s*\?([^\s\)]+)\s*\)/i, item)
              if var && length(var) > 1, do: %{variable: Enum.at(var, 1), direction: :asc}, else: nil

            String.starts_with?(item, "?") ->
              %{variable: String.slice(item, 1..-1//1), direction: :asc}

            true -> nil
          end
        end)
        |> Enum.filter(&(&1 != nil))

      nil -> []
    end
  end

  # Parse LIMIT and OFFSET clauses
  def parse_limit_offset(query_part) do
    # Extract LIMIT
    limit_pattern = ~r/LIMIT\s+(?<limit>\d+)/i
    limit = case Regex.named_captures(limit_pattern, query_part) do
      %{"limit" => limit_str} -> String.to_integer(limit_str)
      nil -> nil
    end

    # Extract OFFSET
    offset_pattern = ~r/OFFSET\s+(?<offset>\d+)/i
    offset = case Regex.named_captures(offset_pattern, query_part) do
      %{"offset" => offset_str} -> String.to_integer(offset_str)
      nil -> nil
    end

    {limit, offset}
  end
end

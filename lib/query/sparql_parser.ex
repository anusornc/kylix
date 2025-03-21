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
      normalized_query =
        query
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
    select_pattern =
      ~r/SELECT\s+(?<variables>.+?)\s+WHERE\s+\{(?<where_clause>.+?)\}(?<rest>.*)/is

    case Regex.named_captures(select_pattern, query) do
      %{"variables" => vars, "where_clause" => where, "rest" => rest} ->
        # Log for debugging
        IO.puts("WHERE clause: #{where}")

        # Get variable names from SELECT clause
        variables = parse_variables(vars)

        # Extract OPTIONAL clauses first
        optionals = extract_optionals(where)
        IO.puts("Found OPTIONAL clauses: #{inspect(optionals)}")

        # Extract other special clauses
        unions = extract_unions(where)

        # Clean the WHERE clause by removing OPTIONAL and UNION blocks for top-level filters
        clean_where_for_filters =
          where
          |> remove_optional_blocks()
          |> remove_union_blocks()

        # Extract top-level filters from the cleaned WHERE clause
        top_level_filters = extract_filters(clean_where_for_filters)

        # Get basic patterns from what remains
        basic_patterns = extract_basic_patterns(where, optionals, unions, top_level_filters)

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
          # Use top-level filters only
          filters: top_level_filters,
          optionals: optionals,
          unions: unions,
          has_aggregates: has_aggregates,
          group_by: group_by,
          order_by: order_by,
          limit: limit,
          offset: offset
        }

        # Handle aggregates if present
        query_structure =
          if has_aggregates do
            # Parse the count expression based on the full variable clause
            aggregates = parse_aggregates(vars)
            Map.put(query_structure, :aggregates, aggregates)
          else
            query_structure
          end

        # Apply optimizer to organize patterns and filters
        query_structure = Kylix.Query.SparqlOptimizer.optimize(query_structure)

        {:ok, query_structure}

      nil ->
        {:error, "Invalid SELECT query format"}
    end
  end

  # Extract OPTIONAL clauses from WHERE
  defp extract_optionals(where_str) do
    # Look for OPTIONAL { ... } pattern
    pattern = ~r/OPTIONAL\s*\{\s*([^{}]*)\s*\}/s

    # Find all matches
    Regex.scan(pattern, where_str, capture: :all_but_first)
    |> List.flatten()
    |> Enum.map(fn inner_content ->
      # Parse the contents for patterns and filters
      inner_patterns = extract_basic_patterns(inner_content, [], [], [])
      inner_filters = extract_filters(inner_content)

      %{patterns: inner_patterns, filters: inner_filters}
    end)
  end

  # Extract UNION blocks from WHERE
  defp extract_unions(where_str) do
    # Match expressions like { ... } UNION { ... }
    pattern = ~r/\{\s*([^{}]*)\s*\}\s+UNION\s+\{\s*([^{}]*)\s*\}/s

    Regex.scan(pattern, where_str, capture: :all)
    |> Enum.map(fn [_full_match, left_content, right_content] ->
      # Parse each side
      left_patterns = extract_basic_patterns(left_content, [], [], [])
      left_filters = extract_filters(left_content)

      right_patterns = extract_basic_patterns(right_content, [], [], [])
      right_filters = extract_filters(right_content)

      # Log for debugging
      Logger.debug("UNION left patterns: #{inspect(left_patterns)}")
      Logger.debug("UNION right patterns: #{inspect(right_patterns)}")

      %{
        left: %{patterns: left_patterns, filters: left_filters},
        right: %{patterns: right_patterns, filters: right_filters}
      }
    end)
  end

  # Extract FILTER expressions from a string
  defp extract_filters(str) do
    # Improved pattern to capture nested parentheses in FILTER expressions
    pattern = ~r/FILTER\s*\(\s*((?:[^()]|(?:\([^()]*\)))*)\s*\)/is

    Regex.scan(pattern, str)
    |> Enum.map(fn [_full_match, expr] ->
      parse_filter_expression(expr)
    end)
  end

  # Extract basic triple patterns
  defp extract_basic_patterns(where_str, _optionals, _unions, _filters) do
    # First clean the string by removing OPTIONAL, UNION, and FILTER blocks
    clean_str =
      where_str
      |> remove_optional_blocks()
      |> remove_union_blocks()
      |> remove_filter_blocks()

    # Log for debugging
    Logger.debug("Cleaned WHERE clause: #{clean_str}")

    # Split by dots and process each triple pattern
    triples =
      String.split(clean_str, ".")
      |> Enum.map(&String.trim/1)
      |> Enum.filter(fn s -> String.length(s) > 0 end)
      |> Enum.flat_map(fn pattern ->
        # Split into components
        components =
          pattern
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

    # Log results for debugging
    Logger.debug("Extracted triple patterns: #{inspect(triples)}")
    triples
  end

  # Parse a triple pattern component (subject, predicate, or object)
  defp parse_component(component) do
    cond do
      String.starts_with?(component, "?") ->
        {:var, String.slice(component, 1..-1//1)}

      String.starts_with?(component, "\"") && String.ends_with?(component, "\"") ->
        String.slice(component, 1..-2//1)

      String.starts_with?(component, "<") && String.ends_with?(component, ">") ->
        String.slice(component, 1..-2//1)

      String.contains?(component, ":") ->
        component

      true ->
        component
    end
  end

  # Remove OPTIONAL blocks for clean triple pattern extraction
  defp remove_optional_blocks(str) do
    # Improved regex to handle nested braces
    pattern = ~r/OPTIONAL\s*\{((?:[^{}]|(?:\{[^{}]*\}))*)\}/is
    Regex.replace(pattern, str, "")
  end

  # Remove UNION blocks for clean triple pattern extraction
  defp remove_union_blocks(str) do
    # Improved regex to handle nested braces
    pattern =
      ~r/\{\s*((?:[^{}]|(?:\{[^{}]*\}))*)\s*\}\s+UNION\s+\{\s*((?:[^{}]|(?:\{[^{}]*\}))*)\s*\}/is

    Regex.replace(pattern, str, "")
  end

  # Remove FILTER blocks for clean triple pattern extraction
  defp remove_filter_blocks(str) do
    # Improved regex to handle nested parentheses
    pattern = ~r/FILTER\s*\(\s*((?:[^()]|(?:\([^()]*\)))*)\s*\)/is
    Regex.replace(pattern, str, "")
  end

  # Parse a FILTER expression
  defp parse_filter_expression(expr) do
    # Handle inequality filter (must check first since it contains =)
    cond do
      String.contains?(expr, "!=") ->
        [var, value] = String.split(expr, "!=", parts: 2)

        %{
          type: :inequality,
          variable: extract_var_name(String.trim(var)),
          value: extract_value(String.trim(value))
        }

      # Handle equality filter
      String.contains?(expr, "=") ->
        [var, value] = String.split(expr, "=", parts: 2)

        %{
          type: :equality,
          variable: extract_var_name(String.trim(var)),
          value: extract_value(String.trim(value))
        }

      # Handle regex
      String.match?(expr, ~r/^REGEX\s*\(/i) ->
        regex_parts =
          Regex.named_captures(
            ~r/REGEX\s*\(\s*\?(?<var>[^\s,]+)\s*,\s*"(?<pattern>[^"]+)"/i,
            expr
          )

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
      true ->
        %{type: :unknown, expression: expr}
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
    # First, extract aggregate alias variables like ?relationCount from (COUNT(?x) AS ?y)
    aggregate_aliases =
      Regex.scan(~r/\([^)]+\s+AS\s+\?([^\s\)]+)\)/i, vars_str, capture: :all_but_first)
      |> List.flatten()

    # Extract standalone variables (those starting with ?)
    standalone_vars =
      Regex.scan(~r/\s\?([^\s\(\)]+)/i, " " <> vars_str, capture: :all_but_first)
      |> List.flatten()

    # Combine all variables (removing duplicates)
    (aggregate_aliases ++ standalone_vars) |> Enum.uniq()
  end

  # Parse aggregation expressions
  def parse_aggregates(vars_str) do
    # Extract expressions like (COUNT(?x) AS ?y)
    pattern = ~r/\((\w+)\(\?([^\)]+)\)(?:\s+AS\s+\?([^\s\)]+))?\)/i

    Regex.scan(pattern, vars_str)
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
          if String.starts_with?(var, "?"), do: String.slice(var, 1..-1//1), else: var
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
              %{variable: String.slice(item, 1..-1//1), direction: :asc}

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

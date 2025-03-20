defmodule Kylix.Query.SparqlParser do
  @moduledoc """
  SPARQL Parser for Kylix blockchain queries.

  Parses SPARQL query strings into structured query objects that can be executed
  against the Kylix blockchain storage.
  """

  @doc """
  Parses a SPARQL query string into a structured query representation.

  ## Parameters

  - query: A SPARQL query string

  ## Returns

  - {:ok, query_structure} if parsing is successful
  - {:error, reason} if parsing fails

  ## Examples

      iex> SparqlParser.parse("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
      {:ok, %{type: :select, variables: ["s", "p", "o"], patterns: [%{s: nil, p: nil, o: nil}], filters: []}}
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
        # Parse variables
        variables = parse_variables(vars)

        # Parse WHERE clause patterns, filters, etc.
        {patterns, filters, optionals, unions} = parse_where_clause(where)

        # Check for aggregates
        has_aggregates = Regex.match?(~r/(COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT)\s*\(/i, vars)

        # Parse GROUP BY if present
        group_by = parse_group_by(rest)

        # Parse ORDER BY if present
        order_by = parse_order_by(rest)

        # Parse LIMIT and OFFSET if present
        {limit, offset} = parse_limit_offset(rest)

        # Construct the query structure
        query_structure = %{
          type: :select,
          variables: variables,
          patterns: patterns,
          filters: filters,
          optionals: optionals,
          unions: unions,
          has_aggregates: has_aggregates,
          group_by: group_by,
          order_by: order_by,
          limit: limit,
          offset: offset
        }

        # If we have aggregates, process them
        query_structure = if has_aggregates do
          # Process aggregates using SparqlAggregator
          alias Kylix.Query.SparqlAggregator
          aggregates = extract_aggregates(vars)
          Map.put(query_structure, :aggregates, aggregates)
        else
          query_structure
        end

        {:ok, query_structure}

      nil ->
        {:error, "Invalid SELECT query format"}
    end
  end

  @doc """
  Extracts aggregate functions from SELECT variables.
  """
  def extract_aggregates(vars) do
    alias Kylix.Query.SparqlAggregator

    # Find all aggregate function expressions
    agg_pattern = ~r/(COUNT|SUM|AVG|MIN|MAX|GROUP_CONCAT)\s*\([^\)]+\)/i

    Regex.scan(agg_pattern, vars)
    |> List.flatten()
    |> Enum.map(fn expr ->
      SparqlAggregator.parse_aggregate_expression(expr)
    end)
  end

  @doc """
  Parses GROUP BY clause from the query.
  """
  def parse_group_by(query_part) do
    group_by_pattern = ~r/GROUP\s+BY\s+(?<vars>[^;)]+?)(\s+ORDER\s+BY|\s+LIMIT|\s+OFFSET|$)/i

    case Regex.named_captures(group_by_pattern, query_part) do
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

  @doc """
  Parses ORDER BY clause from the query.
  """
  def parse_order_by(query_part) do
    order_by_pattern = ~r/ORDER\s+BY\s+(?<ordering>[^;)]+?)(\s+LIMIT|\s+OFFSET|$)/i

    case Regex.named_captures(order_by_pattern, query_part) do
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

  @doc """
  Parses LIMIT and OFFSET clauses from the query.
  """
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

  @doc """
  Parses the variables section of a SELECT query.
  """
  def parse_variables(vars_str) do
    vars_str
    |> String.split(" ")
    |> Enum.map(fn var ->
      var = String.trim(var)
      if String.starts_with?(var, "?"), do: String.slice(var, 1..-1//1), else: var
    end)
    |> Enum.filter(&(String.length(&1) > 0))
  end

  @doc """
  Parses the WHERE clause of a SPARQL query.
  Returns a tuple with {patterns, filters, optionals, unions}.
  """
  def parse_where_clause(where_str) do
    # Clean up the where clause
    clean_where = String.trim(where_str)

    # Parse basic triple patterns first
    triple_patterns = parse_triple_patterns(clean_where)

    # Extract FILTER statements
    filters = parse_filters(clean_where)

    # Extract OPTIONAL patterns
    optionals = parse_optionals(clean_where)

    # Extract UNION patterns
    unions = parse_unions(clean_where)

    {triple_patterns, filters, optionals, unions}
  end

  @doc """
  Parses triple patterns from the WHERE clause.
  """
  def parse_triple_patterns(where_str) do
    # This is a simplification; a real parser would be more robust
    # Basic pattern regex for triples: subject predicate object .
    pattern_regex = ~r/(?<s>[^\s]+)\s+(?<p>[^\s]+)\s+(?<o>[^\s]+)\s*\.?/is

    Regex.scan(pattern_regex, where_str, capture: :all_names)
    |> Enum.map(fn [s, p, o] ->
      # Process each component
      subject = parse_triple_component(s)
      predicate = parse_triple_component(p)
      object = parse_triple_component(o)

      %{s: subject, p: predicate, o: object}
    end)
  end

  @doc """
  Parses a single component of a triple pattern (subject, predicate, or object).
  """
  def parse_triple_component(component) do
    component = String.trim(component)
    cond do
      # Variable
      String.starts_with?(component, "?") ->
        nil

      # Literal enclosed in quotes
      String.starts_with?(component, "\"") and String.ends_with?(component, "\"") ->
        String.slice(component, 1..-2//1)

      # URI enclosed in angle brackets
      String.starts_with?(component, "<") and String.ends_with?(component, ">") ->
        String.slice(component, 1..-2//1)

      # Prefixed name
      String.contains?(component, ":") ->
        component

      # Anything else treated as-is
      true ->
        component
    end
  end

  @doc """
  Parses FILTER expressions from the WHERE clause.
  """
  def parse_filters(where_str) do
    filter_regex = ~r/FILTER\s*\((?<filter_expr>.+?)\)/is

    Regex.scan(filter_regex, where_str, capture: :all_names)
    |> Enum.map(fn [expr] ->
      parse_filter_expression(String.trim(expr))
    end)
  end

  @doc """
  Parses a single FILTER expression.
  """
  def parse_filter_expression(expr) do
    # This is a simplification; real parsing would be more complex
    # Examples: ?s = "value", REGEX(?name, "pattern"), etc.
    cond do
      # Equality filter
      String.contains?(expr, "=") ->
        [var, value] = String.split(expr, "=", parts: 2)
        var = String.trim(var)
        value = String.trim(value)

        %{
          type: :equality,
          variable: if(String.starts_with?(var, "?"), do: String.slice(var, 1..-1//1), else: var),
          value: if(String.starts_with?(value, "\"") and String.ends_with?(value, "\""),
                    do: String.slice(value, 1..-2//1),
                    else: value)
        }

      # REGEX filter
      String.starts_with?(expr, "REGEX") ->
        # Extract the variable name and pattern from REGEX(?var, "pattern")
        regex_parts = Regex.named_captures(~r/REGEX\s*\(\s*\?(?<var>[^\s,]+)\s*,\s*"(?<pattern>.+?)"\s*\)/is, expr)

        if regex_parts do
          %{
            type: :regex,
            variable: regex_parts["var"],
            pattern: regex_parts["pattern"]
          }
        else
          %{type: :unknown, expression: expr}
        end

      # Other filters (simplification)
      true ->
        %{type: :unknown, expression: expr}
    end
  end

  @doc """
  Parses OPTIONAL patterns from the WHERE clause.
  """
  def parse_optionals(where_str) do
    optional_regex = ~r/OPTIONAL\s*\{\s*(?<optional_pattern>.+?)\s*\}/is

    Regex.scan(optional_regex, where_str, capture: :all_names)
    |> Enum.map(fn [pattern] ->
      # Recursively parse the pattern inside OPTIONAL
      {patterns, filters, _, _} = parse_where_clause(pattern)
      %{patterns: patterns, filters: filters}
    end)
  end

  @doc """
  Parses UNION patterns from the WHERE clause.
  """
  def parse_unions(where_str) do
    # This is simplified; UNION parsing is complex in real SPARQL
    union_regex = ~r/\{\s*(?<left_pattern>.+?)\s*\}\s+UNION\s+\{\s*(?<right_pattern>.+?)\s*\}/is

    Regex.scan(union_regex, where_str, capture: :all_names)
    |> Enum.map(fn [left, right] ->
      # Parse both sides of the UNION
      {left_patterns, left_filters, _, _} = parse_where_clause(left)
      {right_patterns, right_filters, _, _} = parse_where_clause(right)

      %{
        left: %{patterns: left_patterns, filters: left_filters},
        right: %{patterns: right_patterns, filters: right_filters}
      }
    end)
  end
end

defmodule Kylix.Query.SparqlParser do
  @moduledoc """
  SPARQL Parser for Kylix blockchain queries using NimbleParsec.
  """

  import NimbleParsec
  require Logger

  def add_prov_prefix(id), do: "prov:" <> id
  def apply_default_order_direction({:variable, var}), do: %{variable: var, direction: :asc}

  whitespace = ascii_string([?\s, ?\n, ?\r, ?\t], min: 1)
  optional_whitespace = ascii_string([?\s, ?\n, ?\r, ?\t], min: 0)

  variable =
    ignore(string("?"))
    |> ascii_string([?a..?z, ?A..?Z, ?0..?9, ?_], min: 1)
    |> unwrap_and_tag(:variable)

  double_quoted_content =
    repeat(
      choice([
        ~S(\") |> string() |> replace(?"),
        ~S(\\) |> string() |> replace(?\\),
        ~S(\n) |> string() |> replace(?\n),
        ~S(\r) |> string() |> replace(?\r),
        ~S(\t) |> string() |> replace(?\t),
        ascii_string([not: ?", not: ?\\], min: 1)
      ])
    )
    |> reduce({List, :to_string, []})

  single_quoted_content =
    repeat(
      choice([
        ~S(\') |> string() |> replace(?'),
        ~S(\\) |> string() |> replace(?\\),
        ~S(\n) |> string() |> replace(?\n),
        ~S(\r) |> string() |> replace(?\r),
        ~S(\t) |> string() |> replace(?\t),
        ascii_string([not: ?', not: ?\\], min: 1)
      ])
    )
    |> reduce({List, :to_string, []})

  double_quoted_literal =
    ignore(string("\""))
    |> concat(double_quoted_content)
    |> ignore(string("\""))
    |> unwrap_and_tag(:literal)

  single_quoted_literal =
    ignore(string("'"))
    |> concat(single_quoted_content)
    |> ignore(string("'"))
    |> unwrap_and_tag(:literal)

  simple_literal = choice([double_quoted_literal, single_quoted_literal])

  iri =
    ignore(string("<"))
    |> repeat(ascii_string([not: ?>], min: 1))
    |> reduce({List, :to_string, []})
    |> ignore(string(">"))
    |> unwrap_and_tag(:iri)

  prov_prefix =
    ignore(string("prov:"))
    |> ascii_string([?a..?z, ?A..?Z, ?0..?9, ?_], min: 1)
    |> map({__MODULE__, :add_prov_prefix, []})
    |> unwrap_and_tag(:prov)

  language_tag =
    ignore(string("@"))
    |> ascii_string([?a..?z, ?A..?Z, ?0..?9, ?-], min: 1)
    |> tag(:lang)

  datatype =
    ignore(string("^^"))
    |> choice([prov_prefix, iri])
    |> tag(:datatype)

  literal_with_meta =
    simple_literal
    |> optional(choice([language_tag, datatype]))
    |> tag(:literal_with_meta)

  node = choice([variable, literal_with_meta, iri, prov_prefix])

  triple_pattern =
    node
    |> ignore(whitespace)
    |> concat(node)
    |> ignore(whitespace)
    |> concat(node)
    |> ignore(optional_whitespace)
    |> optional(ignore(string(".")))
    |> ignore(optional_whitespace)
    |> tag(:triple)

  comparison_operator =
    choice([
      string("="),
      string("!="),
      string(">"),
      string("<"),
      string(">="),
      string("<=")
    ])
    |> unwrap_and_tag(:operator)

  filter_operand = choice([variable, literal_with_meta])

  defcombinatorp(:filter_operand, filter_operand)

  filter_expression =
    parsec(:filter_operand)
    |> ignore(optional_whitespace)
    |> concat(comparison_operator)
    |> ignore(optional_whitespace)
    |> concat(parsec(:filter_operand))
    |> tag(:filter_expression)

  defcombinatorp(:filter_expression, filter_expression)

  filter_condition =
    ignore(string("FILTER"))
    |> ignore(optional_whitespace)
    |> ignore(string("("))
    |> ignore(optional_whitespace)
    |> parsec(:filter_expression)
    |> ignore(optional_whitespace)
    |> ignore(string(")"))
    |> tag(:filter)

  defcombinatorp(:patterns_list, parsec(:inner_patterns_list))

  optional_pattern =
    ignore(string("OPTIONAL"))
    |> ignore(whitespace)
    |> ignore(string("{"))
    |> ignore(optional_whitespace)
    |> parsec(:patterns_list)
    |> ignore(optional_whitespace)
    |> ignore(string("}"))
    |> tag(:optional)

  defcombinatorp(:optional_pattern, optional_pattern)

  union_pattern =
    ignore(string("{"))
    |> ignore(optional_whitespace)
    |> parsec(:patterns_list)
    |> ignore(optional_whitespace)
    |> ignore(string("}"))
    |> ignore(whitespace)
    |> ignore(string("UNION"))
    |> ignore(whitespace)
    |> ignore(string("{"))
    |> ignore(optional_whitespace)
    |> parsec(:patterns_list)
    |> ignore(optional_whitespace)
    |> ignore(string("}"))
    |> tag(:union)

  defcombinatorp(:union_pattern, union_pattern)

  # Updated inner_patterns_list
  inner_patterns_list =
    repeat(
      choice([
        triple_pattern,
        filter_condition,
        parsec(:optional_pattern),
        parsec(:union_pattern)
      ])
      |> ignore(optional_whitespace)
    )
    |> lookahead_not(string("GROUP BY") |> string("ORDER BY") |> string("LIMIT") |> string("OFFSET"))
    |> tag(:patterns)

  defcombinatorp(:inner_patterns_list, inner_patterns_list)

  where_clause =
    ignore(string("WHERE"))
    |> ignore(whitespace)
    |> ignore(string("{"))
    |> ignore(optional_whitespace)
    |> parsec(:patterns_list)
    |> ignore(optional_whitespace)
    |> ignore(string("}"))
    |> tag(:where)

  group_by_var =
    ignore(optional_whitespace)
    |> concat(variable)
    |> ignore(optional_whitespace)
    |> optional(ignore(string(",")))

  group_by_clause =
    ignore(string("GROUP BY"))
    |> ignore(whitespace)
    |> repeat(group_by_var)
    |> tag(:group_by)

  order_dir_asc = string("ASC") |> replace(:asc) |> unwrap_and_tag(:direction)
  order_dir_desc = string("DESC") |> replace(:desc) |> unwrap_and_tag(:direction)

  order_expr_with_dir =
    choice([order_dir_asc, order_dir_desc])
    |> ignore(string("("))
    |> ignore(optional_whitespace)
    |> concat(variable)
    |> ignore(optional_whitespace)
    |> ignore(string(")"))
    |> tag(:order_with_dir)

  order_expr_no_dir =
    variable
    |> map({__MODULE__, :apply_default_order_direction, []})
    |> tag(:order_no_dir)

  order_by_var =
    ignore(optional_whitespace)
    |> choice([order_expr_with_dir, order_expr_no_dir])
    |> ignore(optional_whitespace)
    |> optional(ignore(string(",")))

  order_by_clause =
    ignore(string("ORDER BY"))
    |> ignore(whitespace)
    |> repeat(order_by_var)
    |> tag(:order_by)

  integer = integer(min: 1) |> unwrap_and_tag(:value)

  limit_clause =
    ignore(string("LIMIT"))
    |> ignore(whitespace)
    |> concat(integer)
    |> tag(:limit)

  offset_clause =
    ignore(string("OFFSET"))
    |> ignore(whitespace)
    |> concat(integer)
    |> tag(:offset)

  aggregate_function =
    choice([
      string("COUNT"),
      string("SUM"),
      string("AVG"),
      string("MIN"),
      string("MAX"),
      string("GROUP_CONCAT")
    ])
    |> unwrap_and_tag(:function)

  aggregate_expression =
    ignore(string("("))
    |> concat(aggregate_function)
    |> ignore(string("("))
    |> optional(
      string("DISTINCT")
      |> replace(true)
      |> unwrap_and_tag(:distinct)
      |> ignore(whitespace)
    )
    |> concat(variable)
    |> ignore(string(")"))
    |> optional(
      ignore(whitespace)
      |> ignore(string("AS"))
      |> ignore(whitespace)
      |> concat(variable)
      |> unwrap_and_tag(:alias)
    )
    |> ignore(string(")"))
    |> tag(:aggregate)

  select_item =
    choice([aggregate_expression, variable])
    |> ignore(optional_whitespace)
    |> optional(ignore(string(",")))

  select_star =
    string("*")
    |> replace(:all)
    |> unwrap_and_tag(:select_type)

  select_distinct =
    string("DISTINCT")
    |> replace(true)
    |> unwrap_and_tag(:distinct)
    |> ignore(whitespace)

  select_clause =
    ignore(string("SELECT"))
    |> ignore(whitespace)
    |> optional(select_distinct)
    |> choice([
      select_star |> tag(:select),
      repeat(select_item) |> tag(:select)
    ])

  sparql_query =
    select_clause
    |> ignore(optional_whitespace)
    |> concat(where_clause)
    |> ignore(optional_whitespace)
    |> optional(concat(group_by_clause, ignore(optional_whitespace)))
    |> optional(concat(order_by_clause, ignore(optional_whitespace)))
    |> optional(concat(limit_clause, ignore(optional_whitespace)))
    |> optional(concat(offset_clause, ignore(optional_whitespace)))
    |> eos()

  defparsec(:parse_query, sparql_query)

  @doc """
  Parses a SPARQL query string into a structured query representation.
  """
  def parse(query) do
    IO.inspect(query, label: "Raw query input to parser")
    try do
      normalized_query = normalize_query(query)
      Logger.debug("Parsing query: #{normalized_query}")
      case parse_query(normalized_query) do
        {:ok, parsed, "", _, _, _} ->
          IO.inspect(parsed, label: "Parsed before conversion")
          IO.inspect(parsed, label: "Parsed")
          query_structure = convert_to_query_structure(parsed)
          {:ok, query_structure}

        {:ok, _, rest, _, _, _} ->
          Logger.error("Incomplete parse, remaining: #{rest}")
          {:error, "Failed to parse entire query. Stopped at: #{rest}"}

        {:error, reason, rest, _, _, _} ->
          Logger.error("Parse error details - reason: #{reason}, at: #{rest}")
          {:error, "Parse error: #{reason}, at: #{rest}"}
      end
    rescue
      e ->
        Logger.error("Exception in parse: #{Exception.message(e)}")
        {:error, "SPARQL parse error: #{Exception.message(e)}"}
    end
  end

  defp normalize_query(query) do
    query
    |> String.trim()
    |> String.replace(~r/\s+/, " ")
  end

  defp convert_to_query_structure(parsed) do
    {variables, aggregates, has_aggregates} =
      process_select_items(Keyword.get(parsed, :select, []))

    where_info = Keyword.get(parsed, :where, [{:patterns, []}])
    patterns_list = Keyword.get(where_info, :patterns, [])
    {patterns, filters, optionals, unions} = process_where_clause(patterns_list)

    %{
      type: :select,
      variables: Enum.reverse(variables),
      patterns: patterns,
      filters: filters,
      optionals: optionals,
      unions: unions,
      has_aggregates: has_aggregates,
      aggregates: aggregates,
      group_by: extract_group_by(parsed),
      order_by: extract_order_by(parsed),
      limit: extract_limit(parsed),
      offset: extract_offset(parsed),
      pattern_filters: []
    }
  end

  defp process_select_items(select_items) do
    Enum.reduce(select_items, {[], [], false}, fn
      {:variable, var}, {vars, aggs, has_aggs} ->
        {[var | vars], aggs, has_aggs}

      {:aggregate, agg_info}, {vars, aggs, _} ->
        function = Keyword.get(agg_info, :function)
        var = Keyword.get(agg_info, :variable)
        distinct = Keyword.get(agg_info, :distinct, false)

        alias_var =
          case Keyword.get(agg_info, :alias) do
            {:variable, alias_name} -> alias_name
            nil -> "#{String.downcase(function)}_#{var}"
          end

        agg = %{
          function: String.downcase(function) |> String.to_atom(),
          variable: var,
          distinct: distinct,
          alias: alias_var
        }

        {[alias_var | vars], [agg | aggs], true}

      _, acc ->
        acc
    end)
  end

  defp process_where_clause(where_items) do
    Enum.reduce(where_items, {[], [], [], []}, fn
      {:triple, [subj, pred, obj]}, {patterns, filters, optionals, unions} ->
        pattern = %{s: process_node(subj), p: process_node(pred), o: process_node(obj)}
        {[pattern | patterns], filters, optionals, unions}

      {:filter, [expr]}, {patterns, filters, optionals, unions} ->
        filter = process_filter(expr)
        {patterns, [filter | filters], optionals, unions}

      {:optional, opt_info}, {patterns, filters, optionals, unions} ->
        opt_patterns = opt_info[:patterns] || []
        {opt_patterns_list, opt_filters, nested_optionals, _} = process_where_clause(opt_patterns)

        optional = %{
          patterns: opt_patterns_list,
          filters: opt_filters,
          optionals: nested_optionals
        }

        {patterns, filters, [optional | optionals], unions}

      {:union, [{:patterns, left_patterns}, {:patterns, right_patterns}]},
      {patterns, filters, optionals, unions} ->
        {left_pats, left_fils, left_opts, _} = process_where_clause(left_patterns)
        {right_pats, right_fils, right_opts, _} = process_where_clause(right_patterns)

        union = %{
          left: %{patterns: left_pats, filters: left_fils, optionals: left_opts},
          right: %{patterns: right_pats, filters: right_fils, optionals: right_opts}
        }

        {patterns, filters, optionals, [union | unions]}

      _, acc ->
        acc
    end)
    |> then(fn {patterns, filters, optionals, unions} ->
      {Enum.reverse(patterns), Enum.reverse(filters), Enum.reverse(optionals),
       Enum.reverse(unions)}
    end)
  end

  defp process_node({:variable, _var}), do: nil

  defp process_node({:literal_with_meta, [{:literal, value} | rest]}) do
    case rest do
      [{:lang, lang}] -> "#{value}@#{lang}"
      [{:datatype, dt}] -> "#{value}^^#{process_datatype(dt)}"
      [] -> value
      _ -> nil
    end
  end

  defp process_node({:iri, iri}), do: iri
  defp process_node({:prov, prov}), do: prov
  defp process_node(_), do: nil

  defp process_datatype({:iri, iri}), do: iri
  defp process_datatype({:prov, prov}), do: prov
  defp process_datatype(_), do: nil

  defp process_filter({:filter_expression, [left, {:operator, op}, right]}) do
    filter_type =
      case op do
        "=" -> :equality
        "!=" -> :inequality
        ">" -> :greater_than
        "<" -> :less_than
        ">=" -> :greater_than_equal
        "<=" -> :less_than_equal
        _ -> :unknown
      end

    %{
      type: filter_type,
      variable: extract_variable(left),
      value: extract_value(right),
      expression: "#{format_for_expression(left)} #{op} #{format_for_expression(right)}"
    }
  end

  defp format_for_expression({:variable, var}), do: "?#{var}"
  defp format_for_expression({:literal_with_meta, [{:literal, val} | _]}), do: "\"#{val}\""
  defp format_for_expression(other), do: inspect(other)

  defp extract_variable({:variable, var}), do: var
  defp extract_variable(_), do: nil

  defp extract_value({:literal_with_meta, [{:literal, val} | _]}), do: val
  defp extract_value({:variable, var}), do: var
  defp extract_value(_), do: nil

  defp extract_group_by(parsed) do
    case Keyword.get(parsed, :group_by) do
      nil -> []
      group_by -> Enum.map(group_by, fn {:variable, var} -> var end)
    end
  end

  defp extract_order_by(parsed) do
    case Keyword.get(parsed, :order_by) do
      nil ->
        []

      order_by ->
        Enum.map(order_by, fn
          {:order_with_dir, [{:direction, dir}, {:variable, var}]} ->
            %{variable: var, direction: dir}

          {:order_no_dir, [map]} ->
            map

          _ ->
            nil
        end)
        |> Enum.filter(&(&1 != nil))
    end
  end

  defp extract_limit(parsed) do
    case Keyword.get(parsed, :limit) do
      [{:value, value}] -> value
      _ -> nil
    end
  end

  defp extract_offset(parsed) do
    case Keyword.get(parsed, :offset) do
      [{:value, value}] -> value
      _ -> nil
    end
  end
end

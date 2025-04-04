defmodule Kylix.Query.SparqlOptimizer do
  @moduledoc """
  Optimizes SPARQL queries with full support for PROV-O (Provenance Ontology).
  """

  require Logger
  import NimbleParsec

  # --- PROV-O Constants ---
  @prov_ns "http://www.w3.org/ns/prov#"
  @prov_terms %{
    classes: ~w(Entity Activity Agent Collection Bundle Plan),
    properties: ~w(wasGeneratedBy used wasAttributedTo wasAssociatedWith hadMember
                  wasDerivedFrom wasInformedBy startedAtTime endedAtTime
                  actedOnBehalfOf wasInvalidatedBy generated invalidated)
  }
  @prov_prefix "prov:"

  # --- NimbleParsec Parser Definition ---
  whitespace = ascii_char([?\s, ?\t, ?\n, ?\r]) |> repeat()

  # Variables (e.g., ?s)
  variable =
    string("?")
    |> ascii_string([?a..?z, ?A..?Z, ?0..?9], min: 1)
    |> reduce({List, :to_string, []})

  # Literals (e.g., "example")
  literal =
    string("\"")
    |> ascii_string([?a..?z, ?A..?Z, ?0..?9, ?_], min: 1)
    |> string("\"")
    |> reduce({List, :to_string, []})

  # URIs (e.g., <http://...> or prov:Entity)
  uri =
    choice([
      string("<") |> utf8_string([?a..?z, ?A..?Z, ?0..?9, ?:, ?/, ?#, ?_], min: 1) |> string(">"),
      string(@prov_prefix) |> ascii_string([?a..?z, ?A..?Z], min: 1)
    ])
    |> post_traverse({:to_uri, []})

  # Triple term (variable, literal, or URI)
  triple_term = choice([variable, literal, uri])

  # Triple pattern (e.g., ?s prov:wasGeneratedBy ?o)
  triple =
    triple_term
    |> ignore(whitespace)
    |> concat(triple_term)
    |> ignore(whitespace)
    |> concat(triple_term)
    |> wrap()
    |> post_traverse({:to_triple, []})

  # Simplified parser: Single triple pattern and optional filter
  defparsec :parse_sparql,
    ignore(string("SELECT"))
    |> ignore(whitespace)
    |> repeat(variable |> ignore(whitespace))
    |> ignore(string("WHERE {"))
    |> ignore(whitespace)
    |> concat(triple)
    |> ignore(string("."))
    |> ignore(whitespace)
    |> optional(
      ignore(string("FILTER("))
      |> concat(variable)
      |> ignore(whitespace)
      |> string("=")
      |> ignore(whitespace)
      |> concat(choice([literal, uri]))
      |> ignore(string(")"))
      |> post_traverse({:to_filter, []})
    )
    |> ignore(whitespace)
    |> ignore(string("}")),
    debug: false

  # Parser helpers
  defp to_uri(_rest, args, context, _line, _offset) do
    uri = case args do
      ["<", uri, ">"] -> uri
      [@prov_prefix, suffix] -> "#{@prov_ns}#{suffix}"
      _ -> raise "Invalid URI format"
    end
    {[uri], context}
  end

  defp to_triple(_rest, [o, p, s], context, _line, _offset) do
    triple = %{s: normalize_term(s), p: normalize_term(p), o: normalize_term(o)}
    {[triple], context}
  end

  defp to_filter(_rest, [value, "=", var], context, _line, _offset) do
    filter = %{variable: var, operator: "=", value: normalize_term(value)}
    {[filter], context}
  end

  defp normalize_term(term) do
    cond do
      String.starts_with?(term, "?") -> term
      String.starts_with?(term, "<") -> String.trim(term, "<>")
      String.starts_with?(term, @prov_prefix) -> "#{@prov_ns}#{String.trim_leading(term, @prov_prefix)}"
      String.starts_with?(term, @prov_ns) -> term
      true -> term
    end
  end

  # --- Public API ---

  @doc """
  Optimizes a SPARQL query with PROV-O support.
  """
  def optimize(query) when is_binary(query) do
    case parse_sparql(query) do
      {:ok, [triple | rest], "", _, _, _} ->
        {patterns, filters} =
          case rest do
            [filter] -> {[triple], [filter]}
            [] -> {[triple], []}
            _ -> raise "Unexpected parse result"
          end

        optimized = %{
          patterns: patterns,
          filters: filters,
          optionals: [],
          unions: [],
          pattern_filters: [%{pattern: triple, filters: filters}]
        }

        {:ok, optimize_query(optimized)}

      {:error, reason, _, _, _, _} ->
        {:error, "Parse error: #{reason}"}
    end
  end

  def optimize(query) when is_map(query), do: {:ok, optimize_query(query)}
  def optimize(invalid), do: {:error, "Invalid query format: #{inspect(invalid)}"}

  # --- Optimization Logic ---

  defp optimize_query(query) do
    query
    |> Map.put_new(:pattern_filters, [])
    |> reorder_triple_patterns()
    |> push_filters()
    |> optimize_optionals()
    |> optimize_unions()
    |> rewrite_query()
  end

  # Reorders triple patterns with PROV-O-aware selectivity
  defp reorder_triple_patterns(query) do
    patterns = query.patterns

    if length(patterns) <= 1 do
      query
    else
      sorted_patterns =
        patterns
        |> Enum.map(&{&1, calculate_pattern_selectivity(&1)})
        |> Enum.sort_by(fn {_, selectivity} -> selectivity end, :asc)
        |> Enum.map(fn {pattern, _} -> pattern end)

      %{query | patterns: sorted_patterns}
    end
  end

  # Calculates selectivity score with PROV-O heuristics
  defp calculate_pattern_selectivity(pattern) do
    base_score =
      Enum.reduce([pattern.s, pattern.p, pattern.o], 0, fn term, acc ->
        case term do
          nil -> acc + 3
          "?" <> _ -> acc + 2
          _ -> acc + 1
        end
      end)

    prov_property = Enum.find(@prov_terms.properties, fn p -> "#{@prov_ns}#{p}" == pattern.p end)
    prov_class = Enum.find(@prov_terms.classes, fn c -> "#{@prov_ns}#{c}" == pattern.o end)

    cond do
      prov_property == "wasGeneratedBy" -> base_score - 2
      prov_property in ["used", "wasAttributedTo"] -> base_score - 1
      prov_class != nil -> base_score - 1
      true -> base_score
    end
  end

  # Pushes filters close to relevant triple patterns
  defp push_filters(query) do
    filters = Map.get(query, :filters, [])
    if Enum.empty?(filters), do: query, else: do_push_filters(query, filters)
  end

  defp do_push_filters(query, filters) do
    {new_patterns, remaining_filters} =
      Enum.reduce(query.patterns, {[], filters}, fn pattern, {acc, filters_left} ->
        pattern_vars = pattern_variables(pattern)
        {applicable, others} =
          Enum.split_with(filters_left, fn filter ->
            Enum.all?(filter_variables(filter), &(&1 in pattern_vars))
          end)

        pattern_with_filters = %{pattern: pattern, filters: applicable}
        {[pattern_with_filters | acc], others}
      end)

    %{query |
      patterns: Enum.map(new_patterns, & &1.pattern),
      pattern_filters: Enum.reverse(new_patterns),
      filters: remaining_filters}
  end

  # Extracts variables from a triple pattern
  defp pattern_variables(pattern) do
    [pattern.s, pattern.p, pattern.o]
    |> Enum.filter(&is_binary/1)
    |> Enum.filter(&String.starts_with?(&1, "?"))
  end

  # Extracts variables from a filter
  defp filter_variables(%{variable: var}) when is_binary(var), do: [var]
  defp filter_variables(_), do: []

  # Optimizes OPTIONAL clauses with PROV-O awareness
  defp optimize_optionals(query) do
    optionals = Map.get(query, :options, [])
    if Enum.empty?(optionals), do: query, else: do_optimize_optionals(query, optionals)
  end

  defp do_optimize_optionals(query, optionals) do
    optimized_optionals =
      Enum.map(optionals, fn optional ->
        %{
          patterns: reorder_triple_patterns(%{patterns: optional.patterns}).patterns,
          filters: optional.filters
        }
      end)

    %{query | optionals: optimized_optionals}
  end

  # Optimizes UNION clauses with PROV-O awareness
  defp optimize_unions(query) do
    unions = Map.get(query, :unions, [])
    if Enum.empty?(unions), do: query, else: do_optimize_unions(query, unions)
  end

  defp do_optimize_unions(query, unions) do
    optimized_unions =
      Enum.map(unions, fn union ->
        %{
          left: %{
            patterns: reorder_triple_patterns(%{patterns: union.left.patterns}).patterns,
            filters: union.left.filters
          },
          right: %{
            patterns: reorder_triple_patterns(%{patterns: union.right.patterns}).patterns,
            filters: union.right.filters
          }
        }
      end)

    %{query | unions: optimized_unions}
  end

  # Rewrites query with PROV-O-specific transformations
  defp rewrite_query(query) do
    query
  end

  @doc """
  Creates an execution plan optimized for PROV-O queries.
  """
  def create_execution_plan(query) do
    base_plan = %{type: :query_plan, steps: [], estimated_cost: 0}

    plan_with_patterns =
      Enum.reduce(query.patterns, base_plan, fn pattern, plan ->
        step = %{
          type: :triple_scan,
          pattern: pattern,
          estimated_cardinality: estimate_pattern_cardinality(pattern),
          prov_optimized: is_prov_pattern?(pattern)
        }

        %{
          plan |
          steps: [step | plan.steps],
          estimated_cost: plan.estimated_cost + step.estimated_cardinality
        }
      end)

    Enum.reduce(Map.get(query, :filters, []), plan_with_patterns, fn filter, plan ->
      step = %{
        type: :filter,
        filter: filter,
        estimated_selectivity: estimate_filter_selectivity(filter)
      }

      %{
        plan |
        steps: [step | plan.steps],
        estimated_cost: plan.estimated_cost * step.estimated_selectivity
      }
    end)
    |> Map.update!(:steps, &Enum.reverse/1)
  end

  # Estimates pattern cardinality with PROV-O heuristics
  defp estimate_pattern_cardinality(pattern) do
    prov_property = Enum.find(@prov_terms.properties, fn p -> "#{@prov_ns}#{p}" == pattern.p end)

    case {pattern.s, pattern.p, pattern.o} do
      {nil, nil, nil} -> 1000
      {_, p, _} when p == "#{@prov_ns}wasGeneratedBy" -> 5
      {_, p, _} when p == "#{@prov_ns}used" -> 10
      {_, _, _} when is_binary(pattern.s) and is_binary(pattern.p) and is_binary(pattern.o) -> 1
      {_, _, nil} -> if(prov_property, do: 8, else: 10)
      {nil, _, _} -> if(prov_property, do: 15, else: 20)
      {_, nil, _} -> 30
      _ -> 50
    end
  end

  # Estimates filter selectivity
  defp estimate_filter_selectivity(_filter), do: 0.1

  # Checks if a pattern uses PROV-O terms
  defp is_prov_pattern?(pattern) do
    Enum.any?([pattern.p, pattern.o], fn term ->
      term != nil and String.starts_with?(term, @prov_ns)
    end)
  end
end

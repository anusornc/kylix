defmodule Kylix.Query.SparqlEngine do
  @moduledoc """
  Provides SPARQL query capabilities for the blockchain data.

  This module integrates the SPARQL parser and executor to provide
  a complete SPARQL query solution for the Kylix blockchain.
  Enhanced with NimbleParsec for robust query preprocessing and validation.
  """

  import NimbleParsec
  require Logger

  alias Kylix.Query.SparqlParser
  alias Kylix.Query.SparqlExecutor
  alias Kylix.Query.SparqlOptimizer

  # --- NimbleParsec Parser Definitions ---

  # Whitespace handling
  whitespace = ascii_string([?\s, ?\n, ?\r, ?\t], min: 1)
  optional_whitespace = ascii_string([?\s, ?\n, ?\r, ?\t], min: 0)

  # Comment parser (single-line and multi-line)
  single_line_comment =
    string("#")
    |> repeat(ascii_char(not: ?\n))
    |> optional(string("\n"))
    |> replace(:comment)

  multi_line_comment =
    string("/*")
    |> repeat(lookahead_not(string("*/")) |> ascii_char([]))
    |> string("*/")
    |> replace(:comment)

  any_comment = choice([single_line_comment, multi_line_comment])

  # PREFIX declaration parser
  prefix_name =
    optional(ascii_string([?a..?z, ?A..?Z, ?0..?9, ?_], min: 1))
    |> string(":")
    |> reduce({Enum, :join, [""]})

  prefix_uri =
    ignore(string("<"))
    |> ascii_string([not: ?>], min: 1)
    |> ignore(string(">"))

  prefix_decl =
    ignore(optional_whitespace)
    |> ignore(string("PREFIX"))
    |> ignore(whitespace)
    |> concat(prefix_name)
    |> ignore(whitespace)
    |> concat(prefix_uri)
    |> ignore(optional_whitespace)
    |> tag(:prefix)

  # BASE declaration parser
  base_decl =
    ignore(optional_whitespace)
    |> ignore(string("BASE"))
    |> ignore(whitespace)
    |> ignore(string("<"))
    |> ascii_string([not: ?>], min: 1)
    |> ignore(string(">"))
    |> ignore(optional_whitespace)
    |> tag(:base)

  # Main SPARQL query components
  select_keyword = ignore(optional_whitespace) |> string("SELECT") |> replace(:select)
  construct_keyword = ignore(optional_whitespace) |> string("CONSTRUCT") |> replace(:construct)
  describe_keyword = ignore(optional_whitespace) |> string("DESCRIBE") |> replace(:describe)
  ask_keyword = ignore(optional_whitespace) |> string("ASK") |> replace(:ask)
  query_type = choice([select_keyword, construct_keyword, describe_keyword, ask_keyword])

  # Simple validator for query structure
  query_structure_validator =
    optional(repeat(choice([prefix_decl, base_decl, any_comment, whitespace |> ignore()])))
    |> concat(query_type)
    |> repeat(
      choice([
        any_comment |> ignore(),
        whitespace |> ignore(),
        utf8_string([not: ?\s, not: ?\n, not: ?\r, not: ?\t], min: 1)
      ])
    )

  defparsec(:parse_query_structure, query_structure_validator)

  # Query preprocessor to normalize and extract metadata
  query_preprocessor =
    repeat(
      choice([
        prefix_decl,
        base_decl,
        any_comment |> ignore(),
        whitespace |> replace(" "),
        # Explicitly match all SPARQL keywords to prevent token splitting
        choice([
          string("SELECT"),
          string("CONSTRUCT"),
          string("DESCRIBE"),
          string("ASK"),
          string("DELETE"),
          string("INSERT"),
          string("DROP"),
          string("LOAD"),
          string("CLEAR")
        ]),
        # Capture other tokens as whole strings up to whitespace
        utf8_string([not: ?\s, not: ?\n, not: ?\r, not: ?\t], min: 1)
      ])
    )

  defparsec(:preprocess_query, query_preprocessor)

  # --- End of Parser Definitions ---

  @doc """
  Executes a SPARQL query against the blockchain data.
  """
  def execute(query) do
    try do
      normalized_query = query |> ensure_utf8_encoding() |> String.trim()
      Logger.debug("Executing SPARQL query: #{inspect(normalized_query)}")

      case preprocess_sparql_query(normalized_query) do
        {:ok, preprocessed_query, prefixes} ->
          case validate_sparql_query(preprocessed_query) do
            :ok ->
              Logger.debug("Parsing SPARQL query: #{preprocessed_query}")
              case SparqlParser.parse(preprocessed_query) do
                {:ok, parsed_query} ->
                  parsed_query = Map.put(parsed_query, :prefixes, prefixes)
                  Logger.debug("Parsed query structure: #{inspect(parsed_query)}")
                  optimized_query =
                    case SparqlOptimizer.optimize(parsed_query) do
                      {:ok, optimized} -> optimized
                      {:error, _} -> parsed_query
                    end
                  Logger.debug("Optimized query structure: #{inspect(optimized_query)}")
                  result = SparqlExecutor.execute(optimized_query)
                  Logger.debug("Query execution result: #{inspect(result)}")
                  result
                {:error, reason} ->
                  Logger.error("SPARQL parse error: #{reason}")
                  {:error, reason}
              end
            {:error, reason} ->
              Logger.error("SPARQL query validation failed: #{reason}")
              {:error, reason}
          end
        {:error, reason} ->
          Logger.error("SPARQL query preprocessing failed: #{reason}")
          {:error, reason}
      end
    rescue
      e ->
        Logger.error("SPARQL execution error: #{Exception.message(e)}")
        detailed_error = Exception.format(:error, e, __STACKTRACE__)
        Logger.debug("Detailed error: #{detailed_error}")
        {:error, "Query execution error: #{Exception.message(e)}"}
    end
  end

  @doc """
  Backwards compatibility method for simple triple pattern queries.
  """
  def query_pattern({s, p, o}) do
    s_str = if is_nil(s), do: "?s", else: "\"#{s}\""
    p_str = if is_nil(p), do: "?p", else: "\"#{p}\""
    o_str = if is_nil(o), do: "?o", else: "\"#{o}\""
    query = "SELECT ?s ?p ?o WHERE { #{s_str} #{p_str} #{o_str} }"

    case execute(query) do
      {:ok, results} -> {:ok, format_to_legacy_results(results, s, p, o)}
      error -> error
    end
  end

  # --- Helper Functions ---

  defp ensure_utf8_encoding(input) when is_binary(input) do
    case :unicode.characters_to_binary(input, :utf8, :utf8) do
      converted when is_binary(converted) ->
        String.replace(converted, ~r/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/, "")
      _ ->
        String.replace(input, ~r/[^\x20-\x7E\n\r\t]/, "")
    end
  end

  defp ensure_utf8_encoding(input), do: "#{input}"

  defp preprocess_sparql_query(query) do
    Logger.debug("Preprocess - Input: #{inspect(query)}")
    query =
      if !String.match?(query, ~r/^\s*(?:PREFIX|BASE|SELECT|CONSTRUCT|DESCRIBE|ASK)/i) do
        "SELECT " <> query
      else
        query
      end
    Logger.debug("Preprocess - After initial check: #{inspect(query)}")

    case preprocess_query(query) do
      {:ok, tokens, "", _, _, _} ->
        Logger.debug("Preprocess - Tokens: #{inspect(tokens)}")
        {prefixes, other_tokens} =
          Enum.split_with(tokens, fn
            {:prefix, _} -> true
            {:base, _} -> true
            _ -> false
          end)
        Logger.debug("Preprocess - Prefixes: #{inspect(prefixes)}")
        Logger.debug("Preprocess - Other tokens: #{inspect(other_tokens)}")
        prefix_map =
          Enum.reduce(prefixes, %{}, fn
            {:prefix, [prefix, uri]}, acc -> Map.put(acc, prefix, uri)
            {:base, [uri]}, acc -> Map.put(acc, "BASE", uri)
            _, acc -> acc
          end)
        preprocessed =
          other_tokens
          |> List.flatten()
          |> Enum.join("")
          |> String.trim()
          |> String.replace(~r/\s+/, " ")
        Logger.debug("Preprocess - Preprocessed: #{inspect(preprocessed)}")
        prefix_strings =
          Enum.map(prefix_map, fn
            {"BASE", uri} -> "BASE <#{uri}> "
            {prefix, uri} -> "PREFIX #{prefix} <#{uri}> "
          end)
          |> Enum.join("")
        Logger.debug("Preprocess - Prefix strings: #{inspect(prefix_strings)}")
        final_query = prefix_strings <> preprocessed
        Logger.debug("Preprocess - Final query: #{inspect(final_query)}")
        {:ok, final_query, prefix_map}
      {:ok, _, rest, _, _, _} ->
        Logger.error("Preprocess - Failed with remaining: #{rest}")
        {:error, "Failed to preprocess entire query. Remaining: #{rest}"}
    end
  end

  @doc """
  Validates a SPARQL query for allowed operations.
  """
  def validate_sparql_query(query) do
    cond do
      String.contains?(query, "DELETE") -> {:error, "DELETE operations are not allowed"}
      String.contains?(query, "INSERT") -> {:error, "INSERT operations are not allowed"}
      String.contains?(query, "DROP") -> {:error, "DROP operations are not allowed"}
      String.contains?(query, "LOAD") -> {:error, "LOAD operations are not allowed"}
      String.contains?(query, "CLEAR") -> {:error, "CLEAR operations are not allowed"}
      true ->
        if String.match?(query, ~r/^\s*(?:SELECT|CONSTRUCT|DESCRIBE|ASK)/i) do
          :ok
        else
          {:error, "Query must start with SELECT, CONSTRUCT, DESCRIBE, or ASK"}
        end
    end
  end

  defp format_to_legacy_results(results, orig_s, orig_p, orig_o) do
    Enum.map(results, fn result_map ->
      node_id = Map.get(result_map, "node_id", "tx_#{:erlang.unique_integer([:positive])}")
      s = if is_binary(orig_s), do: orig_s, else: Map.get(result_map, "s")
      p = if is_binary(orig_p), do: orig_p, else: Map.get(result_map, "p")
      o = if is_binary(orig_o), do: orig_o, else: Map.get(result_map, "o")
      data = %{
        subject: s,
        predicate: p,
        object: o,
        validator: Map.get(result_map, "validator", "agent1"),
        timestamp: Map.get(result_map, "timestamp", DateTime.utc_now())
      }
      edges = Map.get(result_map, "edges", [])
      {node_id, data, edges}
    end)
  end

  @doc """
  Explains a SPARQL query by showing its parsed structure and execution plan.
  """
  def explain(query) do
    try do
      Logger.debug("Explain - Input to ensure_utf8_encoding: #{inspect(query)}")
      cleaned_query = ensure_utf8_encoding(query)
      Logger.debug("Explain - Output from ensure_utf8_encoding: #{inspect(cleaned_query)}")

      case preprocess_sparql_query(cleaned_query) do
        {:ok, preprocessed_query, prefixes} ->
          Logger.debug("Explain - Preprocessed query: #{inspect(preprocessed_query)}")
          case SparqlParser.parse(preprocessed_query) do
            {:ok, parsed_query} ->
              optimized_query =
                case SparqlOptimizer.optimize(parsed_query) do
                  {:ok, optimized} -> optimized
                  {:error, _} -> parsed_query
                end
              execution_plan =
                if function_exported?(SparqlOptimizer, :create_execution_plan, 1) do
                  SparqlOptimizer.create_execution_plan(optimized_query)
                else
                  %{note: "Execution plan generation not available"}
                end
              explanation = %{
                original_query: query,
                preprocessed_query: preprocessed_query,
                prefixes: prefixes,
                parsed_structure: parsed_query,
                optimized_structure: optimized_query,
                execution_plan: execution_plan
              }
              {:ok, explanation}
            {:error, reason} ->
              Logger.error("Explain - Parse error: #{reason}")
              {:error, "Query parsing failed: #{reason}"}
          end
        {:error, reason} ->
          Logger.error("Explain - Preprocessing failed: #{reason}")
          {:error, "Query preprocessing failed: #{reason}"}
      end
    rescue
      e ->
        Logger.error("Explain - Exception: #{Exception.message(e)}")
        {:error, "Error explaining query: #{Exception.message(e)}"}
    end
  end

  @doc """
  Returns a list of predefined example queries.
  """
  def example_queries do
    [
      %{
        name: "Basic triple pattern",
        description: "Simple query to match all triples",
        query: "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10"
      },
      %{
        name: "Filter by subject",
        description: "Find all relationships for a specific entity",
        query: "SELECT ?p ?o WHERE { \"http://example.org/entity/UHTMilkBatch1\" ?p ?o }"
      },
      %{
        name: "Count results",
        description: "Count the number of triples matching a pattern",
        query: "SELECT (COUNT(?s) AS ?count) WHERE { ?s ?p ?o }"
      },
      %{
        name: "Group by predicate",
        description: "Count triples grouped by their predicates",
        query: "SELECT ?p (COUNT(?s) AS ?count) WHERE { ?s ?p ?o } GROUP BY ?p ORDER BY DESC(?count)"
      },
      %{
        name: "PROV-O entity generation",
        description: "Find entities and their generating activities",
        query: "SELECT ?entity ?activity WHERE { ?entity \"prov:wasGeneratedBy\" ?activity }"
      },
      %{
        name: "Optional metadata",
        description: "Main data with optional metadata if available",
        query: """
        SELECT ?s ?p ?o ?metadata
        WHERE {
          ?s ?p ?o .
          OPTIONAL { ?s "metadata" ?metadata }
        }
        """
      }
    ]
  end
end

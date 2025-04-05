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
  select_keyword =
    ignore(optional_whitespace)
    |> string("SELECT")
    |> replace(:select)

  construct_keyword =
    ignore(optional_whitespace)
    |> string("CONSTRUCT")
    |> replace(:construct)

  describe_keyword =
    ignore(optional_whitespace)
    |> string("DESCRIBE")
    |> replace(:describe)

  ask_keyword =
    ignore(optional_whitespace)
    |> string("ASK")
    |> replace(:ask)

  query_type = choice([select_keyword, construct_keyword, describe_keyword, ask_keyword])

  # Simple validator for query structure
  query_structure_validator =
    optional(repeat(choice([prefix_decl, base_decl, any_comment, whitespace |> ignore()])))
    |> concat(query_type)
    |> repeat(
      choice([
        any_comment |> ignore(),
        whitespace |> ignore(),
        ascii_char([]) |> utf8_string([], min: 1)
      ])
    )

  defparsec(:parse_query_structure, query_structure_validator)

  # Query preprocessor to normalize and extract metadata
  query_preprocessor =
    repeat(
      choice([
        # Capture prefixes
        prefix_decl,
        # Capture base URI
        base_decl,
        # Ignore comments
        any_comment |> ignore(),
        # Normalize whitespace sequences to a single space
        whitespace |> replace(" "),
        # Keep everything else
        ascii_char([]) |> utf8_string([], min: 1)
      ])
    )

  defparsec(:preprocess_query, query_preprocessor)

  # --- End of Parser Definitions ---

  @doc """
  Executes a SPARQL query against the blockchain data.

  ## Parameters

  - query: A SPARQL query string

  ## Returns

  - {:ok, results} if query executed successfully
  - {:error, reason} otherwise

  ## Examples

      iex> SparqlEngine.execute("SELECT ?s ?p ?o WHERE { ?s ?p ?o }")
      {:ok, [%{"s" => "Alice", "p" => "knows", "o" => "Bob"}, ...]}
  """
  def execute(query) do
    try do
      # Ensure proper encoding and normalization of the input query
      normalized_query =
        query
        |> ensure_utf8_encoding()
        |> String.trim()

      # Log the normalized query for debugging
      Logger.debug("Executing SPARQL query: #{inspect(normalized_query)}")

      # Preprocess the query
      case preprocess_sparql_query(normalized_query) do
        {:ok, preprocessed_query, prefixes} ->
          # Validate the query for security
          case validate_sparql_query(preprocessed_query) do
            :ok ->
              # Parse the SPARQL query
              Logger.debug("Parsing SPARQL query: #{preprocessed_query}")

              case SparqlParser.parse(preprocessed_query) do
                {:ok, parsed_query} ->
                  # Add prefixes to parsed query
                  parsed_query = Map.put(parsed_query, :prefixes, prefixes)
                  Logger.debug("Parsed query structure: #{inspect(parsed_query)}")

                  # Optimize the query
                  optimized_query =
                    case SparqlOptimizer.optimize(parsed_query) do
                      {:ok, optimized} -> optimized
                      # Fall back to non-optimized query
                      {:error, _} -> parsed_query
                    end

                  Logger.debug("Optimized query structure: #{inspect(optimized_query)}")

                  # Execute the optimized query
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
  Converts a simple triple pattern to a SPARQL query and executes it.

  ## Parameters

  - pattern: A tuple {subject, predicate, object} where nil acts as a wildcard

  ## Returns

  - {:ok, results} if query executed successfully
  - {:error, reason} otherwise
  """
  def query_pattern({s, p, o}) do
    # Convert the pattern to a SPARQL query, treating non-nil values as URIs
    s_str = if is_nil(s), do: "?s", else: "\"#{s}\""
    p_str = if is_nil(p), do: "?p", else: "\"#{p}\""
    o_str = if is_nil(o), do: "?o", else: "\"#{o}\""

    query = "SELECT ?s ?p ?o WHERE { #{s_str} #{p_str} #{o_str} }"

    # Execute the query
    case execute(query) do
      {:ok, results} ->
        # Format results to match the legacy format expected by existing code
        legacy_format_results = format_to_legacy_results(results, s, p, o)
        {:ok, legacy_format_results}

      error ->
        error
    end
  end

  # --- Helper Functions ---

  # Ensure proper UTF-8 encoding of the input string
  defp ensure_utf8_encoding(input) when is_binary(input) do
    case :unicode.characters_to_binary(input, :utf8, :utf8) do
      converted when is_binary(converted) ->
        # Replace any potentially problematic control characters
        String.replace(converted, ~r/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/, "")

      _ ->
        # If conversion fails, try another approach - just strip out non-ASCII characters
        String.replace(input, ~r/[^\x20-\x7E\n\r\t]/, "")
    end
  end

  defp ensure_utf8_encoding(input), do: "#{input}"

  # Preprocess SPARQL query to normalize and collect prefixes
  defp preprocess_sparql_query(query) do
    # Ensure query starts with SELECT, CONSTRUCT, etc.
    query =
      if !String.match?(query, ~r/^\s*(?:PREFIX|BASE|SELECT|CONSTRUCT|DESCRIBE|ASK)/i) do
        "SELECT " <> query
      else
        query
      end

    case preprocess_query(query) do
      {:ok, tokens, "", _, _, _} ->
        # Extract prefixes
        {prefixes, other_tokens} =
          Enum.split_with(tokens, fn
            {:prefix, _} -> true
            {:base, _} -> true
            _ -> false
          end)

        # Convert prefix tokens to a usable format
        prefix_map =
          Enum.reduce(prefixes, %{}, fn
            {:prefix, [prefix, uri]}, acc -> Map.put(acc, prefix, uri)
            {:base, [uri]}, acc -> Map.put(acc, "BASE", uri)
            _, acc -> acc
          end)

        # Reconstruct the preprocessed query without losing structure
        preprocessed =
          other_tokens
          |> List.flatten()
          |> Enum.join("")
          |> String.trim()
          |> String.replace(~r/\s+/, " ")

        # Create prefix strings to include in the query
        prefix_strings =
          Enum.map(prefix_map, fn
            {"BASE", uri} -> "BASE <#{uri}> "
            {prefix, uri} -> "PREFIX #{prefix} <#{uri}> "
          end)
          |> Enum.join("")

        # Return preprocessed query with prefix declarations
        final_query = prefix_strings <> preprocessed
        {:ok, final_query, prefix_map}

      {:ok, _, rest, _, _, _} ->
        {:error, "Failed to preprocess entire query. Remaining: #{rest}"}

    end
  end

  @doc """
  Validates a SPARQL query for allowed operations.
  """
  def validate_sparql_query(query) do
    # Check for disallowed operations first (faster than parsing)
    cond do
      String.contains?(query, "DELETE") ->
        {:error, "DELETE operations are not allowed"}

      String.contains?(query, "INSERT") ->
        {:error, "INSERT operations are not allowed"}

      String.contains?(query, "DROP") ->
        {:error, "DROP operations are not allowed"}

      String.contains?(query, "LOAD") ->
        {:error, "LOAD operations are not allowed"}

      String.contains?(query, "CLEAR") ->
        {:error, "CLEAR operations are not allowed"}

      true ->
        # Verify it starts with a valid query type
        if String.match?(query, ~r/^\s*(?:SELECT|CONSTRUCT|DESCRIBE|ASK)/i) do
          :ok
        else
          {:error, "Query must start with SELECT, CONSTRUCT, DESCRIBE, or ASK"}
        end
    end
  end

  defp format_to_legacy_results(results, orig_s, orig_p, orig_o) do
    # The storage engine returns results as {node_id, data, edges}
    # We need to simulate this format from our SPARQL results
    Enum.map(results, fn result_map ->
      node_id = Map.get(result_map, "node_id", "tx_#{:erlang.unique_integer([:positive])}")

      # Fill in the original values for any constants in the pattern
      s =
        cond do
          is_binary(orig_s) -> orig_s
          true -> Map.get(result_map, "s")
        end

      p =
        cond do
          is_binary(orig_p) -> orig_p
          true -> Map.get(result_map, "p")
        end

      o =
        cond do
          is_binary(orig_o) -> orig_o
          true -> Map.get(result_map, "o")
        end

      # Construct data map with all required fields
      data = %{
        subject: s,
        predicate: p,
        object: o,
        validator: Map.get(result_map, "validator", "agent1"),
        timestamp: Map.get(result_map, "timestamp", DateTime.utc_now())
      }

      # Get edges if available
      edges = Map.get(result_map, "edges", [])

      # Return in the expected format
      {node_id, data, edges}
    end)
  end

  @doc """
  Explains a SPARQL query by showing its parsed structure and execution plan.
  Useful for debugging and understanding query performance.

  ## Parameters

  - query: A SPARQL query string

  ## Returns

  - {:ok, explanation} with the query explanation
  - {:error, reason} if the query could not be parsed
  """
  def explain(query) do
    try do
      # Clean the query first
      cleaned_query = ensure_utf8_encoding(query)

      # Preprocess the query
      case preprocess_sparql_query(cleaned_query) do
        {:ok, preprocessed_query, prefixes} ->
          # Parse the query to get structure
          case SparqlParser.parse(preprocessed_query) do
            {:ok, parsed_query} ->
              # Optimize the query and handle possible error
              optimized_query =
                case SparqlOptimizer.optimize(parsed_query) do
                  {:ok, optimized} -> optimized
                  # Fall back to non-optimized query
                  {:error, _} -> parsed_query
                end

              # Generate execution plan if available
              execution_plan =
                if function_exported?(SparqlOptimizer, :create_execution_plan, 1) do
                  SparqlOptimizer.create_execution_plan(optimized_query)
                else
                  %{note: "Execution plan generation not available"}
                end

              # Build explanation
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
              {:error, "Query parsing failed: #{reason}"}
          end

        {:error, reason} ->
          {:error, "Query preprocessing failed: #{reason}"}
      end
    rescue
      e ->
        {:error, "Error explaining query: #{Exception.message(e)}"}
    end
  end

  @doc """
  Returns a list of predefined example queries that demonstrate
  various SPARQL features supported by the Kylix engine.

  ## Returns

  - List of example queries with descriptions
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
        query:
          "SELECT ?p (COUNT(?s) AS ?count) WHERE { ?s ?p ?o } GROUP BY ?p ORDER BY DESC(?count)"
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

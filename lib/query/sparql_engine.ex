defmodule Kylix.Query.SparqlEngine do
  @moduledoc """
  Provides SPARQL query capabilities for the blockchain data.

  This module integrates the SPARQL parser and executor to provide
  a complete SPARQL query solution for the Kylix blockchain.
  """

  alias Kylix.Query.SparqlParser
  alias Kylix.Query.SparqlExecutor
  require Logger

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
      # Validate the query for security
      case validate_query(query) do
        :ok ->
          # Parse the SPARQL query
          Logger.debug("Parsing SPARQL query: #{query}")
          case SparqlParser.parse(query) do
            {:ok, parsed_query} ->
              Logger.debug("Parsed query structure: #{inspect(parsed_query)}")

              # Optimize the query
              alias Kylix.Query.SparqlOptimizer
              optimized_query = SparqlOptimizer.optimize(parsed_query)
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
    rescue
      e ->
        Logger.error("SPARQL execution error: #{Exception.message(e)}")
        {:error, Exception.message(e)}
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
    # Convert the pattern to a SPARQL query
    s_str = if is_nil(s), do: "?s", else: "\"#{s}\""
    p_str = if is_nil(p), do: "?p", else: "\"#{p}\""
    o_str = if is_nil(o), do: "?o", else: "\"#{o}\""

    query = "SELECT ?s ?p ?o WHERE { #{s_str} #{p_str} #{o_str} }"

    # Execute the query
    case execute(query) do
      {:ok, results} ->
        # Format results to match the legacy format expected by existing code
        legacy_format_results = format_to_legacy_results(results)
        {:ok, legacy_format_results}

      error -> error
    end
  end

  defp format_to_legacy_results(results) do
    # The storage engine returns results as {node_id, data, edges}
    # We need to simulate this format from our SPARQL results
    Enum.map(results, fn result_map ->
      node_id = Map.get(result_map, "node_id", "unknown")

      # Construct data map
      data = %{
        subject: Map.get(result_map, "s"),
        predicate: Map.get(result_map, "p"),
        object: Map.get(result_map, "o"),
        validator: Map.get(result_map, "validator"),
        timestamp: Map.get(result_map, "timestamp")
      }

      # Get edges if available
      edges = Map.get(result_map, "edges", [])

      # Return in the expected format
      {node_id, data, edges}
    end)
  end

  @doc """
  Performs query plan optimization for a parsed SPARQL query.
  """
  def optimize_query_plan(parsed_query) do
    # This is a placeholder for future optimization logic
    # For now, it just returns the original query unchanged

    # Possible optimizations:
    # 1. Reorder triple patterns for most selective first
    # 2. Push filters down to be applied as early as possible
    # 3. Rewrite certain patterns for more efficient execution

    parsed_query
  end

  @doc """
  Validates a SPARQL query for allowed operations.
  """
  def validate_query(query) do
    # Check for disallowed features or security concerns
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
        :ok
    end
  end
end

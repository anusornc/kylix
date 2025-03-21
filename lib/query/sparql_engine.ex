defmodule Kylix.Query.SparqlEngine do
  @moduledoc """
  Provides SPARQL query capabilities for the blockchain data.

  This module integrates the SPARQL parser and executor to provide
  a complete SPARQL query solution for the Kylix blockchain.
  """

  alias Kylix.Query.SparqlParser
  alias Kylix.Query.SparqlExecutor
  alias Kylix.Query.SparqlOptimizer
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
        legacy_format_results = format_to_legacy_results(results, s, p, o)
        {:ok, legacy_format_results}

      error -> error
    end
  end

  defp format_to_legacy_results(results, orig_s, orig_p, orig_o) do
    # The storage engine returns results as {node_id, data, edges}
    # We need to simulate this format from our SPARQL results
    Enum.map(results, fn result_map ->
      node_id = Map.get(result_map, "node_id", "tx_#{:erlang.unique_integer([:positive])}")

      # Fill in the original values for any constants in the pattern
      s = cond do
        is_binary(orig_s) -> orig_s
        true -> Map.get(result_map, "s")
      end

      p = cond do
        is_binary(orig_p) -> orig_p
        true -> Map.get(result_map, "p")
      end

      o = cond do
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

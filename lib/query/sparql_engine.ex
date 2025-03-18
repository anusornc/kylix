defmodule Kylix.Query.SparqlEngine do
  @moduledoc """
  Provides SPARQL query capabilities for the blockchain data.
  """

  alias Kylix.Storage.PersistentDAGEngine, as: DAG

  @doc """
  Executes a SPARQL query against the blockchain data.

  ## Parameters

  - query: A SPARQL query string

  ## Returns

  - {:ok, results} if query executed successfully
  - {:error, reason} otherwise
  """
  def execute(query) do
    try do
      # Parse the SPARQL query
      parsed_query = parse_sparql(query)

      # Execute the query against the blockchain data
      results = execute_parsed_query(parsed_query)

      {:ok, results}
    rescue
      e -> {:error, Exception.message(e)}
    end
  end

  # This is a simplified parser - a real implementation would use a proper SPARQL parser
  defp parse_sparql(query) do
    # Extract the basic triple pattern from a simple SELECT query
    # This is a very simplified example - a real implementation would be much more complex
    pattern_regex = ~r/SELECT\s+.+\s+WHERE\s+\{\s*(?<s>.+?)\s+(?<p>.+?)\s+(?<o>.+?)\s*\}/is

    case Regex.named_captures(pattern_regex, query) do
      %{"s" => s, "p" => p, "o" => o} ->
        # Convert variables to nil for pattern matching
        s = if String.starts_with?(s, "?"), do: nil, else: String.trim(s, "\"")
        p = if String.starts_with?(p, "?"), do: nil, else: String.trim(p, "\"")
        o = if String.starts_with?(o, "?"), do: nil, else: String.trim(o, "\"")

        {s, p, o}

      nil ->
        raise "Invalid SPARQL query format"
    end
  end

  defp execute_parsed_query(pattern) do
    # Use the DAG engine to execute the query
    case DAG.query(pattern) do
      {:ok, results} ->
        # Transform results to match SPARQL result format
        Enum.map(results, fn {_node_id, data, _edges} ->
          %{
            "subject" => data.subject,
            "predicate" => data.predicate,
            "object" => data.object,
            "validator" => data.validator,
            "timestamp" => DateTime.to_iso8601(data.timestamp)
          }
        end)

      error ->
        raise "Query execution failed: #{inspect(error)}"
    end
  end
end

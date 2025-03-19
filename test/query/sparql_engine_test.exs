defmodule Kylix.Query.SparqlEngineTest do
  use ExUnit.Case
  alias Kylix.Storage.DAGEngine

  # Mock module for SparqlEngine to use the in-memory DAGEngine in test mode
  defmodule MockSparqlEngine do
    # Use the same API as the real SparqlEngine but use DAGEngine instead of PersistentDAGEngine
    def execute(query) do
      try do
        # Parse the SPARQL query
        parsed_query = parse_sparql(query)

        # Execute the query against the in-memory DAG
        results = execute_parsed_query(parsed_query)

        {:ok, results}
      rescue
        e -> {:error, Exception.message(e)}
      end
    end

    # This is the same parser as in the original SparqlEngine
    defp parse_sparql(query) do
      # Extract the basic triple pattern from a simple SELECT query
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

    # Modified to use DAGEngine instead of PersistentDAGEngine
    defp execute_parsed_query(pattern) do
      # Use the in-memory DAGEngine for tests
      case DAGEngine.query(pattern) do
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

  setup do
    # Stop and restart the application with a clean slate
    :ok = Application.stop(:kylix)
    {:ok, _} = Application.ensure_all_started(:kylix)

    # Add test data to the DAG
    setup_test_data()

    :ok
  end

  describe "SPARQL query execution" do
    test "executes simple SELECT query with exact match" do
      query = """
      SELECT ?s ?p ?o WHERE {
        "Alice" "knows" "Bob"
      }
      """

      {:ok, results} = MockSparqlEngine.execute(query)

      assert length(results) == 1
      result = hd(results)
      assert result["subject"] == "Alice"
      assert result["predicate"] == "knows"
      assert result["object"] == "Bob"
    end

    test "executes query with subject variable" do
      query = """
      SELECT ?s ?p ?o WHERE {
        ?s "likes" "Pizza"
      }
      """

      {:ok, results} = MockSparqlEngine.execute(query)

      assert length(results) == 1
      result = hd(results)
      assert result["subject"] == "Alice"
      assert result["predicate"] == "likes"
      assert result["object"] == "Pizza"
    end

    test "executes query with predicate variable" do
      query = """
      SELECT ?s ?p ?o WHERE {
        "Bob" ?p "Charlie"
      }
      """

      {:ok, results} = MockSparqlEngine.execute(query)

      assert length(results) == 1
      result = hd(results)
      assert result["subject"] == "Bob"
      assert result["predicate"] == "knows"
      assert result["object"] == "Charlie"
    end

    test "executes query with object variable" do
      query = """
      SELECT ?s ?p ?o WHERE {
        "Bob" "likes" ?o
      }
      """

      {:ok, results} = MockSparqlEngine.execute(query)

      assert length(results) == 1
      result = hd(results)
      assert result["subject"] == "Bob"
      assert result["predicate"] == "likes"
      assert result["object"] == "Sushi"
    end

    test "executes query with multiple results" do
      query = """
      SELECT ?s ?p ?o WHERE {
        ?s "knows" ?o
      }
      """

      {:ok, results} = MockSparqlEngine.execute(query)

      assert length(results) == 2

      # Sort results by subject for consistent assertions
      sorted_results = Enum.sort_by(results, & &1["subject"])

      alice = Enum.at(sorted_results, 0)
      assert alice["subject"] == "Alice"
      assert alice["predicate"] == "knows"
      assert alice["object"] == "Bob"

      bob = Enum.at(sorted_results, 1)
      assert bob["subject"] == "Bob"
      assert bob["predicate"] == "knows"
      assert bob["object"] == "Charlie"
    end

    test "returns empty result for non-matching query" do
      query = """
      SELECT ?s ?p ?o WHERE {
        "Unknown" "predicate" "value"
      }
      """

      {:ok, results} = MockSparqlEngine.execute(query)
      assert Enum.empty?(results)
    end

    test "returns error for invalid query format" do
      query = "This is not a valid SPARQL query"

      result = MockSparqlEngine.execute(query)
      assert {:error, _message} = result
    end
  end

  # Helper function to set up test data in the DAG
  defp setup_test_data do
    # Create nodes with triple-like data for testing
    DAGEngine.add_node("tx1", %{
      subject: "Alice",
      predicate: "knows",
      object: "Bob",
      validator: "test-validator",
      timestamp: DateTime.utc_now()
    })

    DAGEngine.add_node("tx2", %{
      subject: "Alice",
      predicate: "likes",
      object: "Pizza",
      validator: "test-validator",
      timestamp: DateTime.utc_now()
    })

    DAGEngine.add_node("tx3", %{
      subject: "Bob",
      predicate: "knows",
      object: "Charlie",
      validator: "test-validator",
      timestamp: DateTime.utc_now()
    })

    DAGEngine.add_node("tx4", %{
      subject: "Bob",
      predicate: "likes",
      object: "Sushi",
      validator: "test-validator",
      timestamp: DateTime.utc_now()
    })

    # Add some edges between related transactions
    DAGEngine.add_edge("tx1", "tx2", "same_subject")
    DAGEngine.add_edge("tx3", "tx4", "same_subject")
    DAGEngine.add_edge("tx1", "tx3", "knows_chain")
  end
end

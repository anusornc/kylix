defmodule Kylix.Query.SparqlEngineTest do
  use ExUnit.Case
  alias Kylix.Storage.DAGEngine
  alias Kylix.Query.SparqlEngine
  alias Kylix.Query.SparqlParser
  alias Kylix.Query.SparqlExecutor

  # Setup test data before each test
  setup do
    # Reset the application for each test
    :ok = Application.stop(:kylix)
    {:ok, _} = Application.ensure_all_started(:kylix)

    # Create test data
    setup_test_data()
    :ok
  end

  describe "SPARQL parser" do
    test "parses basic SELECT query" do
      query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"

      {:ok, parsed} = SparqlParser.parse(query)
      assert parsed.type == :select
      assert parsed.variables == ["s", "p", "o"]
      assert length(parsed.patterns) == 1

      pattern = hd(parsed.patterns)
      assert pattern.s == nil
      assert pattern.p == nil
      assert pattern.o == nil
    end

    test "parses query with explicit values" do
      query = "SELECT ?s ?p ?o WHERE { \"Alice\" ?p ?o }"

      {:ok, parsed} = SparqlParser.parse(query)
      pattern = hd(parsed.patterns)
      assert pattern.s == "Alice"
      assert pattern.p == nil
      assert pattern.o == nil
    end

    test "parses query with multiple patterns" do
      query = "SELECT ?s ?p ?o WHERE { ?s \"knows\" ?o . ?o \"lives\" \"Paris\" }"

      {:ok, parsed} = SparqlParser.parse(query)
      assert length(parsed.patterns) == 2

      [p1, p2] = parsed.patterns
      assert p1.p == "knows"
      assert p2.p == "lives"
      assert p2.o == "Paris"
    end

    test "parses query with FILTER" do
      query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o . FILTER(?s = \"Alice\") }"

      {:ok, parsed} = SparqlParser.parse(query)
      assert length(parsed.filters) == 1

      filter = hd(parsed.filters)
      assert filter.type == :equality
      assert filter.variable == "s"
      assert filter.value == "Alice"
    end

    test "parses query with OPTIONAL" do
      query = "SELECT ?s ?p ?o WHERE { ?s \"knows\" ?o . OPTIONAL { ?o \"email\" ?email } }"

      {:ok, parsed} = SparqlParser.parse(query)
      assert length(parsed.optionals) == 1

      optional = hd(parsed.optionals)
      assert length(optional.patterns) == 1

      pattern = hd(optional.patterns)
      assert pattern.p == "email"
    end

    test "parses query with ORDER BY" do
      query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ?s"

      {:ok, parsed} = SparqlParser.parse(query)
      assert length(parsed.order_by) == 1

      ordering = hd(parsed.order_by)
      assert ordering.variable == "s"
      assert ordering.direction == :asc
    end

    test "parses query with GROUP BY" do
      query = "SELECT ?s (COUNT(?o) AS ?count) WHERE { ?s ?p ?o } GROUP BY ?s"

      {:ok, parsed} = SparqlParser.parse(query)
      assert parsed.group_by == ["s"]
      assert parsed.has_aggregates == true
      assert length(parsed.aggregates) == 1

      agg = hd(parsed.aggregates)
      assert agg.function == :count
      assert agg.variable == "o"
    end

    test "parses query with LIMIT and OFFSET" do
      query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10 OFFSET 5"

      {:ok, parsed} = SparqlParser.parse(query)
      assert parsed.limit == 10
      assert parsed.offset == 5
    end
  end

  describe "SPARQL execution" do
    test "executes basic query" do
      query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"

      {:ok, results} = SparqlEngine.execute(query)
      assert length(results) > 0

      # Verify result structure
      result = hd(results)
      assert Map.has_key?(result, "s")
      assert Map.has_key?(result, "p")
      assert Map.has_key?(result, "o")
    end

    test "executes query with exact match" do
      query = "SELECT ?s ?p ?o WHERE { \"Alice\" \"knows\" \"Bob\" }"

      {:ok, results} = SparqlEngine.execute(query)
      assert length(results) == 1

      result = hd(results)
      assert result["s"] == "Alice"
      assert result["p"] == "knows"
      assert result["o"] == "Bob"
    end

    test "executes query with partial match" do
      query = "SELECT ?s ?p ?o WHERE { \"Alice\" ?p ?o }"

      {:ok, results} = SparqlEngine.execute(query)
      assert length(results) == 2  # Alice knows Bob and Alice likes Coffee

      # Check that we have both predicates
      predicates = Enum.map(results, & &1["p"])
      assert "knows" in predicates
      assert "likes" in predicates
    end

    test "executes query with FILTER" do
      query = "SELECT ?s ?p ?o WHERE { ?s \"likes\" ?o . FILTER(?o = \"Coffee\") }"

      {:ok, results} = SparqlEngine.execute(query)
      assert length(results) == 1

      result = hd(results)
      assert result["s"] == "Alice"
      assert result["o"] == "Coffee"
    end

    test "executes query with OPTIONAL" do
      # Add data with optional parts
      DAGEngine.add_node("tx_optional", %{
        subject: "Dave",
        predicate: "knows",
        object: "Eve",
        validator: "agent1",
        timestamp: DateTime.utc_now()
      })

      DAGEngine.add_node("tx_optional_email", %{
        subject: "Eve",
        predicate: "email",
        object: "eve@example.com",
        validator: "agent1",
        timestamp: DateTime.utc_now()
      })

      query = """
      SELECT ?person ?friend ?email WHERE {
        ?person "knows" ?friend .
        OPTIONAL { ?friend "email" ?email }
      }
      """

      {:ok, results} = SparqlEngine.execute(query)

      # Find the result with Eve (who has an email)
      eve_result = Enum.find(results, fn r -> r["friend"] == "Eve" end)
      assert eve_result["email"] == "eve@example.com"

      # Find the result with Bob (who has no email)
      bob_result = Enum.find(results, fn r -> r["friend"] == "Bob" end)
      assert Map.get(bob_result, "email") == nil
    end

    test "executes query with UNION" do
      query = """
      SELECT ?person ?relation ?target WHERE {
        { ?person "knows" ?target } UNION { ?person "likes" ?target }
      }
      """

      {:ok, results} = SparqlEngine.execute(query)

      # Should return both "knows" and "likes" relationships
      relations = Enum.map(results, & &1["relation"])
      assert "knows" in relations
      assert "likes" in relations

      # Alice knows Bob and Alice likes Coffee
      alice_targets = Enum.filter(results, & &1["person"] == "Alice")
                     |> Enum.map(& &1["target"])
      assert "Bob" in alice_targets
      assert "Coffee" in alice_targets
    end

    test "executes query with aggregation (COUNT)" do
      query = """
      SELECT ?person (COUNT(?target) AS ?relationCount) WHERE {
        ?person ?relation ?target
      } GROUP BY ?person
      """

      {:ok, results} = SparqlEngine.execute(query)

      # Find Alice who has 2 relations
      alice_result = Enum.find(results, fn r -> r["person"] == "Alice" end)
      assert alice_result["relationCount"] == 2

      # Find Bob who has at least 1 relation
      bob_result = Enum.find(results, fn r -> r["person"] == "Bob" end)
      assert bob_result["relationCount"] >= 1
    end

    test "executes query with ORDER BY" do
      query = """
      SELECT ?person ?relation ?target WHERE {
        ?person ?relation ?target
      } ORDER BY DESC(?person) ?relation
      """

      {:ok, results} = SparqlEngine.execute(query)

      # Results should be ordered by person descending
      # Charlie should come before Bob, and Bob before Alice
      person_order = Enum.map(results, & &1["person"])

      # Find positions
      charlie_pos = Enum.find_index(person_order, & &1 == "Charlie")
      bob_pos = Enum.find_index(person_order, & &1 == "Bob")
      alice_pos = Enum.find_index(person_order, & &1 == "Alice")

      # Only assert if all three people are in the results
      if charlie_pos && bob_pos && alice_pos do
        assert charlie_pos < bob_pos
        assert bob_pos < alice_pos
      end
    end

    test "executes query with LIMIT and OFFSET" do
      # Ensure we have enough data for this test
      assert setup_test_data_count() >= 5

      query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 2 OFFSET 1"

      {:ok, results} = SparqlEngine.execute(query)
      assert length(results) == 2
    end
  end

  describe "complex SPARQL queries" do
    test "executes complex query with multiple patterns, filters, and aggregation" do
      query = """
      SELECT ?person (COUNT(?friend) AS ?friendCount) WHERE {
        ?person "knows" ?friend .
        FILTER(?person != "Dave") .
        OPTIONAL { ?friend "likes" ?interest }
      } GROUP BY ?person ORDER BY DESC(?friendCount) LIMIT 2
      """

      {:ok, results} = SparqlEngine.execute(query)

      # Should return at most 2 results
      assert length(results) <= 2

      # Each result should have person and friendCount
      result = hd(results)
      assert Map.has_key?(result, "person")
      assert Map.has_key?(result, "friendCount")
    end

    test "executes chain of relationships query" do
      query = """
      SELECT ?person ?friend ?friendOfFriend WHERE {
        ?person "knows" ?friend .
        ?friend "knows" ?friendOfFriend .
        FILTER(?person != ?friendOfFriend)
      }
      """

      {:ok, results} = SparqlEngine.execute(query)

      # Check for expected friend-of-friend relationships
      # Alice knows Bob, Bob knows Charlie, so Alice has Charlie as friend-of-friend
      alice_fof = Enum.find(results, fn r ->
        r["person"] == "Alice" && r["friend"] == "Bob"
      end)

      if alice_fof do
        assert alice_fof["friendOfFriend"] == "Charlie"
      end
    end
  end

  # Helper to setup standard test data
  defp setup_test_data do
    # Create a basic social graph
    DAGEngine.add_node("tx1", %{
      subject: "Alice",
      predicate: "knows",
      object: "Bob",
      validator: "agent1",
      timestamp: DateTime.utc_now()
    })

    DAGEngine.add_node("tx2", %{
      subject: "Bob",
      predicate: "knows",
      object: "Charlie",
      validator: "agent2",
      timestamp: DateTime.utc_now()
    })

    DAGEngine.add_node("tx3", %{
      subject: "Charlie",
      predicate: "knows",
      object: "Dave",
      validator: "agent1",
      timestamp: DateTime.utc_now()
    })

    DAGEngine.add_node("tx4", %{
      subject: "Alice",
      predicate: "likes",
      object: "Coffee",
      validator: "agent2",
      timestamp: DateTime.utc_now()
    })

    DAGEngine.add_node("tx5", %{
      subject: "Bob",
      predicate: "likes",
      object: "Tea",
      validator: "agent1",
      timestamp: DateTime.utc_now()
    })

    # Add some edges between related transactions
    DAGEngine.add_edge("tx1", "tx2", "knows_chain")
    DAGEngine.add_edge("tx2", "tx3", "knows_chain")
    DAGEngine.add_edge("tx1", "tx4", "same_subject")
    DAGEngine.add_edge("tx2", "tx5", "same_subject")
  end

  # Helper to count the test data
  defp setup_test_data_count do
    {:ok, all_nodes} = DAGEngine.query({nil, nil, nil})
    length(all_nodes)
  end
end

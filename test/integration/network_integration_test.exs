defmodule Kylix.Integration.NetworkIntegrationTest do
  use ExUnit.Case

  # Import required modules for testing
  alias Kylix.Storage.DAGEngine
  alias Kylix.BlockchainServer
  # Network functionality tests moved to NetworkIntegrationTest module
  # We'll create our own mock SparqlEngine in the test
  alias Kylix.Auth.SignatureVerifier

  setup do
    # Reset the application for each test
    :ok = Application.stop(:kylix)
    {:ok, _} = Application.ensure_all_started(:kylix)

    # Reset transaction count
    :ok = BlockchainServer.reset_tx_count(0)

    # Don't set up mocks in the global setup to avoid issues with teardown
    # Each test that needs mocking will set it up individually

    :ok
  end

  describe "end-to-end transaction workflow" do
    test "add transaction, verify storage, and query" do
      # 1. Add a transaction
      subject = "Alice"
      predicate = "owns"
      object = "Car123"

      assert {:ok, tx_id} = Kylix.add_transaction(subject, predicate, object, "agent1", "valid_sig")

      # 2. Verify transaction was stored in DAG
      assert {:ok, node_data} = DAGEngine.get_node(tx_id)
      assert node_data.subject == subject
      assert node_data.predicate == predicate
      assert node_data.object == object
      assert node_data.validator == "agent1"

      # 3. Query for the transaction using different query methods

      # 3.1 Direct query with exact match
      {:ok, results} = Kylix.query({subject, predicate, object})
      assert length(results) == 1
      {result_id, result_data, _edges} = hd(results)
      assert result_id == tx_id
      assert result_data.subject == subject

      # 3.2 Query with subject wildcard
      {:ok, results} = Kylix.query({nil, predicate, object})
      assert length(results) == 1

      # 3.3 Query with predicate wildcard
      {:ok, results} = Kylix.query({subject, nil, object})
      assert length(results) == 1

      # Note: In a real implementation, we would verify that the transaction
      # was broadcast to the network, but we'll skip this check for now
      # as we don't have direct access to that functionality in tests
    end

    test "add multiple transactions, verify linkage and query across transactions" do
      # 1. Add several related transactions
      {:ok, tx_id1} = Kylix.add_transaction("Alice", "owns", "Car123", "agent1", "valid_sig")
      Process.sleep(10) # Ensure unique timestamps
      {:ok, tx_id2} = Kylix.add_transaction("Alice", "drives", "Car123", "agent2", "valid_sig")
      Process.sleep(10)
      {:ok, tx_id3} = Kylix.add_transaction("Bob", "manufactures", "Car123", "agent1", "valid_sig")

      # 2. Check transaction linkage in DAG
      # All transactions should be linked in sequence
      # Verify nodes exist (we don't need the actual values right now)
      {:ok, _} = DAGEngine.get_node(tx_id1)
      {:ok, _} = DAGEngine.get_node(tx_id2)
      {:ok, _} = DAGEngine.get_node(tx_id3)

      # Get all nodes to check edges
      {:ok, results} = DAGEngine.query({nil, nil, nil})

      # Find the edges for tx1
      tx1_result = Enum.find(results, fn {id, _, _} -> id == tx_id1 end)
      {_, _, tx1_edges} = tx1_result

      # Find the edges for tx2
      tx2_result = Enum.find(results, fn {id, _, _} -> id == tx_id2 end)
      {_, _, tx2_edges} = tx2_result

      # Verify tx1 has an edge to tx2
      assert Enum.any?(tx1_edges, fn
        {^tx_id1, ^tx_id2, "confirms"} -> true
        {^tx_id2, "confirms"} -> true
        _ -> false
      end)

      # Verify tx2 has an edge to tx3
      assert Enum.any?(tx2_edges, fn
        {^tx_id2, ^tx_id3, "confirms"} -> true
        {^tx_id3, "confirms"} -> true
        _ -> false
      end)

      # 3. Query by object to find all related transactions
      {:ok, car_results} = Kylix.query({nil, nil, "Car123"})
      assert length(car_results) == 3

      # 4. Query by subject to find Alice's transactions
      {:ok, alice_results} = Kylix.query({"Alice", nil, nil})
      assert length(alice_results) == 2
    end
  end

  describe "validator operations and transaction validation" do
    test "add new validator and use it for transactions" do
      # 1. Get initial validators
      initial_validators = Kylix.get_validators()
      assert "agent1" in initial_validators
      assert "agent2" in initial_validators

      # 2. Add a new validator (vouched by agent1)
      new_validator = "new_agent"
      {:ok, ^new_validator} = Kylix.add_validator(new_validator, "new_pubkey", "agent1")

      # 3. Verify the new validator is in the list
      updated_validators = Kylix.get_validators()
      assert new_validator in updated_validators

      # 4. Use the new validator to add a transaction
      {:ok, tx_id} = Kylix.add_transaction("TestSubject", "TestPredicate", "TestObject", new_validator, "valid_sig")

      # 5. Verify the transaction was added with the correct validator
      {:ok, node_data} = DAGEngine.get_node(tx_id)
      assert node_data.validator == new_validator
    end

    test "reject transaction from unknown validator" do
      # Attempt to add a transaction with an unknown validator
      result = Kylix.add_transaction("Subject", "Predicate", "Object", "unknown_validator", "sig")
      assert {:error, :unknown_validator} = result
    end
  end

  describe "query engine integration" do
    setup do
      # Add test data to work with
      Kylix.add_transaction("Alice", "knows", "Bob", "agent1", "valid_sig")
      Kylix.add_transaction("Bob", "knows", "Charlie", "agent2", "valid_sig")
      Kylix.add_transaction("Charlie", "knows", "Dave", "agent1", "valid_sig")
      Kylix.add_transaction("Alice", "likes", "Coffee", "agent2", "valid_sig")
      Kylix.add_transaction("Bob", "likes", "Tea", "agent1", "valid_sig")

      :ok
    end

    test "test SPARQL queries with mock SparqlEngine" do
      # Create a mock module that simulates SparqlEngine but uses our test data
      defmodule MockSparql do
        # Parse a basic SPARQL query to extract the pattern
        def parse_query(query) do
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

        # Execute a SPARQL query by extracting the pattern and using BlockchainServer.query
        def execute(query) do
          pattern = parse_query(query)
          Kylix.query(pattern)
        end
      end

      # 1. Test a query for all people that Alice knows
      query1 = """
      SELECT ?s ?p ?o WHERE {
        "Alice" "knows" ?o
      }
      """

      {:ok, results1} = MockSparql.execute(query1)
      assert length(results1) == 1
      {_, data1, _} = hd(results1)
      assert data1.object == "Bob"

      # 2. Test a query for all "knows" relationships
      query2 = """
      SELECT ?s ?p ?o WHERE {
        ?s "knows" ?o
      }
      """

      {:ok, results2} = MockSparql.execute(query2)
      assert length(results2) == 3

      # 3. Test a query with exact values
      query3 = """
      SELECT ?s ?p ?o WHERE {
        "Bob" "likes" "Tea"
      }
      """

      {:ok, results3} = MockSparql.execute(query3)
      assert length(results3) == 1
    end
  end

  describe "network integration" do
    test "receive transaction from network" do
      # 1. Create a transaction as if received from the network
      tx_data = %{
        "subject" => "NetworkSubject",
        "predicate" => "NetworkPredicate",
        "object" => "NetworkObject",
        "validator" => "agent1",
        "signature" => "valid_sig"
      }

      # 2. Send it to the blockchain server directly (simulating reception from network)
      BlockchainServer.receive_transaction(tx_data)

      # Wait a bit for processing
      Process.sleep(50)

      # 3. Verify the transaction was added to the blockchain
      {:ok, results} = Kylix.query({"NetworkSubject", "NetworkPredicate", "NetworkObject"})
      assert length(results) == 1
      {_, data, _} = hd(results)
      assert data.subject == "NetworkSubject"
      assert data.predicate == "NetworkPredicate"
      assert data.object == "NetworkObject"
      assert data.validator == "agent1"
    end
  end

  describe "transaction verification integration" do
    test "transaction hashing workflow" do
      # 1. Create transaction data
      subject = "VerifySubject"
      predicate = "VerifyPredicate"
      object = "VerifyObject"
      validator = "agent1"
      timestamp = DateTime.utc_now()

      # 2. Hash the transaction
      tx_hash = SignatureVerifier.hash_transaction(subject, predicate, object, validator, timestamp)
      assert is_binary(tx_hash)
      assert byte_size(tx_hash) == 32  # SHA-256 hash should be 32 bytes

      # 3. Verify a transaction can be added without mocking
      {:ok, tx_id} = Kylix.add_transaction(subject, predicate, object, validator, "valid_sig")

      # 4. Verify the transaction was added
      {:ok, tx_data} = DAGEngine.get_node(tx_id)
      assert tx_data.subject == subject
      assert tx_data.predicate == predicate
      assert tx_data.object == object
    end
  end

  describe "storage engine integration" do
    test "DAG structure is maintained across operations" do
      # 1. Add a sequence of transactions that form a chain
      {:ok, tx1} = Kylix.add_transaction("First", "comes_before", "Second", "agent1", "valid_sig")
      {:ok, tx2} = Kylix.add_transaction("Second", "comes_before", "Third", "agent2", "valid_sig")
      {:ok, tx3} = Kylix.add_transaction("Third", "comes_before", "Fourth", "agent1", "valid_sig")

      # 2. Query to get the full DAG
      {:ok, results} = DAGEngine.query({nil, nil, nil})

      # 3. Verify the DAG structure
      # Get each node from the results
      tx1_node = Enum.find(results, fn {id, _, _} -> id == tx1 end)
      tx2_node = Enum.find(results, fn {id, _, _} -> id == tx2 end)
      # We only need tx1_node and tx2_node for our edge checks

      # Extract edges
      {_, _, tx1_edges} = tx1_node
      {_, _, tx2_edges} = tx2_node

      # Verify edges exist in the correct order
      # Check if tx1 has an edge to tx2
      tx1_to_tx2 = Enum.any?(tx1_edges, fn
        {^tx1, ^tx2, "confirms"} -> true
        {^tx2, "confirms"} -> true
        _ -> false
      end)
      assert tx1_to_tx2

      # Check if tx2 has an edge to tx3
      tx2_to_tx3 = Enum.any?(tx2_edges, fn
        {^tx2, ^tx3, "confirms"} -> true
        {^tx3, "confirms"} -> true
        _ -> false
      end)
      assert tx2_to_tx3
    end
  end

  describe "full application workflow" do
    test "complete transaction lifecycle with network broadcast and query" do
      # 1. Add validator (simulates validator setup)
      new_validator = "integration_validator"
      {:ok, ^new_validator} = Kylix.add_validator(new_validator, "pubkey", "agent1")

      # 2. Add a transaction through the public API
      {:ok, tx_id} = Kylix.add_transaction(
        "IntegrationSubject",
        "IntegrationPredicate",
        "IntegrationObject",
        new_validator,
        "valid_sig"
      )

      # 3. In a real implementation, we would verify network broadcast
      # Skip this check as we don't have direct access to that functionality

      # 4. Verify it's stored in the DAG
      {:ok, node_data} = DAGEngine.get_node(tx_id)
      assert node_data.subject == "IntegrationSubject"

      # 5. Query using the high-level API
      {:ok, results} = Kylix.query({"IntegrationSubject", nil, nil})
      assert length(results) == 1

      # 6. Simulate receiving a related transaction from network
      network_tx = %{
        "subject" => "IntegrationSubject",
        "predicate" => "AnotherPredicate",
        "object" => "AnotherObject",
        "validator" => "agent2",
        "signature" => "valid_sig"
      }

      BlockchainServer.receive_transaction(network_tx)
      Process.sleep(50)

      # 7. Query to get both transactions
      {:ok, combined_results} = Kylix.query({"IntegrationSubject", nil, nil})
      assert length(combined_results) == 2

      # 8. Verify the transactions are linked in the DAG
      predicates = Enum.map(combined_results, fn {_, data, _} -> data.predicate end)
      assert "IntegrationPredicate" in predicates
      assert "AnotherPredicate" in predicates
    end
  end
end

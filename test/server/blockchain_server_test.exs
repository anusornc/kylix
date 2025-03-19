defmodule Kylix.BlockchainServerTest do
  use ExUnit.Case
  alias Kylix.BlockchainServer

  setup do
    # Reset application state for each test
    :ok = Application.stop(:kylix)
    {:ok, _} = Application.ensure_all_started(:kylix)

    # Reset transaction count to start fresh
    :ok = BlockchainServer.reset_tx_count(0)
    :ok
  end

  describe "transaction operations" do
    test "add_transaction with valid validator" do
      assert {:ok, tx_id} = BlockchainServer.add_transaction(
               "subject1",
               "predicate1",
               "object1",
               "agent1",
               "valid_sig"
             )

      assert String.starts_with?(tx_id, "tx")
    end

    test "add_transaction with invalid validator" do
      assert {:error, :unknown_validator} = BlockchainServer.add_transaction(
               "subject1",
               "predicate1",
               "object1",
               "unknown_agent",
               "valid_sig"
             )
    end

    test "multiple transactions from different validators" do
      assert {:ok, tx_id1} = BlockchainServer.add_transaction(
               "subject1",
               "predicate1",
               "object1",
               "agent1",
               "valid_sig"
             )

      assert {:ok, tx_id2} = BlockchainServer.add_transaction(
               "subject2",
               "predicate1",
               "object2",
               "agent2",
               "valid_sig"
             )

      assert tx_id1 != tx_id2
    end
  end

  describe "query operations" do
    test "query with exact match" do
      # Add transaction
      {:ok, _} = BlockchainServer.add_transaction(
        "Alice", "knows", "Bob", "agent1", "valid_sig"
      )

      # Query for exact match
      {:ok, results} = BlockchainServer.query({"Alice", "knows", "Bob"})

      assert length(results) == 1
      {_node_id, data, _edges} = hd(results)
      assert data.subject == "Alice"
      assert data.predicate == "knows"
      assert data.object == "Bob"
    end

    test "query with wildcard" do
      # Add multiple transactions
      {:ok, _} = BlockchainServer.add_transaction(
        "Alice", "knows", "Bob", "agent1", "valid_sig"
      )

      {:ok, _} = BlockchainServer.add_transaction(
        "Alice", "likes", "Coffee", "agent2", "valid_sig"
      )

      # Query with subject="Alice", predicate=nil (wildcard)
      {:ok, results} = BlockchainServer.query({"Alice", nil, nil})

      assert length(results) == 2

      # Check that we have both predicates
      predicates = Enum.map(results, fn {_, data, _} -> data.predicate end)
      assert "knows" in predicates
      assert "likes" in predicates
    end

    test "query with no results" do
      {:ok, results} = BlockchainServer.query({"NonExistent", "predicate", "object"})
      assert results == []
    end
  end

  describe "validator operations" do
    test "get_validators returns list of validators" do
      validators = BlockchainServer.get_validators()
      assert is_list(validators)
      assert "agent1" in validators
      assert "agent2" in validators
    end

    test "add_validator with valid existing validator" do
      assert {:ok, "new_agent"} = BlockchainServer.add_validator("new_agent", "pubkey123", "agent1")

      # Check that the new validator is in the list
      validators = BlockchainServer.get_validators()
      assert "new_agent" in validators
    end

    test "add_validator with unknown validator" do
      assert {:error, :unknown_validator} =
        BlockchainServer.add_validator("new_agent", "pubkey123", "unknown_agent")
    end
  end

  describe "network operations" do
    test "receive_transaction processes valid transaction data" do
      # This is typically called by the ValidatorNetwork when it receives a transaction from the network
      tx_data = %{
        "subject" => "NetworkSubject",
        "predicate" => "NetworkPredicate",
        "object" => "NetworkObject",
        "validator" => "agent1",
        "signature" => "valid_sig"
      }

      # Send the transaction data to the BlockchainServer
      GenServer.cast(BlockchainServer, {:receive_transaction, tx_data})

      # Small delay to allow processing
      Process.sleep(50)

      # Query to see if the transaction was added
      {:ok, results} = BlockchainServer.query({"NetworkSubject", "NetworkPredicate", "NetworkObject"})
      assert length(results) == 1
    end
  end

  describe "transaction ordering" do
    test "transactions are linked in sequence" do
      # Add transactions
      {:ok, tx_id1} = BlockchainServer.add_transaction(
        "First", "predicate", "Object", "agent1", "valid_sig"
      )

      {:ok, tx_id2} = BlockchainServer.add_transaction(
        "Second", "predicate", "Object", "agent2", "valid_sig"
      )

      # In test mode, we're using DAGEngine, so we can directly check it
      {:ok, results} = Kylix.Storage.DAGEngine.query({nil, nil, nil})

      # Find the first transaction and check that it has an edge to the second one
      tx1 = Enum.find(results, fn {id, _, _} -> id == tx_id1 end)
      assert tx1 != nil

      {_, _, edges} = tx1
      assert Enum.any?(edges, fn edge ->
        case edge do
          {^tx_id1, ^tx_id2, "confirms"} -> true  # If edges are {from, to, label}
          {^tx_id2, "confirms"} -> true          # If edges are {to, label}
          _ -> false
        end
      end)
    end
  end
end

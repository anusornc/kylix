defmodule Kylix.BlockchainServerTest do
  use ExUnit.Case
  alias Kylix.BlockchainServer
  import Kylix.Auth.SignatureVerifier

  setup do
    # Reset transaction count to start fresh
    :ok = BlockchainServer.reset_tx_count(0)
    {:ok, %{private_key: private_key, public_key: public_key}} = get_test_key_pair()
    {:ok, private_key: private_key, public_key: public_key}
  end

  defp get_test_key_pair() do
    GenServer.call(Kylix.BlockchainServer, :get_test_key_pair)
  end

  describe "transaction operations" do
    test "add_transaction with valid validator", %{private_key: private_key} do
      timestamp = DateTime.utc_now()
      tx_hash = hash_transaction("subject1", "predicate1", "object1", "agent1", timestamp)
      signature = sign(tx_hash, private_key)

      assert {:ok, tx_id} =
               BlockchainServer.add_transaction(
                 "subject1",
                 "predicate1",
                 "object1",
                 "agent1",
                 signature
               )

      assert String.starts_with?(tx_id, "tx")
    end

    test "add_transaction with invalid validator", %{private_key: private_key} do
      timestamp = DateTime.utc_now()
      tx_hash = hash_transaction("subject1", "predicate1", "object1", "unknown_agent", timestamp)
      signature = sign(tx_hash, private_key)

      assert {:error, :unknown_validator} =
               BlockchainServer.add_transaction(
                 "subject1",
                 "predicate1",
                 "object1",
                 "unknown_agent",
                 signature
               )
    end

    test "add_transaction with invalid signature", %{private_key: private_key} do
      timestamp = DateTime.utc_now()
      tx_hash = hash_transaction("subject1", "predicate1", "object1", "agent1", timestamp)
      signature = sign(tx_hash, private_key)
      incorrect_signature = String.replace(signature, 5, 1, "X")

      assert {:error, :invalid_signature} =
               BlockchainServer.add_transaction(
                 "subject1",
                 "predicate1",
                 "object1",
                 "agent1",
                 incorrect_signature
               )
    end

    test "multiple transactions from different validators",
         %{private_key: private_key} do
      timestamp1 = DateTime.utc_now()
      tx_hash1 = hash_transaction("subject1", "predicate1", "object1", "agent1", timestamp1)
      signature1 = sign(tx_hash1, private_key)

      assert {:ok, tx_id1} =
               BlockchainServer.add_transaction(
                 "subject1",
                 "predicate1",
                 "object1",
                 "agent1",
                 signature1
               )

      timestamp2 = DateTime.utc_now()
      tx_hash2 = hash_transaction("subject2", "predicate1", "object2", "agent2", timestamp2)
      signature2 = sign(tx_hash2, private_key)

      assert {:ok, tx_id2} =
               BlockchainServer.add_transaction(
                 "subject2",
                 "predicate1",
                 "object2",
                 "agent2",
                 signature2
               )

      assert tx_id1 != tx_id2
    end
  end

  describe "query operations" do
    test "query with exact match", %{private_key: private_key} do
      # Add transaction
      timestamp = DateTime.utc_now()
      tx_hash = hash_transaction("Alice", "knows", "Bob", "agent1", timestamp)
      signature = sign(tx_hash, private_key)

      {:ok, _} =
        BlockchainServer.add_transaction(
          "Alice",
          "knows",
          "Bob",
          "agent1",
          signature
        )

      # Query for exact match
      {:ok, results} = BlockchainServer.query({"Alice", "knows", "Bob"})

      assert length(results) == 1
      {_node_id, data, _edges} = hd(results)
      assert data.subject == "Alice"
      assert data.predicate == "knows"
      assert data.object == "Bob"
    end

    test "query with wildcard", %{private_key: private_key} do
      # Add multiple transactions
      timestamp1 = DateTime.utc_now()
      tx_hash1 = hash_transaction("Alice", "knows", "Bob", "agent1", timestamp1)
      signature1 = sign(tx_hash1, private_key)

      {:ok, _} =
        BlockchainServer.add_transaction(
          "Alice",
          "knows",
          "Bob",
          "agent1",
          signature1
        )

      timestamp2 = DateTime.utc_now()
      tx_hash2 = hash_transaction("Alice", "likes", "Coffee", "agent2", timestamp2)
      signature2 = sign(tx_hash2, private_key)

      {:ok, _} =
        BlockchainServer.add_transaction(
          "Alice",
          "likes",
          "Coffee",
          "agent2",
          signature2
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
      assert {:ok, "new_agent"} =
               BlockchainServer.add_validator("new_agent", "pubkey123", "agent1")

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
    test "receive_transaction processes valid transaction data",
         %{private_key: private_key} do
      # This is typically called by the ValidatorNetwork when it receives a transaction from the network
      timestamp = DateTime.utc_now()
      tx_hash = hash_transaction("NetworkSubject", "NetworkPredicate", "NetworkObject", "agent1", timestamp)
      signature = sign(tx_hash, private_key)

      tx_data = %{
        "subject" => "NetworkSubject",
        "predicate" => "NetworkPredicate",
        "object" => "NetworkObject",
        "validator" => "agent1",
        "signature" => signature
      }

      # Send the transaction data to the BlockchainServer
      GenServer.cast(Kylix.BlockchainServer, {:receive_transaction, tx_data})

      # Small delay to allow processing
      Process.sleep(50)

      # Query to see if the transaction was added
      {:ok, results} =
        BlockchainServer.query({"NetworkSubject", "NetworkPredicate", "NetworkObject"})

      assert length(results) == 1
    end
  end

  describe "transaction ordering" do
    test "transactions are linked in sequence", %{private_key: private_key} do
      # Add transactions
      timestamp1 = DateTime.utc_now()
      tx_hash1 = hash_transaction("First", "predicate", "Object", "agent1", timestamp1)
      signature1 = sign(tx_hash1, private_key)

      {:ok, tx_id1} =
        BlockchainServer.add_transaction(
          "First",
          "predicate",
          "Object",
          "agent1",
          signature1
        )

      timestamp2 = DateTime.utc_now()
      tx_hash2 = hash_transaction("Second", "predicate", "Object", "agent2", timestamp2)
      signature2 = sign(tx_hash2, private_key)

      {:ok, tx_id2} =
        BlockchainServer.add_transaction(
          "Second",
          "predicate",
          "Object",
          "agent2",
          signature2
        )

      # In test mode, we're using DAGEngine, so we can directly check it
      {:ok, results} = Kylix.Storage.DAGEngine.query({nil, nil, nil})

      # Find the first transaction and check that it has an edge to the second one
      tx1 = Enum.find(results, fn {id, _, _} -> id == tx_id1 end)
      assert tx1 != nil

      {_, _, edges} = tx1
      assert Enum.any?(edges, fn edge ->
               case edge do
                 {^tx_id1, ^tx_id2, "confirms"} -> true # If edges are {from, to, label}
                 {^tx_id2, "confirms"} -> true # If edges are {to, label}
                 _ -> false
               end
             end)
    end
  end
end

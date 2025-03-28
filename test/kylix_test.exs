defmodule KylixTest do
  use ExUnit.Case
  import Kylix.Auth.SignatureVerifier

  setup do
    # Reset the application for proper test isolation
    :ok = Application.stop(:kylix)
    {:ok, _} = Application.ensure_all_started(:kylix)

    # Clear the DAG tables completely (just like in kylix_integration_test)
    Kylix.Storage.DAGEngine.clear_all()

    # Reset transaction count for a clean state
    :ok = Kylix.BlockchainServer.reset_tx_count(0)

    # Get test key pair
    {:ok, %{private_key: private_key, public_key: public_key}} = get_test_key_pair()
    {:ok, private_key: private_key, public_key: public_key}

  end

  describe "add transaction with valid validator and signature" do
    test "add transaction with valid validator and signature", %{private_key: private_key} do
      # Create a unique subject to avoid duplicate transaction errors
      unique_subject = "subject-#{System.monotonic_time()}"

      timestamp = DateTime.utc_now()
      tx_hash = hash_transaction(unique_subject, "predicate", "object", "agent1", timestamp)
      signature = sign(tx_hash, private_key)

      assert {:ok, tx_id} = Kylix.add_transaction(unique_subject, "predicate", "object", "agent1", signature)
      assert String.starts_with?(tx_id, "tx")
    end
  end

  describe "add transaction with invalid validator" do
    test "add transaction with invalid validator", %{private_key: private_key} do
      timestamp = DateTime.utc_now()
      tx_hash = hash_transaction("subject", "predicate", "object", "unknown_agent", timestamp)
      signature = sign(tx_hash, private_key)
      assert {:error, :unknown_validator} = Kylix.add_transaction("subject", "predicate", "object", "unknown_agent", signature)
    end
  end

  describe "query transactions" do
    test "query transactions", %{private_key: private_key} do
      timestamp = DateTime.utc_now()
      tx_hash = hash_transaction("subject1", "predicate1", "object1", "agent1", timestamp)
      signature = sign(tx_hash, private_key)
      {:ok, _tx_id} = Kylix.add_transaction("subject1", "predicate1", "object1", "agent1", signature)
      {:ok, _tx_id} = Kylix.add_transaction("subject2", "predicate2", "object2", "agent2", signature)
      {:ok, results} = Kylix.query({"subject1", "predicate1", "object1"})
      assert length(results) == 1
    end
  end

  describe "query transactions with validator rotation" do
    test "query transactions with validator rotation", %{private_key: private_key} do
      timestamp = DateTime.utc_now()
      tx_hash = hash_transaction("subject1", "predicate1", "object1", "agent1", timestamp)
      signature = sign(tx_hash, private_key)
      {:ok, _tx_id} = Kylix.add_transaction("subject1", "predicate1", "object1", "agent1", signature)
      tx_hash = hash_transaction("subject2", "predicate2", "object2", "agent2", timestamp)
      signature = sign(tx_hash, private_key)
      {:ok, _tx_id} = Kylix.add_transaction("subject2", "predicate2", "object2", "agent2", signature)
      {:ok, results} = Kylix.query({nil, nil, nil})
      assert length(results) == 2
    end
  end

  defp get_test_key_pair() do
    GenServer.call(Kylix.BlockchainServer, :get_test_key_pair)
  end
end

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

  describe "validator management" do
    test "get_current_validator returns a validator ID" do
      validator = Kylix.get_current_validator()
      assert is_binary(validator)
      assert validator in ["agent1", "agent2"]
    end

    test "get_validator_metrics returns a map of metrics" do
      metrics = Kylix.get_validator_metrics()
      assert is_map(metrics)

      # Since we're in test env, we should have metrics for agent1 and agent2
      assert Map.has_key?(metrics, "agent1")
      assert Map.has_key?(metrics, "agent2")

      agent1_metrics = metrics["agent1"]
      assert is_map(agent1_metrics)
      assert Map.has_key?(agent1_metrics, :total_transactions)
      assert Map.has_key?(agent1_metrics, :successful_transactions)
    end

    test "get_validator_status returns coordination status" do
      status = Kylix.get_validator_status()
      assert is_map(status)
      assert Map.has_key?(status, :validators)
      assert Map.has_key?(status, :current_validator)
      assert Map.has_key?(status, :total_validators)
      assert is_list(status.validators)
      assert status.total_validators == length(status.validators)
    end
  end

  defp get_test_key_pair() do
    GenServer.call(Kylix.BlockchainServer, :get_test_key_pair)
  end
end

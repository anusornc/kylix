defmodule KylixTest do
  use ExUnit.Case
  import Kylix.Auth.SignatureVerifier

  setup do
    :ok = Application.stop(:kylix)
    {:ok, _} = Application.ensure_all_started(:kylix)

    Kylix.Storage.DAGEngine.clear_all()
    :ok = Kylix.BlockchainServer.reset_tx_count(0)

    {public_key, private_key} = Kylix.Test.Attester.generate_keys()
    attester = Kylix.Test.Attester.seed("attester1", public_key)
    attester2 = Kylix.Test.Attester.seed("attester2", public_key)

    {:ok,
     private_key: private_key, public_key: public_key, attester: attester, attester2: attester2}
  end

  describe "signed Transaction accept" do
    test "accepts a correctly signed Transaction from a member Validator", %{
      public_key: public_key,
      private_key: private_key
    } do
      Kylix.Test.Attester.seed("member_attester", public_key)

      subject = "subject-member"
      predicate = "predicate"
      object = "object"
      tx_hash = hash_transaction(subject, predicate, object, "member_attester")
      signature = sign(tx_hash, private_key)

      assert {:ok, tx_id} =
               Kylix.add_transaction(subject, predicate, object, "member_attester", signature)

      assert String.starts_with?(tx_id, "tx")
    end

    test "signature still verifies after accept-time moves", %{
      public_key: public_key,
      private_key: private_key
    } do
      Kylix.Test.Attester.seed("clock_attester", public_key)

      subject = "subject-clock"
      predicate = "predicate"
      object = "object"
      tx_hash = hash_transaction(subject, predicate, object, "clock_attester")
      signature = sign(tx_hash, private_key)

      Process.sleep(10)

      assert {:ok, _tx_id} =
               Kylix.add_transaction(subject, predicate, object, "clock_attester", signature)
    end

    test "rejects a Transaction whose signature does not match the statement", %{
      public_key: public_key,
      private_key: private_key
    } do
      Kylix.Test.Attester.seed("bad_sig_attester", public_key)

      subject = "subject-good"
      predicate = "predicate"
      object = "object"
      other_hash = hash_transaction("subject-other", predicate, object, "bad_sig_attester")
      bad_signature = sign(other_hash, private_key)

      assert {:error, :invalid_signature} =
               Kylix.add_transaction(
                 subject,
                 predicate,
                 object,
                 "bad_sig_attester",
                 bad_signature
               )
    end
  end

  describe "add transaction with valid validator and signature" do
    test "add transaction with valid validator and signature", %{
      private_key: private_key,
      attester: attester
    } do
      unique_subject = "subject-#{System.monotonic_time()}"
      tx_hash = hash_transaction(unique_subject, "predicate", "object", attester)
      signature = sign(tx_hash, private_key)

      assert {:ok, tx_id} =
               Kylix.add_transaction(unique_subject, "predicate", "object", attester, signature)

      assert String.starts_with?(tx_id, "tx")
    end
  end

  describe "add transaction asynchronously" do
    test "add transaction asynchronously", %{private_key: private_key, attester: attester} do
      unique_subject = "subject-async-#{System.monotonic_time()}"
      tx_hash = hash_transaction(unique_subject, "predicate", "object", attester)
      signature = sign(tx_hash, private_key)

      assert {:ok, ref} =
               Kylix.add_transaction_async(
                 unique_subject,
                 "predicate",
                 "object",
                 attester,
                 signature
               )

      assert is_reference(ref)
    end
  end

  describe "add transaction with invalid validator" do
    test "add transaction with invalid validator", %{private_key: private_key} do
      tx_hash = hash_transaction("subject", "predicate", "object", "unknown_agent")
      signature = sign(tx_hash, private_key)

      assert {:error, :unknown_validator} =
               Kylix.add_transaction("subject", "predicate", "object", "unknown_agent", signature)
    end
  end

  describe "query transactions" do
    test "query transactions", %{
      private_key: private_key,
      attester: attester,
      attester2: attester2
    } do
      tx_hash = hash_transaction("subject1", "predicate1", "object1", attester)
      signature = sign(tx_hash, private_key)

      {:ok, _tx_id} =
        Kylix.add_transaction("subject1", "predicate1", "object1", attester, signature)

      tx_hash = hash_transaction("subject2", "predicate2", "object2", attester2)
      signature = sign(tx_hash, private_key)

      {:ok, _tx_id} =
        Kylix.add_transaction("subject2", "predicate2", "object2", attester2, signature)

      {:ok, results} = Kylix.query({"subject1", "predicate1", "object1"})
      assert length(results) == 1
    end
  end

  describe "get queue status" do
    test "returns expected map keys" do
      status = Kylix.get_queue_status()
      assert is_map(status)
      assert Map.has_key?(status, :queue_length)
      assert Map.has_key?(status, :processing)
      assert Map.has_key?(status, :validators)
      assert Map.has_key?(status, :current_validator)
      assert Map.has_key?(status, :batch_size)
      assert Map.has_key?(status, :processing_interval)
      assert Map.has_key?(status, :stats)
      assert Map.has_key?(status, :transaction_count)
      assert Map.has_key?(status, :pending_count)
      assert Map.has_key?(status, :completed_count)
    end
  end

  describe "query transactions with validator rotation" do
    test "query transactions with validator rotation", %{
      private_key: private_key,
      attester: attester,
      attester2: attester2
    } do
      tx_hash = hash_transaction("subject1", "predicate1", "object1", attester)
      signature = sign(tx_hash, private_key)

      {:ok, _tx_id} =
        Kylix.add_transaction("subject1", "predicate1", "object1", attester, signature)

      tx_hash = hash_transaction("subject2", "predicate2", "object2", attester2)
      signature = sign(tx_hash, private_key)

      {:ok, _tx_id} =
        Kylix.add_transaction("subject2", "predicate2", "object2", attester2, signature)

      {:ok, results} = Kylix.query({nil, nil, nil})
      assert length(results) == 2
    end
  end

  describe "validator management functions" do
    test "get_current_validator returns a validator string" do
      validator = Kylix.get_current_validator()
      assert is_binary(validator)
      assert validator in Kylix.get_validators()
    end

    test "get_validator_metrics returns metrics for all validators" do
      metrics = Kylix.get_validator_metrics()
      assert is_map(metrics)
      validators = Kylix.get_validators()

      for validator <- validators do
        assert Map.has_key?(metrics, validator)
        assert is_map(metrics[validator])
      end
    end

    test "get_validator_status returns status information map" do
      status = Kylix.get_validator_status()
      assert is_map(status)
      assert Map.has_key?(status, :validators)
      assert Map.has_key?(status, :current_validator)
      assert Map.has_key?(status, :performance_metrics)
      assert is_list(status.validators)
    end
  end
end

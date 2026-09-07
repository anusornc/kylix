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

    test "async submit preserves the caller's attester through accept" do
      {public_key, private_key} = Kylix.Test.Attester.generate_keys()
      member = Kylix.Test.Attester.seed("async_attester", public_key)

      subject = "subject-async-attester"
      predicate = "predicate"
      object = "object"
      tx_hash = hash_transaction(subject, predicate, object, member)
      signature = sign(tx_hash, private_key)

      assert {:ok, ref} =
               Kylix.add_transaction_async(subject, predicate, object, member, signature)

      assert is_reference(ref)

      results = wait_for_query({subject, predicate, object})
      assert length(results) == 1
      {_id, data, _edges} = hd(results)
      assert data.validator == member
    end

    test "two async submits from the same member both accept as that member" do
      {public_key, private_key} = Kylix.Test.Attester.generate_keys()
      member = Kylix.Test.Attester.seed("async_repeat_attester", public_key)

      first_subject = "subject-async-repeat-1"
      second_subject = "subject-async-repeat-2"
      predicate = "predicate"
      first_object = "object-a"
      second_object = "object-b"

      first_hash = hash_transaction(first_subject, predicate, first_object, member)
      second_hash = hash_transaction(second_subject, predicate, second_object, member)

      assert {:ok, _ref1} =
               Kylix.add_transaction_async(
                 first_subject,
                 predicate,
                 first_object,
                 member,
                 sign(first_hash, private_key)
               )

      assert {:ok, _ref2} =
               Kylix.add_transaction_async(
                 second_subject,
                 predicate,
                 second_object,
                 member,
                 sign(second_hash, private_key)
               )

      [{_id1, data1, _edges1}] = wait_for_query({first_subject, predicate, first_object})
      [{_id2, data2, _edges2}] = wait_for_query({second_subject, predicate, second_object})

      assert data1.validator == member
      assert data2.validator == member
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

  describe "one roster; the named Validator attests" do
    test "after vouching, a correctly signed Transaction from the new Validator is accepted" do
      {public_key, private_key} = Kylix.Test.Attester.generate_keys()
      vouched = Kylix.Test.Attester.seed("vouched_attester", public_key)

      subject = "subject-vouched"
      predicate = "predicate"
      object = "object"
      tx_hash = hash_transaction(subject, predicate, object, vouched)
      signature = sign(tx_hash, private_key)

      assert {:ok, tx_id} =
               Kylix.add_transaction(subject, predicate, object, vouched, signature)

      assert String.starts_with?(tx_id, "tx")
    end

    test "stored attester is the validator_id argument" do
      {public_key, private_key} = Kylix.Test.Attester.generate_keys()
      named = Kylix.Test.Attester.seed("named_attester", public_key)

      subject = "subject-named"
      predicate = "predicate"
      object = "object"
      tx_hash = hash_transaction(subject, predicate, object, named)
      signature = sign(tx_hash, private_key)

      assert {:ok, _tx_id} =
               Kylix.add_transaction(subject, predicate, object, named, signature)

      {:ok, results} = Kylix.query({subject, predicate, object})
      assert length(results) == 1
      {_id, data, _edges} = hd(results)
      assert data.validator == named
    end

    test "a member Validator can attest more than once" do
      {public_key, private_key} = Kylix.Test.Attester.generate_keys()
      member = Kylix.Test.Attester.seed("repeat_attester", public_key)

      first_hash = hash_transaction("subject-repeat-1", "predicate", "object", member)
      first_signature = sign(first_hash, private_key)

      assert {:ok, _tx1} =
               Kylix.add_transaction(
                 "subject-repeat-1",
                 "predicate",
                 "object",
                 member,
                 first_signature
               )

      second_hash = hash_transaction("subject-repeat-2", "predicate", "object", member)
      second_signature = sign(second_hash, private_key)

      assert {:ok, _tx2} =
               Kylix.add_transaction(
                 "subject-repeat-2",
                 "predicate",
                 "object",
                 member,
                 second_signature
               )
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

      for validator <- ["agent1", "agent2"] do
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

  defp wait_for_query(pattern, timeout_ms \\ 2000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_query(pattern, deadline)
  end

  defp do_wait_for_query(pattern, deadline) do
    case Kylix.query(pattern) do
      {:ok, [_ | _] = results} ->
        results

      _ ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("Transaction was not accepted before timeout: #{inspect(pattern)}")
        else
          Process.sleep(50)
          do_wait_for_query(pattern, deadline)
        end
    end
  end
end

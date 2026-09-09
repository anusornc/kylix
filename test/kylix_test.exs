defmodule KylixTest do
  use ExUnit.Case
  import Kylix.Auth.SignatureVerifier

  setup do
    Kylix.Test.App.restart()

    {public_key, private_key} = Kylix.Test.Attester.generate_keys()
    attester = Kylix.Test.Attester.seed("attester1", public_key)

    {:ok, private_key: private_key, public_key: public_key, attester: attester}
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

    test "async submit is accepted" do
      {public_key, private_key} = Kylix.Test.Attester.generate_keys()
      member = Kylix.Test.Attester.seed("async_attester", public_key)

      subject = "subject-async-attester"
      predicate = "predicate"
      object = "object"
      tx_hash = hash_transaction(subject, predicate, object, member)
      signature = sign(tx_hash, private_key)

      assert {:ok, ref} =
               Kylix.add_transaction_async(subject, predicate, object, member, signature)

      assert {:ok, tx_id} = wait_for_accept(ref)
      assert String.starts_with?(tx_id, "tx")
    end

    test "two async submits from the same member both accept" do
      {public_key, private_key} = Kylix.Test.Attester.generate_keys()
      member = Kylix.Test.Attester.seed("async_repeat_attester", public_key)

      first_subject = "subject-async-repeat-1"
      second_subject = "subject-async-repeat-2"
      predicate = "predicate"
      first_object = "object-a"
      second_object = "object-b"

      first_hash = hash_transaction(first_subject, predicate, first_object, member)
      second_hash = hash_transaction(second_subject, predicate, second_object, member)

      assert {:ok, ref1} =
               Kylix.add_transaction_async(
                 first_subject,
                 predicate,
                 first_object,
                 member,
                 sign(first_hash, private_key)
               )

      assert {:ok, ref2} =
               Kylix.add_transaction_async(
                 second_subject,
                 predicate,
                 second_object,
                 member,
                 sign(second_hash, private_key)
               )

      assert {:ok, tx1} = wait_for_accept(ref1)
      assert {:ok, tx2} = wait_for_accept(ref2)
      assert String.starts_with?(tx1, "tx")
      assert String.starts_with?(tx2, "tx")
      assert tx1 != tx2
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

    test "get_validators lists the Validator add_validator wrote" do
      {public_key, private_key} = Kylix.Test.Attester.generate_keys()
      vouched = Kylix.Test.Attester.seed("listed_attester", public_key)

      validators = Kylix.get_validators()
      refute validators == []
      assert vouched in validators

      subject = "subject-listed"
      predicate = "predicate"
      object = "object"
      tx_hash = hash_transaction(subject, predicate, object, vouched)
      signature = sign(tx_hash, private_key)

      assert {:ok, _tx_id} =
               Kylix.add_transaction(subject, predicate, object, vouched, signature)
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

  defp wait_for_accept(ref, timeout_ms \\ 2000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_accept(ref, deadline)
  end

  defp do_wait_for_accept(ref, deadline) do
    case Kylix.Server.TransactionQueue.get_transaction_status(ref) do
      %{result: {:ok, tx_id}} ->
        {:ok, tx_id}

      %{result: {:error, reason}} ->
        {:error, reason}

      _ ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("Transaction was not accepted before timeout")
        else
          Process.sleep(50)
          do_wait_for_accept(ref, deadline)
        end
    end
  end
end

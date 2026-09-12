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

      assert is_binary(tx_id)
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

      assert is_binary(tx_id)
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

      assert is_binary(tx_id)
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
end

defmodule Kylix.Security.AttackResistanceTest do
  use ExUnit.Case
  import Kylix.Auth.SignatureVerifier

  setup do
    Kylix.Test.App.restart()

    server = Process.whereis(Kylix.BlockchainServer)

    if !server do
      {:ok, _server} = start_supervised(Kylix.BlockchainServer)
    end

    {public_key, private_key} = Kylix.Test.Attester.generate_keys()
    attester = Kylix.Test.Attester.seed("attester1", public_key)
    attester2 = Kylix.Test.Attester.seed("attester2", public_key)

    {:ok,
     %{
       private_key: private_key,
       public_key: public_key,
       attester: attester,
       attester2: attester2,
       server: server
     }}
  end

  describe "signature verification attacks" do
    test "rejects transaction with invalid signature", %{
      private_key: private_key,
      attester: attester
    } do
      other_hash = hash_transaction("other_subject", "predicate", "object", attester)
      bad_signature = sign(other_hash, private_key)

      result =
        Kylix.add_transaction(
          "subject",
          "predicate",
          "object",
          attester,
          bad_signature
        )

      assert {:error, :invalid_signature} = result
    end

    test "rejects transaction with empty signature", %{attester: attester} do
      result =
        Kylix.add_transaction(
          "subject",
          "predicate",
          "object",
          attester,
          ""
        )

      assert {:error, :invalid_signature} = result
    end

    test "rejects transaction with altered signature", %{
      private_key: private_key,
      attester: attester
    } do
      tx_hash =
        hash_transaction(
          "original_subject",
          "original_predicate",
          "original_object",
          attester
        )

      signature = sign(tx_hash, private_key)

      {:ok, _tx_id} =
        Kylix.add_transaction(
          "original_subject",
          "original_predicate",
          "original_object",
          attester,
          signature
        )

      result =
        Kylix.add_transaction(
          "altered_subject",
          "original_predicate",
          "original_object",
          attester,
          signature
        )

      assert {:error, :invalid_signature} = result
    end
  end

  describe "transaction replay attacks" do
    test "prevents exact transaction replay", %{private_key: private_key, attester: attester} do
      unique_subject = "subject-#{System.monotonic_time()}"
      tx_hash = hash_transaction(unique_subject, "predicate", "object", attester)
      signature = sign(tx_hash, private_key)

      {:ok, _} =
        Kylix.add_transaction(
          unique_subject,
          "predicate",
          "object",
          attester,
          signature
        )

      result =
        Kylix.add_transaction(
          unique_subject,
          "predicate",
          "object",
          attester,
          signature
        )

      # Should reject as duplicate or with verification error
      assert {:error, :duplicate_transaction} = result
    end

    test "allows different RDF triples with the same subject", %{
      private_key: private_key,
      attester: attester,
      attester2: attester2
    } do
      tx_hash1 =
        hash_transaction(
          "entity:document1",
          "prov:wasGeneratedBy",
          "activity:process1",
          attester
        )

      signature1 = sign(tx_hash1, private_key)

      {:ok, _} =
        Kylix.add_transaction(
          "entity:document1",
          "prov:wasGeneratedBy",
          "activity:process1",
          attester,
          signature1
        )

      tx_hash2 =
        hash_transaction(
          "entity:document1",
          "prov:wasAttributedTo",
          "agent:user1",
          attester2
        )

      signature2 = sign(tx_hash2, private_key)

      result =
        Kylix.add_transaction(
          "entity:document1",
          "prov:wasAttributedTo",
          "agent:user1",
          attester2,
          signature2
        )

      # Should allow this as it's a different RDF triple
      assert {:ok, _} = result
    end
  end

  describe "validator impersonation attacks" do
    test "rejects transaction from impersonated validator", %{
      private_key: private_key,
      attester: attester
    } do
      tx_hash = hash_transaction("subject", "predicate", "object", attester)
      signature = sign(tx_hash, private_key)

      modified_signature =
        binary_part(signature, 0, 5) <>
          <<88>> <> binary_part(signature, 6, byte_size(signature) - 6)

      result =
        Kylix.add_transaction(
          "subject",
          "predicate",
          "object",
          attester,
          modified_signature
        )

      assert {:error, :invalid_signature} = result
    end

    test "prevents unauthorized validator addition" do
      # Attempt to add a new validator using an unknown validator as reference
      result =
        Kylix.add_validator(
          "malicious_validator",
          "malicious_pubkey",
          # Not in the trusted validator set
          "unknown_validator"
        )

      # Should reject
      assert {:error, :unknown_validator} = result
    end
  end

  describe "RDF structural attacks" do
    test "rejects invalid RDF structures", %{private_key: private_key, attester: attester} do
      tx_hash = hash_transaction("", "predicate", "object", attester)
      signature = sign(tx_hash, private_key)

      result1 =
        Kylix.add_transaction(
          "",
          "predicate",
          "object",
          attester,
          signature
        )

      assert {:error, :invalid_subject} = result1

      tx_hash = hash_transaction("subject", "", "object", attester)
      signature = sign(tx_hash, private_key)

      result2 =
        Kylix.add_transaction(
          "subject",
          "",
          "object",
          attester,
          signature
        )

      assert {:error, :invalid_predicate} = result2

      tx_hash = hash_transaction("subject", "predicate", "", attester)
      signature = sign(tx_hash, private_key)

      result3 =
        Kylix.add_transaction(
          "subject",
          "predicate",
          "",
          attester,
          signature
        )

      assert {:error, :invalid_object} = result3
    end

    test "validates PROV-O relationships", %{private_key: private_key, attester: attester} do
      tx_hash =
        hash_transaction(
          "activity:process1",
          "prov:wasGeneratedBy",
          "entity:document1",
          attester
        )

      signature = sign(tx_hash, private_key)

      result =
        Kylix.add_transaction(
          "activity:process1",
          "prov:wasGeneratedBy",
          "entity:document1",
          attester,
          signature
        )

      # Should reject due to invalid PROV-O relationship
      assert {:error, :invalid_provenance_relationship} = result
    end
  end

  describe "malformed transaction attacks" do
    test "handles malformed subject gracefully", %{private_key: private_key, attester: attester} do
      tx_hash =
        hash_transaction(
          "<script>alert('xss')</script>",
          "predicate",
          "object",
          attester
        )

      signature = sign(tx_hash, private_key)

      result =
        Kylix.add_transaction(
          "<script>alert('xss')</script>",
          "predicate",
          "object",
          attester,
          signature
        )

      # The system should either sanitize the input or accept it safely
      case result do
        {:error, :invalid_subject} ->
          # If you have specific validation for malicious content
          assert true

        {:error, :invalid_signature} ->
          assert true

        {:ok, tx_id} ->
          assert String.starts_with?(tx_id, "tx")
      end
    end

    test "rejects oversized transaction data", %{private_key: private_key, attester: attester} do
      large_subject = String.duplicate("A", 1_100_000)
      tx_hash = hash_transaction(large_subject, "predicate", "object", attester)
      signature = sign(tx_hash, private_key)

      result =
        Kylix.add_transaction(
          large_subject,
          "predicate",
          "object",
          attester,
          signature
        )

      # Should reject due to size limits
      assert {:error, :data_too_large} = result
    end
  end
end

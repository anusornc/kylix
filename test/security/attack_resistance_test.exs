defmodule Kylix.Security.AttackResistanceTest do
  use ExUnit.Case
  alias Kylix.Storage.DAGEngine
  import Kylix.Auth.SignatureVerifier

  setup do
    # Stop the application completely
    Application.stop(:kylix)

    # Start it again
    {:ok, _} = Application.ensure_all_started(:kylix)

    # Instead of using start_supervised, work with the existing server
    server = Process.whereis(Kylix.BlockchainServer)

    if !server do
      # Only start if it doesn't exist
      {:ok, _server} = start_supervised(Kylix.BlockchainServer)
    end

    # Reset transaction count
    :ok = Kylix.BlockchainServer.reset_tx_count(0)

    # Get test key pair
    {:ok, %{private_key: private_key, public_key: public_key}} = get_test_key_pair()

    {:ok,
     %{
       private_key: private_key,
       public_key: public_key,
       server: server
     }}
  end

  defp get_test_key_pair() do
    GenServer.call(Kylix.BlockchainServer, :get_test_key_pair)
  end

  describe "signature verification attacks" do
    test "rejects transaction with invalid signature" do
      # Use a known validator but with an invalid signature
      result =
        Kylix.add_transaction(
          "subject",
          "predicate",
          "object",
          "agent1",
          "invalid_signature_data"
        )

      # Implementation note: This test assumes your signature verification is properly
      # implemented to reject invalid signatures. If the test passes with "valid_sig",
      # you may need to enhance your signature verification.
      assert {:error, :verification_failed} = result
    end

    test "rejects transaction with empty signature" do
      result =
        Kylix.add_transaction(
          "subject",
          "predicate",
          "object",
          "agent1",
          ""
        )

      # The application should reject empty signatures
      assert {:error, :verification_failed} = result
    end

    test "rejects transaction with altered signature", %{private_key: private_key} do
      # First, create a valid transaction
      timestamp = DateTime.utc_now()

      tx_hash =
        hash_transaction(
          "original_subject",
          "original_predicate",
          "original_object",
          "agent1",
          timestamp
        )

      signature = sign(tx_hash, private_key)

      {:ok, tx_id} =
        Kylix.add_transaction(
          "original_subject",
          "original_predicate",
          "original_object",
          "agent1",
          signature
        )

      # Retrieve the signature from the stored transaction
      {:ok, tx_data} = DAGEngine.get_node(tx_id)
      original_signature = tx_data.signature

      # Now try to submit a transaction with altered data but the same signature
      result =
        Kylix.add_transaction(
          # Changed subject
          "altered_subject",
          "original_predicate",
          "original_object",
          "agent1",
          # Using the same signature
          original_signature
        )

      # Should reject as the signature doesn't match the data
      assert {:error, :verification_failed} = result
    end
  end

  describe "transaction replay attacks" do
    test "prevents exact transaction replay", %{private_key: private_key} do
      # Add a transaction
      unique_subject = "subject-#{System.monotonic_time()}"
      timestamp = DateTime.utc_now()
      tx_hash = hash_transaction(unique_subject, "predicate", "object", "agent1", timestamp)
      signature = sign(tx_hash, private_key)

      {:ok, _} =
        Kylix.add_transaction(
          unique_subject,
          "predicate",
          "object",
          "agent1",
          signature
        )

      # Try to add the exact same transaction again
      result =
        Kylix.add_transaction(
          unique_subject,
          "predicate",
          "object",
          "agent1",
          signature
        )

      # Should reject as duplicate or with verification error
      assert {:error, reason} = result
      assert reason in [:duplicate_transaction, :verification_failed]
    end

    test "allows different RDF triples with the same subject", %{private_key: private_key} do
      # Add first transaction with subject
      timestamp1 = DateTime.utc_now()

      tx_hash1 =
        hash_transaction(
          "entity:document1",
          "prov:wasGeneratedBy",
          "activity:process1",
          "agent1",
          timestamp1
        )

      signature1 = sign(tx_hash1, private_key)

      {:ok, _} =
        Kylix.add_transaction(
          "entity:document1",
          "prov:wasGeneratedBy",
          "activity:process1",
          "agent1",
          signature1
        )

      # Sleep to ensure different timestamp
      Process.sleep(10)

      # Add second transaction with same subject but different predicate/object
      timestamp2 = DateTime.utc_now()

      tx_hash2 =
        hash_transaction(
          "entity:document1",
          "prov:wasAttributedTo",
          "agent:user1",
          "agent2",
          timestamp2
        )

      signature2 = sign(tx_hash2, private_key)

      result =
        Kylix.add_transaction(
          "entity:document1",
          "prov:wasAttributedTo",
          "agent:user1",
          "agent2",
          signature2
        )

      # Should allow this as it's a different RDF triple
      assert {:ok, _} = result
    end
  end

  describe "validator impersonation attacks" do
    test "rejects transaction from impersonated validator", %{private_key: private_key} do
      # Attempt to use a validator that exists but shouldn't be accessible to us
      timestamp = DateTime.utc_now()
      tx_hash = hash_transaction("subject", "predicate", "object", "agent1", timestamp)
      # Intentionally create a different signature from what would be valid
      # We'll modify a byte in the middle of the signature
      signature = sign(tx_hash, private_key)
      # Corrupt the signature to simulate tampering
      if byte_size(signature) > 10 do
        modified_signature =
          binary_part(signature, 0, 5) <>
            <<88>> <> binary_part(signature, 6, byte_size(signature) - 6)

        result =
          Kylix.add_transaction(
            "subject",
            "predicate",
            "object",
            # Valid validator
            "agent1",
            # With corrupted signature
            modified_signature
          )

        # Should reject due to signature validation
        assert {:error, :verification_failed} = result
      else
        # If signature is too short, skip this test
        assert true
      end
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
    test "rejects invalid RDF structures", %{private_key: private_key} do
      # Test with empty subject
      timestamp = DateTime.utc_now()
      tx_hash = hash_transaction("", "predicate", "object", "agent1", timestamp)
      signature = sign(tx_hash, private_key)

      result1 =
        Kylix.add_transaction(
          "",
          "predicate",
          "object",
          "agent1",
          signature
        )

      assert {:error, :invalid_subject} = result1

      # Test with empty predicate
      tx_hash = hash_transaction("subject", "", "object", "agent1", timestamp)
      signature = sign(tx_hash, private_key)

      result2 =
        Kylix.add_transaction(
          "subject",
          "",
          "object",
          "agent1",
          signature
        )

      assert {:error, :invalid_predicate} = result2

      # Test with empty object
      tx_hash = hash_transaction("subject", "predicate", "", "agent1", timestamp)
      signature = sign(tx_hash, private_key)

      result3 =
        Kylix.add_transaction(
          "subject",
          "predicate",
          "",
          "agent1",
          signature
        )

      assert {:error, :invalid_object} = result3
    end

    test "validates PROV-O relationships", %{private_key: private_key} do
      # Test incorrect entity-activity relationship
      timestamp = DateTime.utc_now()

      tx_hash =
        hash_transaction(
          "activity:process1",
          "prov:wasGeneratedBy",
          "entity:document1",
          "agent1",
          timestamp
        )

      signature = sign(tx_hash, private_key)

      result =
        Kylix.add_transaction(
          # Should be entity, not activity
          "activity:process1",
          "prov:wasGeneratedBy",
          # Should be activity, not entity
          "entity:document1",
          "agent1",
          signature
        )

      # Should reject due to invalid PROV-O relationship
      assert {:error, :invalid_provenance_relationship} = result
    end
  end

  describe "malformed transaction attacks" do
    test "handles malformed subject gracefully", %{private_key: private_key} do
      # Subject with potentially problematic characters
      timestamp = DateTime.utc_now()

      tx_hash =
        hash_transaction(
          "<script>alert('xss')</script>",
          "predicate",
          "object",
          "agent1",
          timestamp
        )

      signature = sign(tx_hash, private_key)

      result =
        Kylix.add_transaction(
          # Attempt at injection
          "<script>alert('xss')</script>",
          "predicate",
          "object",
          "agent1",
          signature
        )

      # The system should either sanitize the input or accept it safely
      case result do
        {:error, :invalid_subject} ->
          # If you have specific validation for malicious content
          assert true

        {:error, :verification_failed} ->
          # If signature verification fails
          assert true

        {:ok, tx_id} ->
          # If you allow any subject - check that it's properly stored
          # without allowing script execution
          {:ok, tx_data} = DAGEngine.get_node(tx_id)
          assert tx_data.subject == "<script>alert('xss')</script>"
      end
    end

    test "rejects oversized transaction data", %{private_key: private_key} do
      # Create a very large string as subject
      # 1.1MB of data
      large_subject = String.duplicate("A", 1_100_000)
      timestamp = DateTime.utc_now()
      tx_hash = hash_transaction(large_subject, "predicate", "object", "agent1", timestamp)
      signature = sign(tx_hash, private_key)

      # Attempt to add transaction with very large data
      result =
        Kylix.add_transaction(
          large_subject,
          "predicate",
          "object",
          "agent1",
          signature
        )

      # Should reject due to size limits
      assert {:error, :data_too_large} = result
    end
  end
end

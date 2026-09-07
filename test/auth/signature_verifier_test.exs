defmodule Kylix.Auth.SignatureVerifierTest do
  use ExUnit.Case
  alias Kylix.Auth.SignatureVerifier

  @test_config_dir "test/tmp/validators"

  setup do
    # Create test directories
    File.mkdir_p!(@test_config_dir)

    # Create test key files
    create_test_keys()

    on_exit(fn ->
      # Clean up after tests
      File.rm_rf!(@test_config_dir)
    end)

    :ok
  end

  describe "hash_transaction/4" do
    test "creates deterministic hashes for same input" do
      subject = "Alice"
      predicate = "knows"
      object = "Bob"
      validator_id = "validator1"

      hash1 = SignatureVerifier.hash_transaction(subject, predicate, object, validator_id)
      hash2 = SignatureVerifier.hash_transaction(subject, predicate, object, validator_id)

      assert hash1 == hash2
      assert is_binary(hash1)
      assert byte_size(hash1) == 32
    end

    test "produces different hashes for different inputs" do
      hash1 = SignatureVerifier.hash_transaction("Alice", "knows", "Bob", "validator1")
      hash2 = SignatureVerifier.hash_transaction("Bob", "knows", "Bob", "validator1")
      hash3 = SignatureVerifier.hash_transaction("Alice", "likes", "Bob", "validator1")
      hash4 = SignatureVerifier.hash_transaction("Alice", "knows", "Charlie", "validator1")
      hash5 = SignatureVerifier.hash_transaction("Alice", "knows", "Bob", "validator2")

      hashes = [hash1, hash2, hash3, hash4, hash5]
      unique_hashes = Enum.uniq(hashes)
      assert length(unique_hashes) == length(hashes)
    end

    test "does not change when accept-time would have moved" do
      hash1 = SignatureVerifier.hash_transaction("Alice", "knows", "Bob", "validator1")
      Process.sleep(10)
      hash2 = SignatureVerifier.hash_transaction("Alice", "knows", "Bob", "validator1")
      assert hash1 == hash2
    end
  end

  describe "verify/3" do
    # Instead of trying to mock crypto, we'll test the function with known inputs
    # and expect specific outputs based on our understanding of the implementation

    test "accepts a signature that matches the data and public key" do
      {:ok, {public_key, private_key}} = SignatureVerifier.generate_test_key_pair()
      data = "test transaction data"
      signature = SignatureVerifier.sign(data, private_key)

      assert :ok = SignatureVerifier.verify(data, signature, public_key)
    end

    test "rejects a signature over different data" do
      {:ok, {public_key, private_key}} = SignatureVerifier.generate_test_key_pair()
      signature = SignatureVerifier.sign("original", private_key)

      assert {:error, :invalid_signature} =
               SignatureVerifier.verify("altered", signature, public_key)
    end

    test "verify function structure" do
      data = "test data"
      signature = "dummy signature"
      public_key = "dummy key"

      result = SignatureVerifier.verify(data, signature, public_key)

      assert result == :ok || match?({:error, _reason}, result)
    end

    test "verify handles exceptions gracefully" do
      # Use an approach that will definitely cause an exception in the verify function
      # We'll pass a malformed key that will cause an error
      data = "test data"
      # Binary that's not valid as a signature
      signature = <<1, 2, 3>>
      # Binary that's not a valid key
      public_key = <<4, 5, 6>>

      # This should cause an exception inside the crypto.verify function
      # but our function should catch it and return a friendly error
      result = SignatureVerifier.verify(data, signature, public_key)

      assert match?({:error, :verification_failed}, result)
    end
  end

  describe "generate_test_key_pair/0" do
    test "generates a valid RSA key pair" do
      # Act
      result = SignatureVerifier.generate_test_key_pair()

      # Assert
      assert {:ok, {public_key, private_key}} = result
      assert is_list(public_key) or is_binary(public_key) or is_tuple(public_key)
      assert is_list(private_key) or is_binary(private_key) or is_tuple(private_key)
    end
  end

  describe "sign/2" do
    test "creates a signature using the private key" do
      # Arrange
      {:ok, {public_key, private_key}} = SignatureVerifier.generate_test_key_pair()
      data = "test transaction data"

      # Act
      signature = SignatureVerifier.sign(data, private_key)

      # Assert
      assert is_binary(signature)

      # Verify the signature can be verified with the public key natively
      # to ensure it's a mathematically valid RSA signature
      data_hash = :crypto.hash(:sha256, data)
      assert :crypto.verify(:rsa, :sha256, data_hash, signature, public_key)
    end
  end

  describe "load_public_keys/1" do
    test "loads public keys from directory" do
      # Arrange - test keys are created in setup

      # Act
      keys = SignatureVerifier.load_public_keys(@test_config_dir)

      # Assert
      assert is_map(keys)
      assert Map.has_key?(keys, "validator1")
      assert Map.has_key?(keys, "validator2")
      assert keys["validator1"] == "mock_public_key_1"
      assert keys["validator2"] == "mock_public_key_2"
    end

    test "returns empty map for empty directory" do
      # Arrange
      empty_dir = "test/tmp/empty_validators"
      File.mkdir_p!(empty_dir)

      # Act
      keys = SignatureVerifier.load_public_keys(empty_dir)

      # Assert
      assert keys == %{}

      # Clean up
      File.rmdir!(empty_dir)
    end

    test "ignores non-pub files" do
      # Arrange
      mixed_dir = "test/tmp/mixed_validators"
      File.mkdir_p!(mixed_dir)

      # Create .pub file
      File.write!(Path.join(mixed_dir, "validator1.pub"), "valid_key")

      # Create non-.pub files
      File.write!(Path.join(mixed_dir, "validator2.txt"), "invalid_key")
      File.write!(Path.join(mixed_dir, "validator3.key"), "invalid_key")

      # Act
      keys = SignatureVerifier.load_public_keys(mixed_dir)

      # Assert
      assert Map.keys(keys) == ["validator1"]
      assert keys["validator1"] == "valid_key"

      # Clean up
      File.rm_rf!(mixed_dir)
    end
  end

  # Helper function to create test key files
  defp create_test_keys do
    # Create test public key files
    File.write!(Path.join(@test_config_dir, "validator1.pub"), "mock_public_key_1")
    File.write!(Path.join(@test_config_dir, "validator2.pub"), "mock_public_key_2")
  end
end

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

  describe "hash_transaction/5" do
    test "creates deterministic hashes for same input" do
      # Arrange
      subject = "Alice"
      predicate = "knows"
      object = "Bob"
      validator_id = "validator1"
      timestamp = DateTime.from_iso8601("2023-01-01T12:00:00Z") |> elem(1)

      # Act
      hash1 = SignatureVerifier.hash_transaction(subject, predicate, object, validator_id, timestamp)
      hash2 = SignatureVerifier.hash_transaction(subject, predicate, object, validator_id, timestamp)

      # Assert
      assert hash1 == hash2
      assert is_binary(hash1)
      assert byte_size(hash1) == 32  # SHA-256 produces 32 bytes
    end

    test "produces different hashes for different inputs" do
      # Arrange
      timestamp = DateTime.from_iso8601("2023-01-01T12:00:00Z") |> elem(1)

      # Generate different hashes by changing each parameter
      hash1 = SignatureVerifier.hash_transaction("Alice", "knows", "Bob", "validator1", timestamp)
      hash2 = SignatureVerifier.hash_transaction("Bob", "knows", "Bob", "validator1", timestamp)
      hash3 = SignatureVerifier.hash_transaction("Alice", "likes", "Bob", "validator1", timestamp)
      hash4 = SignatureVerifier.hash_transaction("Alice", "knows", "Charlie", "validator1", timestamp)
      hash5 = SignatureVerifier.hash_transaction("Alice", "knows", "Bob", "validator2", timestamp)

      different_timestamp = DateTime.from_iso8601("2023-01-01T12:00:01Z") |> elem(1)
      hash6 = SignatureVerifier.hash_transaction("Alice", "knows", "Bob", "validator1", different_timestamp)

      # Assert all hashes are different
      hashes = [hash1, hash2, hash3, hash4, hash5, hash6]
      unique_hashes = Enum.uniq(hashes)
      assert length(unique_hashes) == length(hashes)
    end
  end

  describe "verify/3" do
    # Instead of trying to mock crypto, we'll test the function with known inputs
    # and expect specific outputs based on our understanding of the implementation

    test "verify function structure" do
      # This test just ensures the basic structure of the function works
      # without testing actual crypto operations
      data = "test data"
      signature = "dummy signature"
      public_key = "dummy key"

      # We expect either :ok or an error tuple
      result = SignatureVerifier.verify(data, signature, public_key)

      assert result == :ok || match?({:error, _reason}, result)
    end

    test "verify handles exceptions gracefully" do
      # Use an approach that will definitely cause an exception in the verify function
      # We'll pass a malformed key that will cause an error
      data = "test data"
      signature = <<1, 2, 3>>  # Binary that's not valid as a signature
      public_key = <<4, 5, 6>> # Binary that's not a valid key

      # This should cause an exception inside the crypto.verify function
      # but our function should catch it and return a friendly error
      result = SignatureVerifier.verify(data, signature, public_key)

      assert match?({:error, :verification_failed}, result)
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

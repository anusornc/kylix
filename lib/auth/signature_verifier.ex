defmodule Kylix.Auth.SignatureVerifier do
  @moduledoc """
  Handles cryptographic signature verification for blockchain transactions.
  Uses public key cryptography to verify that transactions are authorized.
  """

  @doc """
  Verifies a digital signature against the provided data and public key.

  ## Parameters

  - data: The data that was signed, typically a transaction hash
  - signature: The signature to verify
  - public_key: The validator's public key

  ## Returns

  - :ok if signature is valid
  - {:error, reason} otherwise
  """
  def verify(data, signature, public_key) do
    # Hash the data first (SHA-256 is commonly used)
    data_hash = :crypto.hash(:sha256, data)

    try do
      # Verify using RSA or ECDSA
      case :crypto.verify(:rsa, :sha256, data_hash, signature, [public_key, :rsa]) do
        true -> :ok
        false -> {:error, :invalid_signature}
      end
    rescue
      _ -> {:error, :verification_failed}
    end
  end

  @doc """
  Creates a transaction hash from transaction data.
  """
  def hash_transaction(subject, predicate, object, validator_id, timestamp) do
    # Concatenate all fields and hash them
    data = "#{subject}|#{predicate}|#{object}|#{validator_id}|#{DateTime.to_iso8601(timestamp)}"
    :crypto.hash(:sha256, data)
  end

  @doc """
  Loads public keys from the configuration directory.
  """
  def load_public_keys(config_dir) do
    config_dir
    |> File.ls!()
    |> Enum.filter(&String.ends_with?(&1, ".pub"))
    |> Enum.map(fn file ->
      validator_id = Path.rootname(file)
      key_data = File.read!(Path.join(config_dir, file))
      {validator_id, key_data}
    end)
    |> Enum.into(%{})
  end

  @doc """
  Generates an RSA key pair for testing.

  ## Returns

  - {:ok, {public_key, private_key}} on success
  - {:error, reason} on failure
  """
  def generate_test_key_pair do
    {public_key, private_key} = :crypto.generate_key(:rsa, {2048, 65537})
    {:ok, {public_key, private_key}}
  end

  @doc """
  Signs data with the provided private key.
  """
  def sign(data, private_key) do
    data_hash = :crypto.hash(:sha256, data)
    :crypto.sign(:rsa, :sha256, data_hash, private_key)
  end
end

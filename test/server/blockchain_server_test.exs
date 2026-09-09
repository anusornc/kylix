defmodule Kylix.BlockchainServerTest do
  use ExUnit.Case
  alias Kylix.BlockchainServer
  import Kylix.Auth.SignatureVerifier

  setup do
    Kylix.Test.App.restart()

    {public_key, private_key} = Kylix.Test.Attester.generate_keys()
    attester = Kylix.Test.Attester.seed("attester1", public_key)
    attester2 = Kylix.Test.Attester.seed("attester2", public_key)

    {:ok,
     private_key: private_key, public_key: public_key, attester: attester, attester2: attester2}
  end

  describe "transaction operations" do
    test "add_transaction with valid validator", %{
      private_key: private_key,
      attester: attester
    } do
      tx_hash = hash_transaction("subject1", "predicate1", "object1", attester)
      signature = sign(tx_hash, private_key)

      assert {:ok, tx_id} =
               BlockchainServer.add_transaction(
                 "subject1",
                 "predicate1",
                 "object1",
                 attester,
                 signature
               )

      assert String.starts_with?(tx_id, "tx")
    end

    test "add_transaction with invalid validator", %{private_key: private_key} do
      tx_hash = hash_transaction("subject1", "predicate1", "object1", "unknown_agent")
      signature = sign(tx_hash, private_key)

      assert {:error, :unknown_validator} =
               BlockchainServer.add_transaction(
                 "subject1",
                 "predicate1",
                 "object1",
                 "unknown_agent",
                 signature
               )
    end

    test "add_transaction with invalid signature", %{
      private_key: private_key,
      attester: attester
    } do
      other_hash = hash_transaction("other_subject", "predicate1", "object1", attester)
      bad_signature = sign(other_hash, private_key)

      assert {:error, :invalid_signature} =
               BlockchainServer.add_transaction(
                 "subject1",
                 "predicate1",
                 "object1",
                 attester,
                 bad_signature
               )
    end

    test "multiple transactions from different validators", %{
      private_key: private_key,
      attester: attester,
      attester2: attester2
    } do
      tx_hash1 = hash_transaction("subject1", "predicate1", "object1", attester)
      signature1 = sign(tx_hash1, private_key)

      assert {:ok, tx_id1} =
               BlockchainServer.add_transaction(
                 "subject1",
                 "predicate1",
                 "object1",
                 attester,
                 signature1
               )

      tx_hash2 = hash_transaction("subject2", "predicate1", "object2", attester2)
      signature2 = sign(tx_hash2, private_key)

      assert {:ok, tx_id2} =
               BlockchainServer.add_transaction(
                 "subject2",
                 "predicate1",
                 "object2",
                 attester2,
                 signature2
               )

      assert tx_id1 != tx_id2
    end
  end

  describe "validator operations" do
    test "get_validators returns list of validators" do
      validators = BlockchainServer.get_validators()
      assert is_list(validators)
      assert "agent1" in validators
      assert "agent2" in validators
    end

    test "add_validator with valid existing validator" do
      on_exit(fn -> File.rm("config/validators/added_agent.pub") end)

      assert {:ok, "added_agent"} =
               BlockchainServer.add_validator("added_agent", "pubkey123", "agent1")

      validators = BlockchainServer.get_validators()
      assert "added_agent" in validators
    end

    test "add_validator with unknown validator" do
      assert {:error, :unknown_validator} =
               BlockchainServer.add_validator("added_agent", "pubkey123", "unknown_agent")
    end
  end
end

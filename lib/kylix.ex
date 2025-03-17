defmodule Kylix do
  @moduledoc """
  Kylix is a blockchain application that uses a Directed Acyclic Graph (DAG) to store transactions.
  It supports validator-based transaction processing and querying.
  """

  # Public API for interacting with the BlockchainServer
  def add_transaction(subject, predicate, object, validator_id, signature) do
    Kylix.BlockchainServer.add_transaction(subject, predicate, object, validator_id, signature)
  end

  def query(pattern) do
    Kylix.BlockchainServer.query(pattern)
  end

  def get_validators do
    Kylix.BlockchainServer.get_validators()
  end

  def add_validator(validator_id, pubkey, known_by) do
    Kylix.BlockchainServer.add_validator(validator_id, pubkey, known_by)
  end
end

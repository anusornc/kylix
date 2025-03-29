defmodule Kylix do
  @moduledoc """
  Kylix is a blockchain application that uses a Directed Acyclic Graph (DAG) to store transactions.
  It supports validator-based transaction processing and querying.
  """

  # Public API for interacting with the BlockchainServer
  def add_transaction(subject, predicate, object, validator_id, signature) do
    Kylix.BlockchainServer.add_transaction(subject, predicate, object, validator_id, signature)
  end

  @doc """
  Asynchronously adds a transaction to the blockchain via the transaction queue.

  The transaction queue handles validator assignment in a round-robin fashion,
  which can significantly improve throughput in benchmarking scenarios.

  ## Parameters

  * `subject` - Subject of the RDF triple
  * `predicate` - Predicate of the RDF triple
  * `object` - Object of the RDF triple
  * `validator_id` - Validator identifier
  * `signature` - Transaction signature

  ## Returns

  * `{:ok, reference}` - Transaction was submitted successfully and reference can be used for tracking
  """
  def add_transaction_async(subject, predicate, object, validator_id, signature) do
    Kylix.Server.TransactionQueue.submit(subject, predicate, object, validator_id, signature)
  end

  @doc """
  Returns the current status of the transaction queue.

  This includes information about queue length, processing rates, and statistics.
  """
  def get_queue_status do
    Kylix.Server.TransactionQueue.status()
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

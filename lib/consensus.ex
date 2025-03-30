defmodule Kylix.Consensus do
  @moduledoc """
  The Consensus module contains components for Kylix's consensus mechanism,
  including validator coordination, performance tracking, and metrics.
  """

  require Logger

  @doc """
  Initializes the consensus system by checking coordinator availability.

  Returns `:ok` if the consensus system is operating normally.
  """
  def init do
    case check_coordinator_running() do
      true ->
        Logger.info("Consensus system initialized")
        :ok
      false ->
        Logger.warning("Consensus system not ready - ValidatorCoordinator not running")
        {:error, :coordinator_not_running}
    end
  end

  @doc """
  Gets the current round of consensus.

  This is determined by the total number of processed transactions.
  """
  def current_round do
    # Get status from blockchain server for transaction count
    if Process.whereis(Kylix.BlockchainServer) do
      try do
        # The transaction count would be equivalent to the round number
        # We'll need to add an API to get this from the BlockchainServer
        0  # Placeholder until proper API exists
      rescue
        _ -> 0
      end
    else
      0
    end
  end

  @doc """
  Gets the current consensus status.

  Returns a map with various statistics about the consensus system.
  """
  def status do
    case check_coordinator_running() do
      true ->
        coordinator_status = Kylix.Consensus.ValidatorCoordinator.status()

        # Combine with additional information
        Map.merge(coordinator_status, %{
          consensus_type: "Provenance-based Authority Consensus (PAC)",
          selection_method: "Round-robin",
          round: current_round(),
          healthy: true
        })

      false ->
        %{
          consensus_type: "Provenance-based Authority Consensus (PAC)",
          selection_method: "Round-robin",
          validators: [],
          current_validator: nil,
          healthy: false,
          error: "ValidatorCoordinator not running"
        }
    end
  end

  @doc """
  Gets performance metrics for all validators.

  Returns a map of validator IDs to their performance metrics.
  """
  def validator_metrics do
    if check_coordinator_running() do
      Kylix.Consensus.ValidatorCoordinator.get_performance_metrics()
    else
      %{}
    end
  end

  @doc """
  Gets the ID of the validator that should process the next transaction.

  This is used by the BlockchainServer to determine which validator should
  be allowed to commit a transaction.
  """
  def get_next_validator do
    if check_coordinator_running() do
      Kylix.Consensus.ValidatorCoordinator.get_current_validator()
    else
      # Fallback to default validator if coordinator is not running
      "agent1"
    end
  end

  @doc """
  Checks if the ValidatorCoordinator is running.

  Returns `true` if the coordinator is running, `false` otherwise.
  """
  def check_coordinator_running do
    case Process.whereis(Kylix.Consensus.ValidatorCoordinator) do
      nil -> false
      pid when is_pid(pid) -> Process.alive?(pid)
      _ -> false
    end
  end

  @doc """
  Records a transaction result for a validator.

  This is used to track validator performance over time.

  ## Parameters

  * `validator_id` - The ID of the validator
  * `success` - Whether the transaction was successful
  * `tx_time` - The time it took to process the transaction (microseconds)
  """
  def record_transaction_result(validator_id, success, tx_time \\ nil) do
    if check_coordinator_running() do
      Kylix.Consensus.ValidatorCoordinator.record_transaction_performance(
        validator_id,
        success,
        tx_time
      )
    end
    :ok
  end
end

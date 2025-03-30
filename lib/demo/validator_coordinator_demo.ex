defmodule Kylix.Demo.ValidatorCoordinatorDemo do
  @moduledoc """
  Demonstration module for testing the ValidatorCoordinator functionality.
  """

  alias Kylix.Consensus.ValidatorCoordinator
  alias Kylix.Auth.SignatureVerifier
  require Logger

  @doc """
  Runs a demonstration of the ValidatorCoordinator's features.
  """
  def run do
    # Set up a temporary config directory
    tmp_dir = "tmp/validator_demo"
    File.mkdir_p!(tmp_dir)

    # Start with initial validators
    initial_validators = ["agent1", "agent2", "agent3"]

    # Ensure the directory has validator public key files
    Enum.each(initial_validators, fn validator ->
      File.write!(Path.join(tmp_dir, "#{validator}.pub"), "demo_key_#{validator}")
    end)

    # Check if coordinator is already running
    coordinator_running = Process.whereis(Kylix.Consensus.ValidatorCoordinator) != nil

    # Use the existing instance or start a new one
    if coordinator_running do
      IO.puts("\nUsing existing ValidatorCoordinator instance")
    else
      # Only start the coordinator if it's not already running
      case ValidatorCoordinator.start_link(validators: initial_validators, config_dir: tmp_dir) do
        {:ok, _pid} ->
          IO.puts("\nStarted new ValidatorCoordinator instance")
        {:error, {:already_started, _pid}} ->
          IO.puts("\nValidatorCoordinator was started by another process")
        error ->
          IO.puts("\nError starting ValidatorCoordinator: #{inspect(error)}")
      end
    end

    # Display initial state
    print_status("Initial State")

    # Simulate transactions
    simulate_transactions(20)

    # Display metrics after initial transactions
    print_status("After 20 Transactions")

    # Add a new validator
    new_validator = "agent4"
    {:ok, pub_key, _priv_key} = generate_key_pair()
    ValidatorCoordinator.add_validator(new_validator, pub_key, "agent1")

    # Display state after adding a validator
    print_status("After Adding New Validator")

    # Simulate more transactions
    simulate_transactions(20)

    # Display metrics after more transactions
    print_status("After 20 More Transactions")

    # Remove a validator
    ValidatorCoordinator.remove_validator("agent2")

    # Display state after removing a validator
    print_status("After Removing a Validator")

    # Cleanup
    cleanup(tmp_dir)

    :ok
  end

  # Helper functions

  defp simulate_transactions(count) do
    IO.puts("\nSimulating #{count} transactions...")

    Enum.each(1..count, fn i ->
      # Get current validator
      validator = ValidatorCoordinator.get_current_validator()

      # Simulate transaction time (50-250 microseconds)
      tx_time = :rand.uniform(200) + 50

      # Simulate success/failure (90% success rate)
      success? = :rand.uniform(100) <= 90

      # Record the performance
      ValidatorCoordinator.record_transaction_performance(validator, success?, tx_time)

      # Print transaction details
      IO.puts("Transaction #{i}: Validator #{validator}, Time: #{tx_time}μs, Success: #{success?}")
    end)
  end

  defp print_status(label) do
    IO.puts("\n-- #{label} --")

    # Get all validators
    validators = ValidatorCoordinator.get_validators()
    IO.puts("Active validators: #{inspect(validators)}")

    # Get current status
    status = ValidatorCoordinator.status()
    IO.puts("Current validator: #{status.current_validator}")

    # Get performance metrics
    metrics = ValidatorCoordinator.get_performance_metrics()

    IO.puts("\nPerformance Metrics:")
    Enum.each(metrics, fn {validator, stats} ->
      IO.puts("#{validator}:")
      IO.puts("  Total: #{stats.total_transactions}")
      IO.puts("  Success: #{stats.successful_transactions}")
      IO.puts("  Failure Rate: #{stats.failure_rate * 100}%")
      IO.puts("  Avg Tx Time: #{if stats.avg_tx_time, do: "#{Float.round(stats.avg_tx_time, 2)}μs", else: "N/A"}")
      IO.puts("  Last Active: #{DateTime.to_string(stats.last_active)}")
    end)
  end

  # defp generate_test_keys(validators, dir) do
  #   Enum.map(validators, fn validator ->
  #     # Generate key pair
  #     {:ok, pub_key, priv_key} = generate_key_pair()

  #     # Save public key
  #     pub_path = Path.join(dir, "#{validator}.pub")
  #     File.write!(pub_path, pub_key)

  #     # Return info
  #     {validator, pub_key, priv_key}
  #   end)
  # end

  defp generate_key_pair do
    # Use the SignatureVerifier to generate a key pair
    case SignatureVerifier.generate_test_key_pair() do
      {:ok, {pub_key, priv_key}} ->
        {:ok, pub_key, priv_key}
    end
  end

  defp cleanup(dir) do
    # Cleanup temporary directory
    File.rm_rf!(dir)
  end
end

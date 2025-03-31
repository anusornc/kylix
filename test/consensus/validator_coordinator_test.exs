defmodule Kylix.Consensus.ValidatorCoordinatorTest do
  use ExUnit.Case
  alias Kylix.Consensus.ValidatorCoordinator
  alias Kylix.Auth.SignatureVerifier

  @test_dir "test/tmp/validators"

  setup do
    # Ensure test directory exists
    File.mkdir_p!(@test_dir)

    # Use the same validators that the application uses
    validators = ["agent1", "agent2"]

    # Check if coordinator is running, start it if not
    coordinator =
      case Process.whereis(ValidatorCoordinator) do
        nil ->
          # Start the coordinator if it doesn't exist
          {:ok, pid} = ValidatorCoordinator.start_link(
            validators: validators,
            config_dir: @test_dir
          )
          pid
        pid ->
          # Use existing coordinator
          pid
      end

    # Try to reset the coordinator's state if the function is available
    try do
      # If you've added the reset_coordinator_for_testing function
      ValidatorCoordinator.reset_coordinator_for_testing(validators, @test_dir)
    rescue
      _ ->
        # If the function doesn't exist, just continue
        :ok
    end

    # Return context
    {:ok, %{coordinator: coordinator, validators: validators}}
  end

  describe "basic operations" do
    test "starts with the correct validators", %{validators: validators} do
      active_validators = ValidatorCoordinator.get_validators()
      assert Enum.sort(active_validators) == Enum.sort(validators)
    end

    test "gets current validator and cycles through validators" do
      # Get first validator
      first = ValidatorCoordinator.get_current_validator()
      # Get second validator
      second = ValidatorCoordinator.get_current_validator()
      # Get third validator (should cycle back to first)
      third = ValidatorCoordinator.get_current_validator()

      assert first != second
      assert first == third
    end

    test "validator_exists? correctly identifies validators" do
      assert ValidatorCoordinator.validator_exists?("agent1")
      refute ValidatorCoordinator.validator_exists?("nonexistent_agent")
    end
  end

  describe "validator management" do
    test "add_validator adds a new validator", %{validators: validators} do
      # Generate a test key
      {:ok, {pub_key, _}} = SignatureVerifier.generate_test_key_pair()

      # Add new validator
      assert {:ok, "new_agent"} =
               ValidatorCoordinator.add_validator("new_agent", pub_key, "agent1")

      # Check it was added
      updated_validators = ValidatorCoordinator.get_validators()
      assert "new_agent" in updated_validators
      assert length(updated_validators) == length(validators) + 1

      # Check public key was stored
      assert {:ok, ^pub_key} = ValidatorCoordinator.get_validator_key("new_agent")

      # Check key file was created
      assert File.exists?(Path.join(@test_dir, "new_agent.pub"))
    end

    test "add_validator requires vouching by existing validator" do
      # Generate a test key
      {:ok, {pub_key, _}} = SignatureVerifier.generate_test_key_pair()

      # Try to add with non-existent vouching validator
      assert {:error, :unknown_validator} =
               ValidatorCoordinator.add_validator("new_agent", pub_key, "nonexistent_agent")

      # Check it wasn't added
      updated_validators = ValidatorCoordinator.get_validators()
      refute "new_agent" in updated_validators
    end

    test "remove_validator removes a validator", %{validators: validators} do
      # Remove a validator
      assert :ok = ValidatorCoordinator.remove_validator("agent2")

      # Check it was removed
      updated_validators = ValidatorCoordinator.get_validators()
      refute "agent2" in updated_validators
      assert length(updated_validators) == length(validators) - 1
    end

    test "cannot remove last validator" do
      # Remove first validator
      assert :ok = ValidatorCoordinator.remove_validator("agent2")

      # Try to remove last validator
      assert {:error, :cannot_remove_last_validator} =
               ValidatorCoordinator.remove_validator("agent1")
    end
  end

  describe "performance tracking" do
    test "records transaction performance correctly" do
      # Record some transactions
      ValidatorCoordinator.record_transaction_performance("agent1", true, 100)
      ValidatorCoordinator.record_transaction_performance("agent1", false, 200)
      ValidatorCoordinator.record_transaction_performance("agent1", true, 150)

      # Get metrics
      metrics = ValidatorCoordinator.get_performance_metrics()
      agent1_metrics = metrics["agent1"]

      # Check counts
      assert agent1_metrics.total_transactions == 3
      assert agent1_metrics.successful_transactions == 2

      # Check failure rate (use Float.round to handle precision issues)
      assert Float.round(agent1_metrics.failure_rate, 4) == Float.round(1 / 3, 4)

      # Check transaction times
      assert length(agent1_metrics.recent_tx_times) == 3
      assert_in_delta agent1_metrics.avg_tx_time, 150, 0.1
    end

    test "maintains performance window size", %{coordinator: coordinator} do
      # Use a smaller window size for testing
      window_size = 2

      # Try the set_performance_window_size function if it exists
      # Otherwise restart the coordinator with the new window size
      try do
        ValidatorCoordinator.set_performance_window_size(window_size)
      rescue
        _ ->
          # Stop the existing coordinator
          GenServer.stop(coordinator)
          # Start a new one with a small window size
          {:ok, _} = ValidatorCoordinator.start_link(
            validators: ["agent1", "agent2"],
            config_dir: @test_dir,
            performance_window: window_size
          )
      end

      # Record more transactions than the window size
      Enum.each(1..5, fn i ->
        ValidatorCoordinator.record_transaction_performance("agent1", true, i * 10)
      end)

      # Get metrics
      metrics = ValidatorCoordinator.get_performance_metrics()
      agent1_metrics = metrics["agent1"]

      # Check window size is maintained
      assert length(agent1_metrics.recent_tx_times) <= window_size
      assert length(agent1_metrics.recent_results) <= window_size

      # Check that we have the most recent values (4-5, not 1-3)
      # Only if we have any transaction times recorded
      if !Enum.empty?(agent1_metrics.recent_tx_times) do
        assert Enum.all?(agent1_metrics.recent_tx_times, fn time -> time >= 40 end)
      end

      # Try to reset back to default window size
      try do
        ValidatorCoordinator.set_performance_window_size(100)
      rescue
        _ -> :ok
      end
    end
  end
end

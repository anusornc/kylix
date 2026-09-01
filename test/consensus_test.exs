defmodule Kylix.ConsensusTest do
  use ExUnit.Case

  alias Kylix.Consensus

  describe "check_coordinator_running/0" do
    test "returns true when ValidatorCoordinator is running" do
      # Since Application.ensure_all_started(:kylix) is in test_helper.exs,
      # ValidatorCoordinator should be running globally.
      assert Consensus.check_coordinator_running() == true
    end

    test "returns false when ValidatorCoordinator is stopped" do
      # Temporarily stop the ValidatorCoordinator
      Supervisor.terminate_child(Kylix.Supervisor, Kylix.Consensus.ValidatorCoordinator)

      # The process is dead, so it should return false
      assert Consensus.check_coordinator_running() == false

      # Restart the coordinator for subsequent tests
      Supervisor.restart_child(Kylix.Supervisor, Kylix.Consensus.ValidatorCoordinator)
    end
  end
end

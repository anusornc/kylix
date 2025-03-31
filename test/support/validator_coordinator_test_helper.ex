defmodule Kylix.Test.ValidatorCoordinatorHelper do
  @moduledoc false

  # This helper is exclusively for test usage

  def reset_coordinator_for_testing(coordinator, config_dir) do
    # Reset the coordinator state for testing purposes
    GenServer.call(coordinator, {:reset_for_testing, config_dir})
  end
end

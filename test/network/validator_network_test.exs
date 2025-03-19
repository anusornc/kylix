defmodule Kylix.Network.ValidatorNetworkTest do
  use ExUnit.Case
  alias Kylix.Network.ValidatorNetwork
  alias Kylix.BlockchainServer

  setup do
    # Stop and restart the application with a clean slate
    :ok = Application.stop(:kylix)
    {:ok, _} = Application.ensure_all_started(:kylix)

    # Reset transaction count for each test
    :ok = BlockchainServer.reset_tx_count(0)

    # Get the validator network process
    validator_pid = Process.whereis(ValidatorNetwork)

    # Return values for test use
    %{validator_pid: validator_pid}
  end

  describe "validator network basic operations" do
    test "starts successfully with configured options", %{validator_pid: pid} do
      assert is_pid(pid)
      assert Process.alive?(pid)
    end

    test "get_peers returns empty list initially" do
      peers = ValidatorNetwork.get_peers()
      assert is_list(peers)
      assert Enum.empty?(peers)
    end
  end

  # We can't easily test the connection establishment or transaction broadcast
  # directly without mocking :gen_tcp, so we'll focus on the other functionality

  describe "network message handling" do
    test "processes transaction messages", %{validator_pid: pid} do
      # Create a test transaction
      tx_data = %{
        "subject" => "test_subject",
        "predicate" => "test_predicate",
        "object" => "test_object",
        "validator" => "agent1",
        "signature" => "valid_sig"
      }

      # Setup the BlockchainServer mock
      :meck.new(Kylix.BlockchainServer, [:passthrough])
      :meck.expect(Kylix.BlockchainServer, :receive_transaction, fn _ -> :ok end)

      # Create a transaction message
      transaction_message = Jason.encode!(%{
        type: "transaction",
        data: tx_data,
        timestamp: DateTime.utc_now() |> DateTime.to_iso8601()
      })

      # Send the message directly to the process - use a real socket value (like a pid)
      # to avoid the :gen_tcp.send error with References
      test_socket = pid  # Using the process itself as a "socket" to avoid gen_tcp errors

      # Send the message
      send(pid, {:tcp, test_socket, transaction_message})

      # Give some time for processing
      Process.sleep(50)

      # Verify that BlockchainServer.receive_transaction was called
      assert :meck.called(Kylix.BlockchainServer, :receive_transaction, [tx_data])

      # Clean up
      :meck.unload(Kylix.BlockchainServer)
    end

    test "processes properly formatted messages", %{validator_pid: pid} do
      # Send a properly formatted but unknown type message
      valid_json_message = Jason.encode!(%{
        type: "unknown_type",
        data: "test data",
        timestamp: DateTime.utc_now() |> DateTime.to_iso8601()
      })

      # The process should handle this without crashing
      send(pid, {:tcp, pid, valid_json_message})

      # Give time for processing
      Process.sleep(50)

      # Verify the process is still alive
      assert Process.alive?(pid)
    end
  end
end

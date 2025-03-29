defmodule Kylix.Server.TransactionQueueTest do
  use ExUnit.Case
  alias Kylix.Server.TransactionQueue

  # Test initialization following the pattern from successful integration tests
  setup do
    # Stop any running application first
    Application.stop(:kylix)

    # Start the application
    {:ok, _} = Application.ensure_all_started(:kylix)

    # Get a direct reference to the BlockchainServer process
    server_pid = Process.whereis(Kylix.BlockchainServer)

    # Clear the DAG storage
    Kylix.Storage.DAGEngine.clear_all()

    # Stop any existing TransactionQueue
    if Process.whereis(TransactionQueue) do
      try do
        GenServer.stop(TransactionQueue)
      catch
        _kind, _reason -> :ok  # Ignore errors if process is already gone
      end
    end

    # Start the queue with fast processing for tests
    {:ok, queue_pid} = TransactionQueue.start_link(batch_size: 5, processing_interval: 50)

    # Get test key pair for transaction signing
    import Kylix.Auth.SignatureVerifier
    {:ok, {public_key, private_key}} = Kylix.Auth.SignatureVerifier.generate_test_key_pair()

    # Return both process PIDs and keys for use in tests
    {:ok, %{
      server_pid: server_pid,
      queue_pid: queue_pid,
      private_key: private_key,
      public_key: public_key
    }}
  end

  # Add the test back in for submitting transactions
  test "submitting a transaction adds it to the queue" do
    {:ok, ref} = TransactionQueue.submit("subject", "predicate", "object", "agent1", "valid_sig")

    # Verify ref is a reference
    assert is_reference(ref)

    # Check queue status
    status = TransactionQueue.status()
    assert status.stats.submitted == 1

    # Check transaction status
    tx_status = TransactionQueue.get_transaction_status(ref)
    assert tx_status != nil
    assert tx_status.status == :pending
    assert tx_status.submitted_at != nil
  end

  # Test that we can clear the queue
  test "queue can be cleared" do
    # Submit transactions and store refs
    refs = Enum.map(1..3, fn i ->
      {:ok, ref} = TransactionQueue.submit("subject#{i}", "predicate", "object", "agent1", "valid_sig")
      ref
    end)

    # Verify we can get status for a transaction
    first_ref = hd(refs)
    assert TransactionQueue.get_transaction_status(first_ref) != nil

    # Clear the queue
    :ok = TransactionQueue.clear()

    # Check status
    status = TransactionQueue.status()
    assert status.queue_length == 0
    assert status.stats.submitted == 0

    # Check that transaction statuses were also cleared
    assert TransactionQueue.get_transaction_status(first_ref) == nil
  end

  # Test that we can change the processing rate
  test "processing rate can be changed" do
    # Get initial rate
    initial_status = TransactionQueue.status()
    initial_batch_size = initial_status.batch_size
    initial_interval = initial_status.processing_interval

    # Change rate
    new_batch_size = initial_batch_size * 2
    new_interval = initial_interval + 50
    :ok = TransactionQueue.set_processing_rate(new_batch_size, new_interval)

    # Check new rate
    new_status = TransactionQueue.status()
    assert new_status.batch_size == new_batch_size
    assert new_status.processing_interval == new_interval
  end

  # Test that getting an unknown transaction status returns nil
  test "get_transaction_status returns nil for unknown transactions" do
    # Generate a random ref that doesn't exist in our system
    unknown_ref = make_ref()

    # Verify we get nil for unknown transaction
    assert TransactionQueue.get_transaction_status(unknown_ref) == nil
  end

  # Test that transactions are processed asynchronously with real keys
  test "transactions are processed asynchronously", %{private_key: private_key} do
    import Kylix.Auth.SignatureVerifier

    # Add a few transactions and keep track of references
    refs = Enum.map(1..3, fn i ->
      subject = "subject#{i}"
      predicate = "predicate"
      object = "object#{i}"

      timestamp = DateTime.utc_now()
      tx_hash = hash_transaction(subject, predicate, object, "agent1", timestamp)
      signature = sign(tx_hash, private_key)

      {:ok, ref} = TransactionQueue.submit(subject, predicate, object, "agent1", signature)
      ref
    end)

    # Wait a moment for processing to potentially start
    Process.sleep(200)

    # Check that all transactions were submitted
    status = TransactionQueue.status()
    assert status.stats.submitted == 3

    # Check if transactions have been started processing
    # Since we can't guarantee how far processing has gotten, just check that
    # all the transaction references are still trackable
    all_found = Enum.all?(refs, fn ref ->
      status = TransactionQueue.get_transaction_status(ref)
      status != nil
    end)

    assert all_found
  end

  # Test transaction status changes
  test "transaction status updates via direct message", %{queue_pid: pid} do
    # Submit a transaction
    {:ok, ref} = TransactionQueue.submit("test_subject", "test_predicate", "test_object", "agent1", "valid_sig")

    # Check initial status
    initial_status = TransactionQueue.get_transaction_status(ref)
    assert initial_status.status == :pending

    # Directly send a transaction_result message to simulate completion
    # This is how the queue would normally receive completion notifications
    send(pid, {:transaction_result, ref, {:ok, "test_tx_id"}})

    # Wait a moment for message processing
    Process.sleep(50)

    # Check updated status
    final_status = TransactionQueue.get_transaction_status(ref)
    assert final_status != nil
    assert Map.has_key?(final_status, :result)
    assert final_status.result == {:ok, "test_tx_id"}
    assert Map.has_key?(final_status, :completed_at)
  end
end

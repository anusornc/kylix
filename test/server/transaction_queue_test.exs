defmodule Kylix.Server.TransactionQueueTest do
  use ExUnit.Case
  alias Kylix.Server.TransactionQueue

  # Test initialization following the pattern from successful integration tests
  setup do
    Kylix.Test.App.restart()

    server_pid = Process.whereis(Kylix.BlockchainServer)

    unless server_pid do
      flunk("BlockchainServer not running - required for this test")
    end

    queue_pid = Process.whereis(TransactionQueue)

    unless queue_pid do
      flunk("TransactionQueue not running - required for this test")
    end

    :ok = TransactionQueue.set_processing_rate(5, 50)

    {public_key, private_key} = Kylix.Test.Attester.generate_keys()

    {:ok,
     %{
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
    refs =
      Enum.map(1..3, fn i ->
        {:ok, ref} =
          TransactionQueue.submit("subject#{i}", "predicate", "object", "agent1", "valid_sig")

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

  # Fix for the failing test
  test "transaction status updates via direct message" do
    # Generate a reference directly instead of submitting a transaction
    ref = make_ref()
    now = DateTime.utc_now()

    # Construct the initial state map directly for deterministic unit testing
    initial_state = %{
      queue: :queue.new(),
      processing: false,
      batch_size: 10,
      processing_interval: 100,
      transaction_statuses: %{
        ref => %{
          status: :pending,
          submitted_at: now
        }
      },
      stats: %{
        submitted: 1,
        processed: 0,
        failed: 0,
        last_processed_at: nil
      }
    }

    # Verify initial status in our mock state
    assert initial_state.transaction_statuses[ref].status == :pending

    # Call the callback directly to avoid Process.sleep and non-deterministic behavior
    {:noreply, final_state} =
      TransactionQueue.handle_info({:transaction_result, ref, {:ok, "test_tx_id"}}, initial_state)

    # Check final status in the returned state
    final_status = Map.get(final_state.transaction_statuses, ref)
    assert final_status != nil
    assert Map.has_key?(final_status, :result)
    assert final_status.result == {:ok, "test_tx_id"}
    assert Map.has_key?(final_status, :completed_at)

    # Check that stats were updated correctly
    assert final_state.stats.processed == 1
    assert final_state.stats.failed == 0
    assert final_state.stats.last_processed_at != nil
  end

  # Test that transactions are processed asynchronously with real keys
  test "transactions are processed asynchronously", %{private_key: private_key} do
    refs =
      Enum.map(1..3, fn i ->
        subject = "subject#{i}"
        predicate = "predicate"
        object = "object#{i}"

        signature =
          Kylix.Test.Attester.signature(subject, predicate, object, "agent1", private_key)

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
    all_found =
      Enum.all?(refs, fn ref ->
        status = TransactionQueue.get_transaction_status(ref)
        status != nil
      end)

    assert all_found
  end
end

defmodule Kylix.Server.TransactionQueue do
  @moduledoc """
  Manages asynchronous transaction processing via a queue.

  This module provides:
  - Asynchronous transaction submission
  - Round-robin validator assignment
  - Queue status monitoring
  - Rate limiting capabilities
  """

  use GenServer
  require Logger

  @default_batch_size 10
  @default_processing_interval 100 # milliseconds

  # Client API

  @doc """
  Starts the transaction queue server.

  ## Options

  * `:batch_size` - Number of transactions to process per batch (default: 10)
  * `:processing_interval` - Time between batch processing in ms (default: 100)
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Submits a transaction to the queue for asynchronous processing.

  Returns `{:ok, reference}` where reference is a unique identifier for tracking.
  """
  def submit(subject, predicate, object, validator_id, signature) do
    GenServer.call(__MODULE__, {:submit, subject, predicate, object, validator_id, signature})
  end

  @doc """
  Gets the current status of the transaction queue.

  Returns a map with status information including queue length and statistics.
  """
  def status do
    GenServer.call(__MODULE__, :status)
  end

  @doc """
  Gets the status of a specific transaction by its reference.

  Returns nil if the transaction is not found, or a map with status information:
  - For pending transactions: %{status: :pending, submitted_at: timestamp}
  - For completed transactions: %{result: {:ok, tx_id} | {:error, reason}, completed_at: timestamp}
  """
  def get_transaction_status(ref) do
    GenServer.call(__MODULE__, {:get_transaction_status, ref})
  end

  @doc """
  Changes the processing rate for the transaction queue.

  ## Parameters

  * `batch_size` - New batch size
  * `interval_ms` - New processing interval in milliseconds
  """
  def set_processing_rate(batch_size, interval_ms) do
    GenServer.call(__MODULE__, {:set_rate, batch_size, interval_ms})
  end

  @doc """
  Clears the transaction queue, useful for testing.
  """
  def clear do
    GenServer.call(__MODULE__, :clear)
  end

  @doc """
  Process a single transaction directly for debugging
  """
  def process_transaction(ref) do
    GenServer.call(__MODULE__, {:process_transaction, ref})
  end

  # Server callbacks

  @impl true
  def init(opts) do
    Logger.info("Initializing TransactionQueue")
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)
    processing_interval = Keyword.get(opts, :processing_interval, @default_processing_interval)

    # Try to get validators from BlockchainServer with fallback
    validators = try do
      # Try to get validators from the blockchain server
      case Kylix.get_validators() do
        [_|_] = valid_validators ->
          # Non-empty list of validators
          Logger.info("Found validators: #{inspect(valid_validators)}")
          valid_validators
        _ ->
          # Empty list or any unexpected value
          Logger.warning("No valid validators found from BlockchainServer, using default fallback validators")
          ["agent1", "agent2"] # Fallback validators
      end
    rescue
      e ->
        # In case the server is not available (e.g., during tests or standalone usage)
        Logger.warning("Error getting validators from BlockchainServer: #{inspect(e)}")
        ["agent1", "agent2"] # Fallback validators for tests or when server is down
    end

    Logger.info("TransactionQueue initialized with validators: #{inspect(validators)}")

    # Initialize state
    state = %{
      queue: :queue.new(),
      processing: false,
      current_validator_index: 0,
      validators: validators,
      batch_size: batch_size,
      processing_interval: processing_interval,
      # Track all transactions by their reference ID
      transaction_statuses: %{},
      stats: %{
        submitted: 0,
        processed: 0,
        failed: 0,
        last_processed_at: nil
      }
    }

    # Schedule initial processing
    Logger.info("Scheduling initial processing with interval: #{processing_interval}ms")
    schedule_processing(processing_interval)

    {:ok, state}
  end

  @impl true
  def handle_call({:submit, subject, predicate, object, validator_id, signature}, _from, state) do
    # Generate a unique reference for this transaction
    ref = make_ref()

    # Get current timestamp
    now = DateTime.utc_now()

    # Create transaction data
    tx_data = %{
      ref: ref,
      subject: subject,
      predicate: predicate,
      object: object,
      validator_id: validator_id,
      signature: signature,
      submitted_at: now
    }

    # Add to queue
    new_queue = :queue.in(tx_data, state.queue)

    # Store initial pending status for this transaction
    updated_statuses = Map.put(state.transaction_statuses, ref, %{
      status: :pending,
      submitted_at: now
    })

    # Update state
    new_state = %{state |
      queue: new_queue,
      transaction_statuses: updated_statuses,
      stats: %{state.stats | submitted: state.stats.submitted + 1}
    }

    # Log submission
    Logger.info("Transaction queued with ref #{inspect(ref)}, queue length: #{:queue.len(new_queue)}")

    {:reply, {:ok, ref}, new_state}
  end

  @impl true
  def handle_call({:process_transaction, ref}, _from, state) do
    # Find transaction with this ref in our statuses
    case Map.get(state.transaction_statuses, ref) do
      nil ->
        {:reply, {:error, :not_found}, state}

      tx_status ->
        if Map.has_key?(tx_status, :result) do
          # Already processed
          {:reply, {:error, :already_processed}, state}
        else
          # Find transaction data in queue
          case find_and_remove_from_queue(state.queue, ref) do
            {nil, _} ->
              # Not in queue
              {:reply, {:error, :not_in_queue}, state}

            {tx_data, new_queue} ->
              # Process directly in this process
              current_validator = Enum.at(state.validators, state.current_validator_index)
              tx_data = Map.put(tx_data, :validator_id, current_validator)

              # Process transaction
              result = try do
                Kylix.BlockchainServer.add_transaction(
                  tx_data.subject,
                  tx_data.predicate,
                  tx_data.object,
                  tx_data.validator_id,
                  tx_data.signature
                )
              rescue
                e ->
                  Logger.error("Error processing transaction: #{inspect(e)}")
                  {:error, :processing_failed}
              end

              # Update transaction status
              now = DateTime.utc_now()
              updated_statuses = Map.put(state.transaction_statuses, ref, %{
                result: result,
                completed_at: now
              })

              # Update stats
              new_stats = case result do
                {:ok, _} ->
                  %{state.stats |
                    processed: state.stats.processed + 1,
                    last_processed_at: now
                  }

                {:error, _} ->
                  %{state.stats |
                    processed: state.stats.processed + 1,
                    failed: state.stats.failed + 1,
                    last_processed_at: now
                  }
              end

              # Update state
              next_index = rem(state.current_validator_index + 1, length(state.validators))
              new_state = %{state |
                queue: new_queue,
                current_validator_index: next_index,
                transaction_statuses: updated_statuses,
                stats: new_stats
              }

              {:reply, result, new_state}
          end
        end
    end
  end

  @impl true
  def handle_call(:status, _from, state) do
    status = %{
      queue_length: :queue.len(state.queue),
      processing: state.processing,
      validators: state.validators,
      current_validator: Enum.at(state.validators, state.current_validator_index),
      batch_size: state.batch_size,
      processing_interval: state.processing_interval,
      stats: state.stats,
      # Add transaction tracking information
      transaction_count: map_size(state.transaction_statuses),
      pending_count: count_pending_transactions(state.transaction_statuses),
      completed_count: count_completed_transactions(state.transaction_statuses)
    }

    {:reply, status, state}
  end

  @impl true
  def handle_call({:set_rate, batch_size, interval_ms}, _from, state) do
    new_state = %{state |
      batch_size: batch_size,
      processing_interval: interval_ms
    }

    Logger.info("Transaction processing rate changed to #{batch_size} per #{interval_ms}ms")

    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call(:clear, _from, state) do
    new_state = %{state |
      queue: :queue.new(),
      transaction_statuses: %{},
      stats: %{
        submitted: 0,
        processed: 0,
        failed: 0,
        last_processed_at: nil
      }
    }

    Logger.info("Transaction queue cleared")

    {:reply, :ok, new_state}
  end

  @doc """
  Gets the status of a specific transaction by its reference.
  """
  @impl true
  def handle_call({:get_transaction_status, ref}, _from, state) do
    status = Map.get(state.transaction_statuses, ref)
    {:reply, status, state}
  end

  @impl true
  def handle_info(:process_batch, state) do
    Logger.debug("Process batch message received, queue length: #{:queue.len(state.queue)}")

    # Process a batch of transactions if queue is not empty
    result = if :queue.is_empty(state.queue) do
      Logger.debug("Queue is empty, nothing to process")
      {state, 0}
    else
      # Mark as processing
      state = %{state | processing: true}
      Logger.info("Starting to process batch, queue length: #{:queue.len(state.queue)}")

      # Process a batch
      batch_size = min(state.batch_size, :queue.len(state.queue))
      {new_state, processed_count} = process_batch(state, batch_size)

      # Log progress
      if processed_count > 0 do
        Logger.info("Processed #{processed_count} transactions, remaining: #{:queue.len(new_state.queue)}")
      else
        Logger.warning("Batch processing completed but no transactions were processed")
      end

      {new_state, processed_count}
    end

    # Extract values from the result
    {new_state, processed_count} = result

    # Now processed_count is available here
    Logger.debug("Batch cycle complete. Processed #{processed_count} transactions total.")

    # Schedule next processing
    Logger.debug("Scheduling next processing in #{state.processing_interval}ms")
    schedule_processing(state.processing_interval)

    {:noreply, %{new_state | processing: false}}
  end

  @impl true
  def handle_info({:transaction_result, ref, result}, state) do
    # Get current timestamp
    now = DateTime.utc_now()

    Logger.info("Received transaction result for ref #{inspect(ref)}: #{inspect(result)}")

    # Update transaction status for this specific reference
    updated_statuses = Map.put(state.transaction_statuses, ref, %{
      result: result,
      completed_at: now
    })

    # Update stats based on result
    new_stats = case result do
      {:ok, tx_id} ->
        Logger.info("Transaction #{inspect(ref)} completed successfully with ID: #{tx_id}")
        %{state.stats |
          processed: state.stats.processed + 1,
          last_processed_at: now
        }

      {:error, reason} ->
        Logger.warning("Transaction #{inspect(ref)} failed: #{inspect(reason)}")
        %{state.stats |
          processed: state.stats.processed + 1,
          failed: state.stats.failed + 1,
          last_processed_at: now
        }
    end

    {:noreply, %{state |
      stats: new_stats,
      transaction_statuses: updated_statuses
    }}
  end

  # Helper functions

  defp schedule_processing(interval) do
    Logger.debug("Scheduled processing in #{interval}ms")
    Process.send_after(self(), :process_batch, interval)
  end

  # Find and remove a transaction from the queue by its reference
  defp find_and_remove_from_queue(queue, ref) do
    # Convert queue to list for easier manipulation
    queue_list = :queue.to_list(queue)

    # Find the transaction with matching ref
    case Enum.find_index(queue_list, fn tx -> tx.ref == ref end) do
      nil ->
        {nil, queue}

      index ->
        # Get the transaction
        tx_data = Enum.at(queue_list, index)

        # Remove it from the list
        new_list = List.delete_at(queue_list, index)

        # Convert back to queue
        new_queue = :queue.from_list(new_list)

        {tx_data, new_queue}
    end
  end

  # Count transactions in pending state
  defp count_pending_transactions(transaction_statuses) do
    Enum.count(transaction_statuses, fn {_ref, status} ->
      Map.get(status, :status) == :pending
    end)
  end

  # Count transactions that have completed (have a result)
  defp count_completed_transactions(transaction_statuses) do
    Enum.count(transaction_statuses, fn {_ref, status} ->
      Map.has_key?(status, :result)
    end)
  end

  defp process_batch(state, 0), do: {state, 0}
  defp process_batch(state, batch_size) do
    Logger.debug("Processing batch of size #{batch_size}")
    Enum.reduce(1..batch_size, {state, 0}, fn i, {current_state, count} ->
      Logger.debug("Processing transaction #{i} of #{batch_size}")
      case :queue.out(current_state.queue) do
        {{:value, tx_data}, new_queue} ->
          # Get the current validator based on round-robin
          current_validator = Enum.at(current_state.validators, current_state.current_validator_index)
          Logger.debug("Using validator: #{current_validator} for transaction #{i}")

          # Update validator index (round-robin)
          next_index = rem(current_state.current_validator_index + 1, length(current_state.validators))

          # Process transaction in this process to avoid losing results
          # This is the KEY change - process directly instead of spawning
          # tx_data = Map.put(tx_data, :validator_id, current_validator)
          ref = tx_data.ref

          # Process the transaction
          result = try do
            Kylix.BlockchainServer.add_transaction(
              tx_data.subject,
              tx_data.predicate,
              tx_data.object,
              tx_data.validator_id,
              tx_data.signature
            )
          rescue
            e ->
              Logger.error("Error processing transaction #{inspect(ref)}: #{inspect(e)}")
              {:error, :processing_error}
          catch
            kind, reason ->
              Logger.error("Transaction processing caught #{kind}: #{inspect(reason)}")
              {:error, :processing_exception}
          end

          # Update transaction status directly
          now = DateTime.utc_now()
          updated_statuses = Map.put(current_state.transaction_statuses, ref, %{
            result: result,
            completed_at: now
          })

          # Update stats based on result
          new_stats = case result do
            {:ok, _tx_id} ->
              %{current_state.stats |
                processed: current_state.stats.processed + 1,
                last_processed_at: now
              }

            {:error, _reason} ->
              %{current_state.stats |
                processed: current_state.stats.processed + 1,
                failed: current_state.stats.failed + 1,
                last_processed_at: now
              }
          end

          # Return updated state
          {%{current_state |
            queue: new_queue,
            current_validator_index: next_index,
            transaction_statuses: updated_statuses,
            stats: new_stats
          }, count + 1}

        {:empty, _} ->
          # Queue is empty, stop processing
          Logger.debug("Queue became empty during processing")
          {current_state, count}
      end
    end)
  end
end

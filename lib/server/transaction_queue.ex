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

  # Server callbacks

  @impl true
  def init(opts) do
    batch_size = Keyword.get(opts, :batch_size, @default_batch_size)
    processing_interval = Keyword.get(opts, :processing_interval, @default_processing_interval)

    # Try to get validators from BlockchainServer with fallback
    validators = try do
      # Try to get validators from the blockchain server
      case Kylix.get_validators() do
        [_|_] = valid_validators ->
          # Non-empty list of validators
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
    Logger.debug("Transaction queued with ref #{inspect(ref)}, queue length: #{:queue.len(new_queue)}")

    {:reply, {:ok, ref}, new_state}
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
    # Process a batch of transactions if queue is not empty
    {new_state, processing} = if :queue.is_empty(state.queue) do
      {state, false}
    else
      # Mark as processing
      state = %{state | processing: true}

      # Process a batch
      batch_size = min(state.batch_size, :queue.len(state.queue))
      {new_state, processed_count} = process_batch(state, batch_size)

      # Log progress
      if processed_count > 0 do
        Logger.info("Processed #{processed_count} transactions, remaining: #{:queue.len(new_state.queue)}")
      end

      {new_state, true}
    end

    # Schedule next processing
    schedule_processing(state.processing_interval)

    {:noreply, %{new_state | processing: processing}}
  end

  @impl true
  def handle_info({:transaction_result, ref, result}, state) do
    # Get current timestamp
    now = DateTime.utc_now()

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
    Process.send_after(self(), :process_batch, interval)
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
    Enum.reduce(1..batch_size, {state, 0}, fn _, {current_state, count} ->
      case :queue.out(current_state.queue) do
        {{:value, tx_data}, new_queue} ->
          # Get the current validator based on round-robin
          current_validator = Enum.at(current_state.validators, current_state.current_validator_index)

          # Process transaction in separate process to not block
          tx_data = Map.put(tx_data, :validator_id, current_validator)

          spawn(fn ->
            # Try to add the transaction but handle errors gracefully
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
            catch
              :exit, reason ->
                Logger.error("Transaction processing exited: #{inspect(reason)}")
                {:error, :processing_exited}
            end

            # Send result back to queue
            send(self(), {:transaction_result, tx_data.ref, result})
          end)

          # Update validator index (round-robin)
          next_index = rem(current_state.current_validator_index + 1, length(current_state.validators))

          # Return updated state
          {%{current_state |
            queue: new_queue,
            current_validator_index: next_index
          }, count + 1}

        {:empty, _} ->
          # Queue is empty, stop processing
          {current_state, count}
      end
    end)
  end
end

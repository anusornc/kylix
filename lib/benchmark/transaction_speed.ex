defmodule Kylix.Benchmark.TransactionSpeed do
  @moduledoc """
  Benchmark module for testing Kylix transaction throughput.
  Supports both synchronous and asynchronous transaction submission.
  Saves results to /data/benchmark directory.
  """

  @output_dir "data/benchmark"
  import Kylix.Auth.SignatureVerifier

  @doc """
  Runs a benchmark test using synchronous transaction submission.
  This is the standard approach where each transaction completes before the next is submitted.
  """
  def run_baseline_test(num_transactions \\ 1000) do
    ensure_output_dir()

    # Reset the application
    {:ok, _} = Application.ensure_all_started(:kylix)

    # Get test key pair with fallback to generating a new one
    {:ok, %{private_key: private_key, public_key: _public_key}} = get_test_key_pair_with_fallback()

    # Set up test data
    subject_base = "entity:test"
    predicate = "prov:wasGeneratedBy"
    object_base = "activity:process"
    validator = "agent1"  # Use a known validator

    # Start timing
    start_time = System.monotonic_time(:millisecond)

    # Execute transactions
    IO.puts("Running #{num_transactions} transactions for baseline test...")
    results = Enum.map(1..num_transactions, fn i ->
      subject = "#{subject_base}#{i}"
      object = "#{object_base}#{i}"

      # Generate proper signature for each transaction
      timestamp = DateTime.utc_now()
      tx_hash = hash_transaction(subject, predicate, object, validator, timestamp)
      signature = sign(tx_hash, private_key)

      # Add transaction and measure time
      tx_start = System.monotonic_time(:microsecond)
      result = Kylix.add_transaction(subject, predicate, object, validator, signature)
      tx_end = System.monotonic_time(:microsecond)
      tx_time = tx_end - tx_start

      # Log progress every 100 transactions
      if rem(i, 100) == 0, do: IO.puts("Completed #{i} transactions. Last took #{tx_time}μs")

      # Log success/failure
      case result do
        {:ok, tx_id} ->
          IO.puts("Transaction #{i} successful with ID: #{tx_id}")
        {:error, reason} ->
          IO.puts("Transaction #{i} failed: #{inspect(reason)}")
      end

      {result, tx_time}
    end)

    # End timing
    end_time = System.monotonic_time(:millisecond)
    total_time = end_time - start_time

    # Calculate statistics
    successful_txs = Enum.count(results, fn {{status, _}, _} -> status == :ok end)
    transaction_times = Enum.map(results, fn {_, time} -> time end)
    avg_tx_time = Enum.sum(transaction_times) / num_transactions

    # Calculate TPS (Transactions Per Second)
    tps = if total_time > 0, do: successful_txs / (total_time / 1000), else: 0.0

    # Prepare result data
    result = %{
      test_type: "baseline",
      timestamp: DateTime.utc_now() |> DateTime.to_iso8601(),
      hardware_info: get_hardware_info(),
      total_transactions: num_transactions,
      successful_transactions: successful_txs,
      total_time_ms: total_time,
      average_tx_time_us: avg_tx_time,
      transactions_per_second: tps,
      min_tx_time_us: Enum.min(transaction_times),
      max_tx_time_us: Enum.max(transaction_times),
      transaction_times: transaction_times,
      percentiles: calculate_percentiles(transaction_times)
    }

    # Save result to file
    save_result_to_file(result, "baseline_test")

    # Print summary
    print_test_summary("Baseline Test", result)

    result
  end

  @doc """
  Runs a benchmark test using asynchronous transaction submission via the transaction queue.
  This approach allows for much higher throughput as transactions are submitted without waiting
  for previous ones to complete.
  """
  def run_async_test(num_transactions \\ 1000, opts \\ []) do
    ensure_output_dir()

    # Reset the application
    {:ok, _} = Application.ensure_all_started(:kylix)

    # Configure the transaction queue
    batch_size = Keyword.get(opts, :batch_size, 20)
    processing_interval = Keyword.get(opts, :processing_interval, 50)
    Kylix.Server.TransactionQueue.set_processing_rate(batch_size, processing_interval)
    Kylix.Server.TransactionQueue.clear()

    # Get test key pair with fallback to generating a new one
    {:ok, %{private_key: private_key, public_key: _public_key}} = get_test_key_pair_with_fallback()

    # Set up test data
    subject_base = "entity:async_test"
    predicate = "prov:wasGeneratedBy"
    object_base = "activity:process"
    validator = "agent1"  # Use a known validator - the queue will handle rotation

    # Start timing
    start_time = System.monotonic_time(:millisecond)

    # Submit transactions asynchronously
    IO.puts("Submitting #{num_transactions} transactions asynchronously...")
    transaction_refs = Enum.map(1..num_transactions, fn i ->
      subject = "#{subject_base}#{i}"
      object = "#{object_base}#{i}"

      # Generate proper signature for each transaction
      timestamp = DateTime.utc_now()
      tx_hash = hash_transaction(subject, predicate, object, validator, timestamp)
      signature = sign(tx_hash, private_key)

      # Measure submission time only - actual processing happens asynchronously
      submit_start = System.monotonic_time(:microsecond)
      {:ok, ref} = Kylix.add_transaction_async(subject, predicate, object, validator, signature)
      submit_end = System.monotonic_time(:microsecond)
      submit_time = submit_end - submit_start

      # Log progress every 100 transactions
      if rem(i, 100) == 0, do: IO.puts("Submitted #{i} transactions. Last took #{submit_time}μs")

      {ref, submit_time}
    end)

    # End submission timing
    submission_end_time = System.monotonic_time(:millisecond)
    submission_total_time = submission_end_time - start_time

    # Extract submission times and references
    submission_times = Enum.map(transaction_refs, fn {_ref, time} -> time end)
    refs = Enum.map(transaction_refs, fn {ref, _time} -> ref end)

    # Wait for processing to complete
    IO.puts("Waiting for transactions to be processed...")
    wait_for_processing(num_transactions)

    # End timing (includes both submission and processing)
    end_time = System.monotonic_time(:millisecond)
    total_time = end_time - start_time

    # Get queue status
    status = Kylix.get_queue_status()
    _queue_processed = status.stats.processed
    _queue_failed = status.stats.failed

    # Get result information for each transaction
    tx_results = Enum.map(refs, fn ref ->
      Kylix.Server.TransactionQueue.get_transaction_status(ref)
    end)

    # Count successful and failed transactions
    success_count = Enum.count(tx_results, fn
      nil -> false
      status -> match?({:ok, _}, Map.get(status, :result, {:error, :not_processed}))
    end)

    failure_count = Enum.count(tx_results, fn
      nil -> false
      status -> match?({:error, _}, Map.get(status, :result, {:ok, "counted_as_success"}))
    end)

    # Calculate statistics
    avg_submit_time = Enum.sum(submission_times) / num_transactions
    submit_throughput = num_transactions / (submission_total_time / 1000)
    total_throughput = success_count / (total_time / 1000)

    # Prepare result data
    result = %{
      test_type: "async_queue",
      timestamp: DateTime.utc_now() |> DateTime.to_iso8601(),
      hardware_info: get_hardware_info(),
      queue_config: %{
        batch_size: batch_size,
        processing_interval: processing_interval
      },
      total_transactions: num_transactions,
      successful_transactions: success_count,
      failed_transactions: failure_count,
      submission_time_ms: submission_total_time,
      total_time_ms: total_time,
      average_submit_time_us: avg_submit_time,
      submission_throughput: submit_throughput,
      transactions_per_second: total_throughput,
      min_submit_time_us: Enum.min(submission_times),
      max_submit_time_us: Enum.max(submission_times),
      submission_times: submission_times,
      percentiles: calculate_percentiles(submission_times)
    }

    # Save result to file
    save_result_to_file(result, "async_test")

    # Print summary
    print_async_test_summary("Async Queue Test", result)

    result
  end

  # Wait for processing to complete
  defp wait_for_processing(expected_count, max_wait_ms \\ 30000) do
    start_time = System.monotonic_time(:millisecond)

    wait_with_timeout(fn ->
      status = Kylix.get_queue_status()
      processed = status.stats.processed
      queue_length = status.queue_length

      current_time = System.monotonic_time(:millisecond)
      elapsed = current_time - start_time

      # Every few seconds, print status
      if rem(div(elapsed, 1000), 2) == 0 do
        IO.puts("Progress: #{processed}/#{expected_count} processed, #{queue_length} in queue, #{div(elapsed, 1000)}s elapsed")
      end

      # Check if we've processed enough or if queue is empty and we've processed something
      if processed >= expected_count || (queue_length == 0 && processed > 0) do
        true
      else
        # Wait a bit before checking again
        Process.sleep(100)
        false
      end
    end, max_wait_ms, "Timeout waiting for transaction processing")
  end

  # Helper function to wait with timeout
  defp wait_with_timeout(condition_fn, max_wait_ms, timeout_message) do
    start_time = System.monotonic_time(:millisecond)

    unless wait_until(condition_fn, max_wait_ms) do
      current_time = System.monotonic_time(:millisecond)
      elapsed = current_time - start_time
      IO.puts("#{timeout_message} after #{elapsed}ms")
    end
  end

  # Wait until condition is true or timeout
  defp wait_until(condition_fn, max_wait_ms) do
    start_time = System.monotonic_time(:millisecond)

    Stream.cycle([1])
    |> Enum.reduce_while(false, fn _, _ ->
      current_time = System.monotonic_time(:millisecond)
      if current_time - start_time > max_wait_ms do
        {:halt, false}
      else
        if condition_fn.() do
          {:halt, true}
        else
          {:cont, false}
        end
      end
    end)
  end

  # Helper function to get test key pair with fallback
  defp get_test_key_pair_with_fallback() do
    case get_test_key_pair() do
      {:ok, %{private_key: private_key, public_key: public_key}} when not is_nil(private_key) ->
        # Valid key pair from the server
        {:ok, %{private_key: private_key, public_key: public_key}}

      _ ->
        # No valid key pair, generate a temporary one
        IO.puts("No valid test key pair available, generating a temporary one...")
        {:ok, {public_key, private_key}} = Kylix.Auth.SignatureVerifier.generate_test_key_pair()
        {:ok, %{private_key: private_key, public_key: public_key}}
    end
  end

  # Helper function to get test key pair from the blockchain server
  defp get_test_key_pair() do
    GenServer.call(Kylix.BlockchainServer, :get_test_key_pair)
  end

  # Helper functions
  defp ensure_output_dir do
    File.mkdir_p!(@output_dir)
  end

  defp save_result_to_file(result, name, dir \\ nil) do
    dir = dir || @output_dir

    # Create timestamp-based filename
    timestamp = DateTime.utc_now() |> DateTime.to_iso8601() |> String.replace(":", "-")
    filename = "#{name}_#{timestamp}.json"

    # Write to file
    file_path = Path.join(dir, filename)
    File.write!(file_path, Jason.encode!(result, pretty: true))

    IO.puts("Results saved to: #{file_path}")
  end

  defp calculate_percentiles(times) do
    sorted_times = Enum.sort(times)
    len = length(sorted_times)

    %{
      p50: Enum.at(sorted_times, floor(len * 0.5)),
      p90: Enum.at(sorted_times, floor(len * 0.9)),
      p95: Enum.at(sorted_times, floor(len * 0.95)),
      p99: Enum.at(sorted_times, floor(len * 0.99))
    }
  end

  defp get_hardware_info do
    # Hardware info retrieval code - simplified for now
    "Linux: CPU info, RAM info"
  end

  defp print_test_summary(test_name, result) do
    IO.puts("\n#{test_name} Results:")
    IO.puts(String.duplicate("-", String.length(test_name) + 9))
    IO.puts("Total transactions: #{result.total_transactions}")
    IO.puts("Successful transactions: #{result.successful_transactions}")
    IO.puts("Total time: #{result.total_time_ms}ms")
    IO.puts("Average transaction time: #{result.average_tx_time_us |> Float.round(2)}μs")
    IO.puts("Transactions per second (TPS): #{result.transactions_per_second |> Float.round(2)}")
    IO.puts("Min transaction time: #{result.min_tx_time_us}μs")
    IO.puts("Max transaction time: #{result.max_tx_time_us}μs")
    IO.puts("P50 latency: #{result.percentiles.p50}μs")
    IO.puts("P99 latency: #{result.percentiles.p99}μs")
  end

  defp print_async_test_summary(test_name, result) do
    IO.puts("\n#{test_name} Results:")
    IO.puts(String.duplicate("-", String.length(test_name) + 9))
    IO.puts("Total transactions: #{result.total_transactions}")
    IO.puts("Successful transactions: #{result.successful_transactions}")
    IO.puts("Failed transactions: #{result.failed_transactions}")
    IO.puts("Submission time: #{result.submission_time_ms}ms")
    IO.puts("Total processing time: #{result.total_time_ms}ms")
    IO.puts("Average submission time: #{result.average_submit_time_us |> Float.round(2)}μs")
    IO.puts("Submission throughput: #{result.submission_throughput |> Float.round(2)} tx/s")
    IO.puts("Total throughput (TPS): #{result.transactions_per_second |> Float.round(2)} tx/s")
    IO.puts("Min submission time: #{result.min_submit_time_us}μs")
    IO.puts("Max submission time: #{result.max_submit_time_us}μs")
    IO.puts("P50 submission latency: #{result.percentiles.p50}μs")
    IO.puts("P99 submission latency: #{result.percentiles.p99}μs")
  end
end

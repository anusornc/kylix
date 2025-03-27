defmodule Kylix.Benchmark.TransactionSpeed do
  @moduledoc """
  Benchmark module for testing Kylix transaction throughput.
  Saves results to /data/benchmark directory.
  """

  @output_dir "data/benchmark"

  def run_baseline_test(num_transactions \\ 1000) do
    ensure_output_dir()

    # Reset the application
    :ok = Application.stop(:kylix)
    {:ok, _} = Application.ensure_all_started(:kylix)

    # Set up test data
    subject_base = "entity:test"
    predicate = "prov:wasGeneratedBy"
    object_base = "activity:process"
    validator = "agent1"
    signature = "valid_sig"

    # Start timing
    start_time = System.monotonic_time(:millisecond)

    # Execute transactions
    IO.puts("Running #{num_transactions} transactions for baseline test...")
    results = Enum.map(1..num_transactions, fn i ->
      subject = "#{subject_base}#{i}"
      object = "#{object_base}#{i}"

      # Add transaction and measure time
      tx_start = System.monotonic_time(:microsecond)
      result = Kylix.add_transaction(subject, predicate, object, validator, signature)
      tx_end = System.monotonic_time(:microsecond)
      tx_time = tx_end - tx_start

      # Log progress every 100 transactions
      if rem(i, 100) == 0, do: IO.puts("Completed #{i} transactions. Last took #{tx_time}μs")

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
    tps = successful_txs / (total_time / 1000)

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

  # Additional functions like parallel_test, scaling_test, etc.
  # I've omitted these for brevity but they would be similar to run_baseline_test

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
    # Hardware info retrieval code
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
end

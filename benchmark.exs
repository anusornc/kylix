defmodule Benchmark do
  def old_find_and_remove(queue, ref) do
    queue_list = :queue.to_list(queue)
    case Enum.find_index(queue_list, fn tx -> tx.ref == ref end) do
      nil -> {nil, queue}
      index ->
        tx_data = Enum.at(queue_list, index)
        new_list = List.delete_at(queue_list, index)
        new_queue = :queue.from_list(new_list)
        {tx_data, new_queue}
    end
  end

  def new_find_and_remove(queue, ref) do
    do_find(queue, ref, :queue.new())
  end

  defp do_find(queue, ref, acc) do
    case :queue.out(queue) do
      {{:value, tx}, rest} ->
        if tx.ref == ref do
          {tx, :queue.join(acc, rest)}
        else
          do_find(rest, ref, :queue.in(tx, acc))
        end
      {:empty, _} ->
        {nil, acc}
    end
  end

  def run do
    # Generate transactions
    n = 1000
    refs = Enum.map(1..n, fn _ -> make_ref() end)
    txs = Enum.map(refs, fn ref -> %{ref: ref, data: "some_data"} end)

    # Randomly shuffle refs for removal
    shuffled_refs = Enum.shuffle(refs)

    # We start with a full queue and remove everything one by one
    full_queue = Enum.reduce(txs, :queue.new(), fn tx, acc -> :queue.in(tx, acc) end)

    # Measure old performance
    start_old = System.monotonic_time(:microsecond)
    Enum.reduce(shuffled_refs, full_queue, fn ref, q ->
      {_tx, new_q} = old_find_and_remove(q, ref)
      new_q
    end)
    end_old = System.monotonic_time(:microsecond)

    # Measure new performance
    start_new = System.monotonic_time(:microsecond)
    Enum.reduce(shuffled_refs, full_queue, fn ref, q ->
      {_tx, new_q} = new_find_and_remove(q, ref)
      new_q
    end)
    end_new = System.monotonic_time(:microsecond)

    old_time = end_old - start_old
    new_time = end_new - start_new

    IO.puts("--- Benchmark Results (Queue size: #{n}, #{n} Removals) ---")
    IO.puts("Old O(N) approach:       #{old_time} μs")
    IO.puts("New recursive approach:  #{new_time} μs")
    IO.puts("Speedup:                 #{Float.round(old_time / new_time, 2)}x")
  end
end

Benchmark.run()

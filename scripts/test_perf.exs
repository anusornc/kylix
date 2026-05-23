defmodule PerfTest do
  def run do
    validators = Enum.map(1..10_000, &"validator_#{&1}")
    validators_tuple = List.to_tuple(validators)
    tx_count = 5_000_000
    len = length(validators)

    {list_time, _} = :timer.tc(fn ->
      Enum.reduce(1..1000, nil, fn i, _ ->
        Enum.at(validators, rem(tx_count + i, len))
      end)
    end)

    {tuple_time, _} = :timer.tc(fn ->
      Enum.reduce(1..1000, nil, fn i, _ ->
        elem(validators_tuple, rem(tx_count + i, len))
      end)
    end)

    IO.puts("List time: #{list_time} microseconds")
    IO.puts("Tuple time: #{tuple_time} microseconds")
  end
end

PerfTest.run()

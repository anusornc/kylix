defmodule KylixTest do
  use ExUnit.Case
  doctest Kylix

  setup do
    Application.stop(:kylix)
    Application.ensure_all_started(:kylix)
    :ok = GenServer.call(Kylix.BlockchainServer, {:reset_tx_count, 0})
    :ok
  end

  test "add transaction with valid validator and signature" do
    assert {:ok, tx_id} = Kylix.add_transaction("subject", "predicate", "object", "agent1", "valid_sig")
    assert is_binary(tx_id)
  end

  test "add transaction with invalid validator" do
    assert {:error, :unknown_validator} =
             Kylix.add_transaction("subject", "predicate", "object", "unknown_agent", "valid_sig")
  end

  test "query transactions" do
    assert {:ok, _tx_id1} = Kylix.add_transaction("subject1", "predicate1", "object1", "agent1", "valid_sig")
    Process.sleep(10)
    :ok = GenServer.call(Kylix.BlockchainServer, {:reset_tx_count, 1})
    assert {:ok, _tx_id2} = Kylix.add_transaction("subject2", "predicate1", "object2", "agent2", "valid_sig")
    Process.sleep(10)
    {:ok, results} = Kylix.query({nil, "predicate1", nil})
    assert length(results) == 2
  end

  test "query transactions with validator rotation" do
    assert {:ok, _tx_id1} = Kylix.add_transaction("subject1", "predicate1", "object1", "agent1", "valid_sig")
    Process.sleep(10)
    :ok = GenServer.call(Kylix.BlockchainServer, {:reset_tx_count, 1})
    assert {:ok, _tx_id2} = Kylix.add_transaction("subject2", "predicate1", "object2", "agent2", "valid_sig")
    Process.sleep(10)
    {:ok, results} = Kylix.query({nil, "predicate1", nil})
    assert length(results) == 2
  end
end

defmodule Kylix.Eval.Fig1LineageSuiteTest do
  use ExUnit.Case

  alias Kylix.Query.SparqlEngine

  setup do
    :ok = Application.stop(:kylix)
    {:ok, _} = Application.ensure_all_started(:kylix)
    Kylix.Storage.DAGEngine.clear_all()
    :ok = Kylix.BlockchainServer.reset_tx_count(0)

    {:ok, %{private_key: private_key}} =
      GenServer.call(Kylix.BlockchainServer, :get_test_key_pair)

    assert {:ok, 15} = Kylix.Eval.Fig1.record(validator_id: "agent1", private_key: private_key)
    :ok
  end

  test "plot used the model" do
    assert_lineage("used")
  end

  test "fig1 was derived from the model in one hop" do
    assert_lineage("derived-from")
  end

  test "fig1 derivation chain binds model, cleaned-data, and raw-measurements" do
    assert_lineage("derived-from-chain")
  end

  test "fig1 is attributed to alice" do
    assert_lineage("attributed-to")
  end

  test "plot generated fig1" do
    assert_lineage("activity-outputs")
  end

  test "count of entities attributed to alice is 3" do
    assert_lineage("count")
  end

  defp assert_lineage(name) do
    query = Kylix.Eval.Suite.query(name)
    expected = Kylix.Eval.Suite.expected(name)

    assert {:ok, results} = SparqlEngine.execute(query)
    assert results == expected
  end
end

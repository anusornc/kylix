defmodule Kylix.Eval.PersistNotPublicTest do
  use ExUnit.Case

  alias Kylix.Query.SparqlEngine

  setup do
    Kylix.Test.App.restart()
  end

  test "accept counter is not a production reset" do
    refute function_exported?(Kylix.BlockchainServer, :reset_tx_count, 1)
  end

  test "in-memory persist is not cleared through a named storage process" do
    refute function_exported?(Kylix.Storage.DAGEngine, :clear_all, 0)
  end

  test "the facade has no public {s,p,o} query" do
    refute function_exported?(Kylix, :query, 1)
  end

  test "accept's process has no public {s,p,o} query" do
    refute function_exported?(Kylix.BlockchainServer, :query, 1)
  end

  test "Coordinator is not a caller-facing name" do
    refute Code.ensure_loaded?(Kylix.Storage.Coordinator)
  end

  test "the facade has no public persist function" do
    refute function_exported?(Kylix, :persist, 0)
    refute function_exported?(Kylix, :persist, 1)
  end

  test "fig1 generated-by matches at execute/1 after isolating persist by restart", %{} do
    {public_key, private_key} = Kylix.Test.Attester.generate_keys()
    attester = Kylix.Test.Attester.seed("fig1_restart_attester", public_key)

    assert {:ok, 15} = Kylix.Eval.Fig1.record(validator_id: attester, private_key: private_key)

    query = Kylix.Eval.Suite.query("generated-by")
    expected = Kylix.Eval.Suite.expected("generated-by")

    assert {:ok, results} = SparqlEngine.execute(query)
    assert results == expected
  end
end

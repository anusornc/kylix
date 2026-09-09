defmodule Kylix.Eval.Fig1LineageSuiteTest do
  use ExUnit.Case

  alias Kylix.Query.SparqlEngine

  setup do
    Kylix.Test.App.restart()

    {public_key, private_key} = Kylix.Test.Attester.generate_keys()
    attester = Kylix.Test.Attester.seed("fig1_attester", public_key)

    assert {:ok, 15} = Kylix.Eval.Fig1.record(validator_id: attester, private_key: private_key)
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

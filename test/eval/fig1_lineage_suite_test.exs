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
    query = """
    SELECT ?used
    WHERE { "activity:plot" prov:used ?used . }
    """

    assert {:ok, results} = SparqlEngine.execute(query)
    assert [%{"used" => "entity:model"}] = results
  end

  test "fig1 was derived from the model in one hop" do
    query = """
    SELECT ?source
    WHERE { "entity:fig1" prov:wasDerivedFrom ?source . }
    """

    assert {:ok, results} = SparqlEngine.execute(query)
    assert [%{"source" => "entity:model"}] = results
  end

  test "fig1 derivation chain binds model, cleaned-data, and raw-measurements" do
    query = """
    SELECT ?e1 ?e2 ?raw
    WHERE {
      "entity:fig1" prov:wasDerivedFrom ?e1 .
      ?e1 prov:wasDerivedFrom ?e2 .
      ?e2 prov:wasDerivedFrom ?raw .
    }
    """

    assert {:ok, results} = SparqlEngine.execute(query)

    assert [
             %{
               "e1" => "entity:model",
               "e2" => "entity:cleaned-data",
               "raw" => "entity:raw-measurements"
             }
           ] = results
  end

  test "fig1 is attributed to alice" do
    query = """
    SELECT ?agent
    WHERE { "entity:fig1" prov:wasAttributedTo ?agent . }
    """

    assert {:ok, results} = SparqlEngine.execute(query)
    assert [%{"agent" => "agent:alice"}] = results
  end

  test "plot generated fig1" do
    query = """
    SELECT ?entity
    WHERE { ?entity prov:wasGeneratedBy "activity:plot" . }
    """

    assert {:ok, results} = SparqlEngine.execute(query)
    assert [%{"entity" => "entity:fig1"}] = results
  end

  test "count of entities attributed to alice is 3" do
    query = """
    SELECT (COUNT(?entity) AS ?n)
    WHERE { ?entity prov:wasAttributedTo "agent:alice" . }
    """

    assert {:ok, results} = SparqlEngine.execute(query)
    assert [%{"n" => 3}] = results
  end
end

defmodule Kylix.Query.SparqlEngineTest do
  use ExUnit.Case
  alias Kylix.Storage.DAGEngine
  alias Kylix.Query.SparqlEngine

  setup do
    :ok = Application.stop(:kylix)
    {:ok, _} = Application.ensure_all_started(:kylix)
    setup_provo_test_data()
    :ok
  end

  describe "Basic query execution" do
    test "executes basic query" do
      query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
      {:ok, results} = SparqlEngine.execute(query)
      assert length(results) > 0
      result = hd(results)
      assert Map.has_key?(result, "s")
      assert Map.has_key?(result, "p")
      assert Map.has_key?(result, "o")
    end

    test "executes query with exact match for PROV-O wasGeneratedBy relationship" do
      query = """
      SELECT ?entity ?activity WHERE {
        "entity:document1" "prov:wasGeneratedBy" "activity:process1"
      }
      """
      {:ok, results} = SparqlEngine.execute(query)
      assert length(results) == 1
      result = hd(results)
      assert result["entity"] == "entity:document1"
      assert result["activity"] == "activity:process1"
    end

    test "executes query with partial match for PROV-O entity" do
      query = """
      SELECT ?activity WHERE {
        "entity:document1" "prov:wasGeneratedBy" ?activity
      }
      """
      {:ok, results} = SparqlEngine.execute(query)
      assert length(results) >= 1
      result = hd(results)
      assert result["activity"] == "activity:process1"
    end
  end

  describe "Query preprocessing and validation" do
    test "correctly preprocesses a query with comments and whitespace" do
      query = """
      # This is a comment
      SELECT  ?entity   ?activity /* Another comment */
      WHERE   {  ?entity  "prov:wasGeneratedBy"  ?activity  }
      """
      {:ok, results} = SparqlEngine.execute(query)
      assert is_list(results)
      assert length(results) > 0
    end

    test "correctly processes PREFIX declarations" do
      query = """
      PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
      PREFIX prov: <http://www.w3.org/ns/prov#>
      SELECT ?entity ?activity
      WHERE {
        ?entity prov:wasGeneratedBy ?activity
      }
      """
      {:ok, results} = SparqlEngine.execute(query)
      assert is_list(results)
      assert length(results) > 0
    end

    test "rejects queries with disallowed operations" do
      delete_query = """
      DELETE { ?s ?p ?o } WHERE { ?s ?p ?o }
      """
      result = SparqlEngine.execute(delete_query)
      assert match?({:error, _}, result)

      insert_query = """
      INSERT { ?s ?p ?o } WHERE { ?s ?p ?o }
      """
      result = SparqlEngine.execute(insert_query)
      assert match?({:error, _}, result)
    end
  end

  describe "PROV-O specific queries" do
    test "executes query for entities derived from other entities" do
      query = """
      SELECT ?derivedEntity ?sourceEntity
      WHERE {
        ?derivedEntity "prov:wasDerivedFrom" ?sourceEntity
      }
      """
      {:ok, results} = SparqlEngine.execute(query)
      assert length(results) > 0
      result = hd(results)
      assert result["derivedEntity"] == "entity:report1"
      assert result["sourceEntity"] == "entity:document1"
    end

    test "executes query for agent attribution" do
      query = """
      SELECT ?entity ?agent
      WHERE {
        ?entity "prov:wasAttributedTo" ?agent
      }
      """
      {:ok, results} = SparqlEngine.execute(query)
      assert length(results) > 0
      result = hd(results)
      assert result["entity"] == "entity:report1"
      assert result["agent"] == "agent:researcher1"
    end

    test "executes query with multiple PROV-O patterns" do
      query = """
      SELECT ?entity ?activity ?agent
      WHERE {
        ?entity "prov:wasGeneratedBy" ?activity .
        ?activity "prov:wasAssociatedWith" ?agent
      }
      """
      {:ok, results} = SparqlEngine.execute(query)
      assert length(results) > 0
      result = hd(results)
      assert result["entity"] != nil
      assert result["activity"] != nil
      assert result["agent"] != nil
    end
  end

  describe "Query with aggregation" do
    test "executes query with COUNT aggregation" do
      query = """
      SELECT (COUNT(?entity) AS ?entityCount)
      WHERE {
        ?entity "prov:wasGeneratedBy" ?activity
      }
      """
      {:ok, results} = SparqlEngine.execute(query)
      assert length(results) == 1
      result = hd(results)
      assert result["entityCount"] > 0
    end

    test "executes query with GROUP BY" do
      query = """
      SELECT ?agent (COUNT(?entity) AS ?entityCount)
      WHERE {
        ?entity "prov:wasAttributedTo" ?agent
      }
      GROUP BY ?agent
      """
      {:ok, results} = SparqlEngine.execute(query)
      assert length(results) > 0
      result = hd(results)
      assert result["agent"] != nil
      assert result["entityCount"] > 0
    end
  end

  describe "New API functions" do
    test "explain function provides query analysis" do
      query = "SELECT ?entity ?activity WHERE { ?entity \"prov:wasGeneratedBy\" ?activity }"
      {:ok, explanation} = SparqlEngine.explain(query)
      assert Map.has_key?(explanation, :original_query)
      assert Map.has_key?(explanation, :preprocessed_query)
      assert Map.has_key?(explanation, :parsed_structure)
      assert Map.has_key?(explanation, :optimized_structure)
    end

    test "example_queries returns a list of example queries" do
      examples = SparqlEngine.example_queries()
      assert is_list(examples)
      assert length(examples) > 0
      example = hd(examples)
      assert Map.has_key?(example, :name)
      assert Map.has_key?(example, :description)
      assert Map.has_key?(example, :query)
    end
  end

  defp setup_provo_test_data do
    if function_exported?(DAGEngine, :clear_all, 0) do
      DAGEngine.clear_all()
    end

    DAGEngine.add_node("prov1", %{
      subject: "entity:document1",
      predicate: "prov:wasGeneratedBy",
      object: "activity:process1",
      validator: "agent:validator1",
      timestamp: DateTime.utc_now()
    })

    DAGEngine.add_node("prov2", %{
      subject: "activity:process1",
      predicate: "prov:wasAssociatedWith",
      object: "agent:researcher1",
      validator: "agent:validator1",
      timestamp: DateTime.utc_now()
    })

    DAGEngine.add_node("prov3", %{
      subject: "entity:report1",
      predicate: "prov:wasDerivedFrom",
      object: "entity:document1",
      validator: "agent:validator1",
      timestamp: DateTime.utc_now()
    })

    DAGEngine.add_node("prov4", %{
      subject: "entity:report1",
      predicate: "prov:wasAttributedTo",
      object: "agent:researcher1",
      validator: "agent:validator1",
      timestamp: DateTime.utc_now()
    })

    DAGEngine.add_edge("prov1", "prov2", "process_association")
    DAGEngine.add_edge("prov3", "prov1", "derivation_source")
    DAGEngine.add_edge("prov4", "prov2", "attribution_agent")
  end
end

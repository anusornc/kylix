defmodule Kylix.Eval.LineageSubsetTest do
  use ExUnit.Case

  alias Kylix.Query.SparqlEngine

  setup do
    Kylix.Test.App.restart()

    {public_key, private_key} = Kylix.Test.Attester.generate_keys()
    attester = Kylix.Test.Attester.seed("fig1_attester", public_key)
    assert {:ok, 15} = Kylix.Eval.Fig1.record(validator_id: attester, private_key: private_key)
    :ok
  end

  test "execute rejects PREFIX" do
    query = """
    PREFIX prov: <http://www.w3.org/ns/prov#>
    SELECT ?activity
    WHERE { "entity:fig1" prov:wasGeneratedBy ?activity . }
    """

    assert_rejected(query)
  end

  test "execute rejects BASE" do
    query = """
    BASE <http://example.org/>
    SELECT ?activity
    WHERE { "entity:fig1" prov:wasGeneratedBy ?activity . }
    """

    assert_rejected(query)
  end

  @out_of_subset [
    {"CONSTRUCT", "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"},
    {"DESCRIBE", "DESCRIBE ?s WHERE { ?s ?p ?o }"},
    {"ASK", "ASK WHERE { ?s ?p ?o }"},
    {"OPTIONAL",
     "SELECT ?s WHERE { \"entity:fig1\" prov:wasGeneratedBy ?s . OPTIONAL { ?s prov:used ?x } }"},
    {"UNION",
     "SELECT ?s WHERE { { \"entity:fig1\" prov:wasGeneratedBy ?s } UNION { \"entity:fig1\" prov:used ?s } }"},
    {"FILTER",
     "SELECT ?s WHERE { ?s prov:wasAttributedTo \"agent:alice\" . FILTER(?s = \"entity:fig1\") }"},
    {"HAVING",
     "SELECT (COUNT(?e) AS ?n) WHERE { ?e prov:wasAttributedTo \"agent:alice\" } GROUP BY ?e HAVING (COUNT(?e) > 0)"},
    {"property path", "SELECT ?e WHERE { \"entity:fig1\" prov:wasDerivedFrom+ ?e }"},
    {"DELETE", "DELETE { ?s ?p ?o } WHERE { ?s ?p ?o }"},
    {"INSERT", "INSERT { ?s ?p ?o } WHERE { ?s ?p ?o }"},
    {"DROP", "DROP GRAPH <http://example.org/>"},
    {"LOAD", "LOAD <http://example.org/data>"},
    {"CLEAR", "CLEAR GRAPH <http://example.org/>"},
    {"ORDER BY", "SELECT ?s WHERE { \"entity:fig1\" prov:wasGeneratedBy ?s } ORDER BY ?s"},
    {"LIMIT", "SELECT ?s WHERE { \"entity:fig1\" prov:wasGeneratedBy ?s } LIMIT 1"},
    {"OFFSET", "SELECT ?s WHERE { \"entity:fig1\" prov:wasGeneratedBy ?s } OFFSET 1"},
    {"SUM", "SELECT (SUM(?e) AS ?n) WHERE { ?e prov:wasAttributedTo \"agent:alice\" }"},
    {"AVG", "SELECT (AVG(?e) AS ?n) WHERE { ?e prov:wasAttributedTo \"agent:alice\" }"},
    {"MIN", "SELECT (MIN(?e) AS ?n) WHERE { ?e prov:wasAttributedTo \"agent:alice\" }"},
    {"MAX", "SELECT (MAX(?e) AS ?n) WHERE { ?e prov:wasAttributedTo \"agent:alice\" }"},
    {"GROUP_CONCAT",
     "SELECT (GROUP_CONCAT(?e) AS ?n) WHERE { ?e prov:wasAttributedTo \"agent:alice\" }"},
    {"SELECT *", "SELECT * WHERE { \"entity:fig1\" prov:wasGeneratedBy ?s }"},
    {"DISTINCT", "SELECT DISTINCT ?s WHERE { \"entity:fig1\" prov:wasGeneratedBy ?s }"},
    {"COUNT DISTINCT",
     "SELECT (COUNT(DISTINCT ?entity) AS ?n) WHERE { ?entity prov:wasAttributedTo \"agent:alice\" }"}
  ]

  for {name, query} <- @out_of_subset do
    test "execute rejects #{name}" do
      assert_rejected(unquote(query))
    end
  end

  test "execute rejects lowercase order by" do
    assert_rejected("SELECT ?s WHERE { \"entity:fig1\" prov:wasGeneratedBy ?s } order by ?s")
  end

  test "execute does not rewrite a non-SELECT string into SELECT" do
    query = "?activity WHERE { \"entity:fig1\" prov:wasGeneratedBy ?activity . }"
    assert {:error, reason} = SparqlEngine.execute(query)
    assert reason =~ "not in the lineage suite"
  end

  test "execute still answers generated-by after Fig1 is recorded" do
    query = Kylix.Eval.Suite.query("generated-by")
    expected = Kylix.Eval.Suite.expected("generated-by")

    assert {:ok, results} = SparqlEngine.execute(query)
    assert results == expected
  end

  test "only execute/1 is public" do
    assert Code.ensure_loaded?(SparqlEngine)
    assert function_exported?(SparqlEngine, :execute, 1)
    refute function_exported?(SparqlEngine, :explain, 1)
    refute function_exported?(SparqlEngine, :example_queries, 0)
    refute function_exported?(SparqlEngine, :query_pattern, 1)
    refute function_exported?(SparqlEngine, :validate_sparql_query, 1)
    refute function_exported?(SparqlEngine, :parse_query_structure, 1)
    refute function_exported?(SparqlEngine, :preprocess_query, 1)
  end

  test "hop modules are not part of the lineage suite" do
    refute Code.ensure_loaded?(Kylix.Query.SparqlParser)
    refute Code.ensure_loaded?(Kylix.Query.SparqlOptimizer)
    refute Code.ensure_loaded?(Kylix.Query.SparqlExecutor)
    refute Code.ensure_loaded?(Kylix.Query.SparqlAggregator)
    refute Code.ensure_loaded?(Kylix.Query.VariableMapper)
  end

  defp assert_rejected(query) do
    assert {:error, reason} = SparqlEngine.execute(query)
    assert reason =~ "not in the lineage suite"
  end
end

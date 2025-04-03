defmodule Kylix.Query.SparqlParserTest do
  use ExUnit.Case
  alias Kylix.Query.SparqlParser

  describe "parse/1 with basic queries" do
    test "parses basic SELECT query" do
      query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"

      {:ok, parsed} = SparqlParser.parse(query)
      assert parsed.type == :select
      assert parsed.variables == ["s", "p", "o"]
      assert length(parsed.patterns) == 1

      pattern = hd(parsed.patterns)
      assert pattern.s == nil
      assert pattern.p == nil
      assert pattern.o == nil
    end

    test "parses query with explicit values" do
      query = "SELECT ?s ?p ?o WHERE { \"Alice\" ?p ?o }"

      {:ok, parsed} = SparqlParser.parse(query)
      pattern = hd(parsed.patterns)
      assert pattern.s == "Alice"
      assert pattern.p == nil
      assert pattern.o == nil
    end

    test "parses query with multiple patterns" do
      query = "SELECT ?s ?p ?o WHERE { ?s \"knows\" ?o . ?o \"lives\" \"Paris\" }"

      {:ok, parsed} = SparqlParser.parse(query)
      assert length(parsed.patterns) >= 1
      
      pattern = hd(parsed.patterns)
      assert pattern.p == "knows"

      has_lives_pattern = Enum.any?(parsed.patterns, fn p ->
        p.p == "lives" && p.o == "Paris"
      end)
      assert has_lives_pattern
    end
  end

  describe "parse/1 with FILTER clauses" do
    test "parses query with equality FILTER" do
      query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o . FILTER(?s = \"Alice\") }"

      {:ok, parsed} = SparqlParser.parse(query)
      assert length(parsed.filters) > 0

      filter = hd(parsed.filters)
      assert filter.type == :equality
      assert filter.variable == "s"
      assert filter.value == "Alice"
    end

    test "parses query with inequality FILTER" do
      query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o . FILTER(?s != \"Bob\") }"

      {:ok, parsed} = SparqlParser.parse(query)
      assert length(parsed.filters) > 0

      filter = hd(parsed.filters)
      assert filter.type == :inequality
      assert filter.variable == "s"
      assert filter.value == "Bob"
    end

    test "parses query with comparison FILTER" do
      query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o . FILTER(?o > \"10\") }"

      {:ok, parsed} = SparqlParser.parse(query)
      assert length(parsed.filters) > 0

      filter = hd(parsed.filters)
      assert filter.type == :greater_than
      assert filter.variable == "o"
      assert filter.value == "10"
    end
  end

  describe "parse/1 with OPTIONAL patterns" do
    test "parses query with OPTIONAL" do
      query = "SELECT ?s ?p ?o WHERE { ?s \"knows\" ?o . OPTIONAL { ?o \"email\" ?email } }"

      {:ok, parsed} = SparqlParser.parse(query)
      assert length(parsed.optionals) == 1

      optional = hd(parsed.optionals)
      assert length(optional.patterns) == 1

      pattern = hd(optional.patterns)
      assert pattern.p == "email"
    end

    test "parses query with nested OPTIONAL patterns" do
      query = """
      SELECT ?person ?friend ?email ?phone 
      WHERE { 
        ?person "knows" ?friend .
        OPTIONAL { 
          ?friend "email" ?email .
          OPTIONAL { ?friend "phone" ?phone }
        }
      }
      """

      {:ok, parsed} = SparqlParser.parse(query)
      assert length(parsed.optionals) == 1
      
      optional = hd(parsed.optionals)
      assert length(optional.patterns) == 1
      
      # Check the first pattern is about email
      pattern = hd(optional.patterns)
      assert pattern.p == "email"
      
      # Check we have nested optionals
      assert length(optional.optionals) == 1
      
      # Check the nested optional is about phone
      nested_optional = hd(optional.optionals)
      nested_pattern = hd(nested_optional.patterns)
      assert nested_pattern.p == "phone"
    end
  end

  describe "parse/1 with UNION patterns" do
    test "parses query with UNION" do
      query = """
      SELECT ?person ?target 
      WHERE {
        { ?person "knows" ?target } UNION { ?person "likes" ?target }
      }
      """

      {:ok, parsed} = SparqlParser.parse(query)
      assert length(parsed.unions) == 1

      union = hd(parsed.unions)
      assert map_size(union.left) > 0
      assert map_size(union.right) > 0
      
      # Check left side has patterns about 'knows'
      left_pattern = hd(union.left.patterns)
      assert left_pattern.p == "knows"
      
      # Check right side has patterns about 'likes'
      right_pattern = hd(union.right.patterns)
      assert right_pattern.p == "likes"
    end
  end

  describe "parse/1 with aggregation and grouping" do
    test "parses query with GROUP BY" do
      query = "SELECT ?s (COUNT(?o) AS ?count) WHERE { ?s ?p ?o } GROUP BY ?s"

      {:ok, parsed} = SparqlParser.parse(query)
      assert parsed.group_by == ["s"]
      assert parsed.has_aggregates == true
      
      # Check the aggregation function details
      agg = hd(parsed.aggregates)
      assert agg.function == :count
      assert agg.variable == "o"
      assert agg.alias == "count"
    end

    test "parses query with multiple aggregate functions" do
      query = """
      SELECT ?person (COUNT(?friend) AS ?friendCount) (MAX(?age) AS ?maxAge)
      WHERE { 
        ?person "knows" ?friend .
        ?friend "age" ?age 
      } 
      GROUP BY ?person
      """

      {:ok, parsed} = SparqlParser.parse(query)
      assert parsed.group_by == ["person"]
      assert parsed.has_aggregates == true
      assert length(parsed.aggregates) == 2
      
      # Check for the COUNT aggregate
      count_agg = Enum.find(parsed.aggregates, fn a -> a.function == :count end)
      assert count_agg.variable == "friend"
      assert count_agg.alias == "friendCount"
      
      # Check for the MAX aggregate
      max_agg = Enum.find(parsed.aggregates, fn a -> a.function == :max end)
      assert max_agg.variable == "age"
      assert max_agg.alias == "maxAge"
    end
  end

  describe "parse/1 with ORDER BY, LIMIT, and OFFSET" do
    test "parses query with ORDER BY" do
      query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY ?s"

      {:ok, parsed} = SparqlParser.parse(query)
      assert length(parsed.order_by) == 1

      ordering = hd(parsed.order_by)
      assert ordering.variable == "s"
      assert ordering.direction == :asc
    end

    test "parses query with explicit ORDER BY direction" do
      query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } ORDER BY DESC(?s) ASC(?p)"

      {:ok, parsed} = SparqlParser.parse(query)
      assert length(parsed.order_by) == 2

      # Check DESC ordering
      desc_ordering = Enum.find(parsed.order_by, fn o -> o.direction == :desc end)
      assert desc_ordering.variable == "s"
      
      # Check ASC ordering
      asc_ordering = Enum.find(parsed.order_by, fn o -> o.direction == :asc && o.variable == "p" end)
      assert asc_ordering != nil
    end

    test "parses query with LIMIT and OFFSET" do
      query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10 OFFSET 5"

      {:ok, parsed} = SparqlParser.parse(query)
      assert parsed.limit == 10
      assert parsed.offset == 5
    end

    test "parses query with only LIMIT" do
      query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o } LIMIT 10"

      {:ok, parsed} = SparqlParser.parse(query)
      assert parsed.limit == 10
      assert parsed.offset == nil
    end
  end

  describe "parse/1 with literals and IRIs" do
    test "parses query with string literals" do
      query = "SELECT ?s ?p ?o WHERE { ?s ?p \"test string\" }"

      {:ok, parsed} = SparqlParser.parse(query)
      pattern = hd(parsed.patterns)
      assert pattern.o == "test string"
    end

    test "parses query with single-quoted literals" do
      query = "SELECT ?s ?p ?o WHERE { ?s ?p 'test string' }"

      {:ok, parsed} = SparqlParser.parse(query)
      pattern = hd(parsed.patterns)
      assert pattern.o == "test string"
    end

    test "parses query with escaped quotes in literals" do
      query = "SELECT ?s ?p ?o WHERE { ?s ?p \"test \\\"quoted\\\" string\" }"

      {:ok, parsed} = SparqlParser.parse(query)
      pattern = hd(parsed.patterns)
      assert pattern.o == "test \"quoted\" string"
    end

    test "parses query with IRIs" do
      query = "SELECT ?s ?p ?o WHERE { ?s ?p <http://example.org/resource> }"

      {:ok, parsed} = SparqlParser.parse(query)
      pattern = hd(parsed.patterns)
      assert pattern.o == "http://example.org/resource"
    end

    test "parses query with PROV-O prefixed terms" do
      query = "SELECT ?entity ?activity WHERE { ?entity prov:wasGeneratedBy ?activity }"

      {:ok, parsed} = SparqlParser.parse(query)
      pattern = hd(parsed.patterns)
      assert pattern.p == "prov:wasGeneratedBy"
    end
  end

  describe "parse/1 with error handling" do
    test "returns error for malformed query" do
      query = "SELECT ?s ?p WHERE { ?s ?p" # Missing closing brace

      {:error, error_message} = SparqlParser.parse(query)
      assert is_binary(error_message)
      assert String.contains?(error_message, "Parse error") ||
             String.contains?(error_message, "Failed to parse")
    end

    test "returns error for query with syntax error" do
      query = "SELEC ?s ?p ?o WHERE { ?s ?p ?o }" # Misspelled SELECT

      {:error, error_message} = SparqlParser.parse(query)
      assert is_binary(error_message)
      assert String.contains?(error_message, "error") ||
             String.contains?(error_message, "failed")
    end
  end
end
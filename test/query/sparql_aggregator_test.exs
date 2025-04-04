defmodule Kylix.Query.SparqlAggregatorTest do
  use ExUnit.Case
  alias Kylix.Query.SparqlAggregator

  describe "parse_aggregate_expression/1" do
    test "parses COUNT expression" do
      {:ok, result} = SparqlAggregator.parse_aggregate_expression("COUNT(?person)")
      assert result.function == :count
      assert result.variable == "person"
      assert result.distinct == false
      assert result.alias == "count_person"
    end

    test "parses COUNT with DISTINCT" do
      {:ok, result} = SparqlAggregator.parse_aggregate_expression("COUNT(DISTINCT ?person)")
      assert result.function == :count
      assert result.variable == "person"
      assert result.distinct == true
      assert result.alias == "count_distinct_person"
    end

    test "parses COUNT with alias" do
      {:ok, result} = SparqlAggregator.parse_aggregate_expression("COUNT(?person AS ?personCount)")
      assert result.function == :count
      assert result.variable == "person"
      assert result.alias == "personCount"
    end

    test "parses SUM expression" do
      {:ok, result} = SparqlAggregator.parse_aggregate_expression("SUM(?value)")
      assert result.function == :sum
      assert result.variable == "value"
      assert result.alias == "sum_value"
    end

    test "parses AVG expression" do
      {:ok, result} = SparqlAggregator.parse_aggregate_expression("AVG(?age)")
      assert result.function == :avg
      assert result.variable == "age"
      assert result.alias == "avg_age"
    end

    test "parses MIN expression" do
      {:ok, result} = SparqlAggregator.parse_aggregate_expression("MIN(?date)")
      assert result.function == :min
      assert result.variable == "date"
      assert result.alias == "min_date"
    end

    test "parses MAX expression" do
      {:ok, result} = SparqlAggregator.parse_aggregate_expression("MAX(?score)")
      assert result.function == :max
      assert result.variable == "score"
      assert result.alias == "max_score"
    end

    test "parses GROUP_CONCAT expression" do
      {:ok, result} = SparqlAggregator.parse_aggregate_expression("GROUP_CONCAT(?name)")
      assert result.function == :group_concat
      assert result.variable == "name"
      assert result.options.separator == ","
      assert result.alias == "group_concat_name"
    end

    test "parses GROUP_CONCAT with separator" do
      {:ok, result} = SparqlAggregator.parse_aggregate_expression("GROUP_CONCAT(?name SEPARATOR \"; \")")
      assert result.function == :group_concat
      assert result.variable == "name"
      assert result.options.separator == "; "
      assert result.alias == "group_concat_name"
    end

    test "parses GROUP_CONCAT with separator and alias" do
      {:ok, result} = SparqlAggregator.parse_aggregate_expression("GROUP_CONCAT(?name SEPARATOR \", \" AS ?nameList)")
      assert result.function == :group_concat
      assert result.variable == "name"
      assert result.options.separator == ", "
      assert result.alias == "nameList"
    end

    test "handles expressions with whitespace variations" do
      {:ok, result} = SparqlAggregator.parse_aggregate_expression("COUNT ( DISTINCT ?person )")
      assert result.function == :count
      assert result.variable == "person"
      assert result.distinct == true
    end

    test "returns error for invalid expressions" do
      result = SparqlAggregator.parse_aggregate_expression("INVALID(?person)")
      assert match?({:error, _}, result)
    end
  end

  describe "apply_aggregations/4" do
    test "applies COUNT aggregation" do
      # Sample data
      results = [
        %{"s" => "Alice", "p" => "knows", "o" => "Bob"},
        %{"s" => "Alice", "p" => "knows", "o" => "Charlie"},
        %{"s" => "Bob", "p" => "knows", "o" => "Dave"}
      ]

      # COUNT aggregation
      count_agg = %{
        function: :count,
        variable: "o",
        distinct: false,
        alias: "friendCount"
      }

      # Apply aggregation
      [aggregated] = SparqlAggregator.apply_aggregations(results, [count_agg])

      # Verify result
      assert aggregated["friendCount"] == 3
    end

    test "applies COUNT DISTINCT aggregation" do
      # Sample data with duplicate values
      results = [
        %{"s" => "Alice", "p" => "likes", "o" => "Pizza"},
        %{"s" => "Bob", "p" => "likes", "o" => "Pizza"},
        %{"s" => "Charlie", "p" => "likes", "o" => "Pasta"}
      ]

      # COUNT DISTINCT aggregation
      count_distinct_agg = %{
        function: :count,
        variable: "o",
        distinct: true,
        alias: "uniqueItems"
      }

      # Apply aggregation
      [aggregated] = SparqlAggregator.apply_aggregations(results, [count_distinct_agg])

      # Verify result - should count unique values
      assert aggregated["uniqueItems"] == 2  # Pizza, Pasta
    end

    test "applies GROUP BY with COUNT" do
      # Sample data
      results = [
        %{"s" => "Alice", "p" => "knows", "o" => "Bob"},
        %{"s" => "Alice", "p" => "knows", "o" => "Charlie"},
        %{"s" => "Bob", "p" => "knows", "o" => "Dave"}
      ]

      # COUNT aggregation
      count_agg = %{
        function: :count,
        variable: "o",
        distinct: false,
        alias: "friendCount"
      }

      # Apply aggregation with GROUP BY
      aggregated = SparqlAggregator.apply_aggregations(results, [count_agg], ["s"])

      # Verify grouping
      assert length(aggregated) == 2  # Two groups: Alice, Bob

      # Find Alice's group
      alice_group = Enum.find(aggregated, fn group -> group["s"] == "Alice" end)
      assert alice_group["friendCount"] == 2  # Alice knows 2 people

      # Find Bob's group
      bob_group = Enum.find(aggregated, fn group -> group["s"] == "Bob" end)
      assert bob_group["friendCount"] == 1  # Bob knows 1 person
    end

    test "applies multiple aggregations" do
      # Sample data
      results = [
        %{"person" => "Alice", "age" => "25"},
        %{"person" => "Bob", "age" => "30"},
        %{"person" => "Charlie", "age" => "22"}
      ]

      # Multiple aggregations
      aggregations = [
        %{function: :count, variable: "person", distinct: false, alias: "personCount"},
        %{function: :avg, variable: "age", distinct: false, alias: "avgAge"},
        %{function: :min, variable: "age", distinct: false, alias: "minAge"},
        %{function: :max, variable: "age", distinct: false, alias: "maxAge"}
      ]

      # Apply aggregations
      [aggregated] = SparqlAggregator.apply_aggregations(results, aggregations)

      # Verify results
      assert aggregated["personCount"] == 3
      assert aggregated["avgAge"] == 25.666666666666668
      assert aggregated["minAge"] == "22"
      assert aggregated["maxAge"] == "30"
    end

    test "handles empty result sets" do
      # Empty results
      results = []

      # Aggregation
      count_agg = %{
        function: :count,
        variable: "o",
        distinct: false,
        alias: "count"
      }

      # Apply aggregation
      [aggregated] = SparqlAggregator.apply_aggregations(results, [count_agg])

      # Verify result
      assert aggregated["count"] == 0
    end
  end

  describe "enhance_query_with_aggregates/2" do
    test "extracts aggregates from SELECT clause" do
      # Sample query structure
      query_structure = %{
        type: :select,
        variables: ["person", "friendCount"],
        patterns: [%{s: nil, p: "knows", o: nil}]
      }

      # SELECT clause with aggregates
      select_clause = "SELECT ?person (COUNT(?friend) AS ?friendCount) WHERE { ?person knows ?friend } GROUP BY ?person"

      # Enhance query structure
      enhanced = SparqlAggregator.enhance_query_with_aggregates(query_structure, select_clause)

      # Verify result
      assert enhanced.has_aggregates == true
      assert length(enhanced.aggregates) == 1
      assert enhanced.group_by == ["person"]

      # Check the aggregate details
      [agg] = enhanced.aggregates
      assert agg.function == :count
      assert agg.variable == "friend"
      assert agg.alias == "friendCount"
    end

    test "handles multiple aggregates in SELECT clause" do
      query_structure = %{
        type: :select,
        variables: ["person", "friendCount", "avgAge"]
      }

      select_clause = "SELECT ?person (COUNT(?friend) AS ?friendCount) (AVG(?age) AS ?avgAge) WHERE { ... } GROUP BY ?person"

      enhanced = SparqlAggregator.enhance_query_with_aggregates(query_structure, select_clause)

      assert length(enhanced.aggregates) == 2
      assert Enum.any?(enhanced.aggregates, fn agg -> agg.function == :count && agg.alias == "friendCount" end)
      assert Enum.any?(enhanced.aggregates, fn agg -> agg.function == :avg && agg.alias == "avgAge" end)
    end

    test "handles queries without aggregates" do
      query_structure = %{
        type: :select,
        variables: ["s", "p", "o"]
      }

      select_clause = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"

      enhanced = SparqlAggregator.enhance_query_with_aggregates(query_structure, select_clause)

      assert enhanced.has_aggregates == false
      assert enhanced.aggregates == []
      assert enhanced.group_by == []
    end
  end

  describe "compute_aggregate/2" do
    test "computes COUNT correctly" do
      results = [%{"person" => "Alice"}, %{"person" => "Bob"}, %{"person" => "Charlie"}]
      aggregate = %{function: :count, variable: "person", distinct: false}

      assert SparqlAggregator.compute_aggregate(aggregate, results) == 3
    end

    test "computes SUM correctly" do
      results = [%{"value" => "10"}, %{"value" => "20"}, %{"value" => "30"}]
      aggregate = %{function: :sum, variable: "value", distinct: false}

      assert SparqlAggregator.compute_aggregate(aggregate, results) == 60
    end

    test "computes AVG correctly" do
      results = [%{"score" => "10"}, %{"score" => "20"}, %{"score" => "30"}]
      aggregate = %{function: :avg, variable: "score", distinct: false}

      assert SparqlAggregator.compute_aggregate(aggregate, results) == 20.0
    end

    test "computes MIN correctly" do
      results = [%{"temp" => "15"}, %{"temp" => "10"}, %{"temp" => "25"}]
      aggregate = %{function: :min, variable: "temp", distinct: false}

      assert SparqlAggregator.compute_aggregate(aggregate, results) == "10"
    end

    test "computes MAX correctly" do
      results = [%{"temp" => "15"}, %{"temp" => "10"}, %{"temp" => "25"}]
      aggregate = %{function: :max, variable: "temp", distinct: false}

      assert SparqlAggregator.compute_aggregate(aggregate, results) == "25"
    end

    test "computes GROUP_CONCAT correctly" do
      results = [%{"name" => "Alice"}, %{"name" => "Bob"}, %{"name" => "Charlie"}]
      aggregate = %{function: :group_concat, variable: "name", distinct: false, options: %{separator: ", "}}

      assert SparqlAggregator.compute_aggregate(aggregate, results) == "Alice, Bob, Charlie"
    end
  end
end

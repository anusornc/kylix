defmodule Kylix.Query.SparqlExecutorTest do
  use ExUnit.Case
  alias Kylix.Query.SparqlExecutor

  # This test file focuses on the parts of SparqlExecutor that can be tested
  # without depending on external modules or requiring complex mocking

  describe "parse_filter/1" do
    test "parses equality filter correctly" do
      filter_string = "entity = \"entity:document1\""
      {:ok, parsed} = SparqlExecutor.parse_filter(filter_string)

      assert Keyword.get(parsed, :variable) == "entity"
      assert Keyword.get(parsed, :operator) == :eq
      assert Keyword.get(parsed, :string) == "entity:document1"
    end

    test "parses inequality filter correctly" do
      filter_string = "timestamp != 30"
      {:ok, parsed} = SparqlExecutor.parse_filter(filter_string)

      assert Keyword.get(parsed, :variable) == "timestamp"
      assert Keyword.get(parsed, :operator) == :ne
      assert Keyword.get(parsed, :integer) == 30
    end

    test "parses greater than filter correctly" do
      filter_string = "version > 2"
      {:ok, parsed} = SparqlExecutor.parse_filter(filter_string)

      assert Keyword.get(parsed, :variable) == "version"
      assert Keyword.get(parsed, :operator) == :gt
      assert Keyword.get(parsed, :integer) == 2
    end

    test "parses less than filter correctly" do
      filter_string = "count < 10"
      {:ok, parsed} = SparqlExecutor.parse_filter(filter_string)

      assert Keyword.get(parsed, :variable) == "count"
      assert Keyword.get(parsed, :operator) == :lt
      assert Keyword.get(parsed, :integer) == 10
    end

    test "parses boolean filter correctly" do
      filter_string = "isValid = true"
      {:ok, parsed} = SparqlExecutor.parse_filter(filter_string)

      assert Keyword.get(parsed, :variable) == "isValid"
      assert Keyword.get(parsed, :operator) == :eq
      assert Keyword.get(parsed, :boolean) == true
    end

    test "returns error for invalid filter" do
      filter_string = "invalid filter syntax"
      result = SparqlExecutor.parse_filter(filter_string)

      assert match?({:error, _}, result)
    end
  end

  describe "construct_filter/1" do
    test "constructs equality filter" do
      parsed = [variable: "entity", operator: :eq, string: "entity:document1"]
      filter = SparqlExecutor.construct_filter(parsed)

      assert filter.type == :equality
      assert filter.variable == "entity"
      assert filter.value == "entity:document1"
      assert filter.expression == "entity eq \"entity:document1\""
    end

    test "constructs greater than filter" do
      parsed = [variable: "version", operator: :gt, integer: 2]
      filter = SparqlExecutor.construct_filter(parsed)

      assert filter.type == :greater_than
      assert filter.variable == "version"
      assert filter.value == 2
      assert filter.expression == "version gt 2"
    end

    test "constructs boolean filter" do
      parsed = [variable: "isValid", operator: :eq, boolean: true]
      filter = SparqlExecutor.construct_filter(parsed)

      assert filter.type == :equality
      assert filter.variable == "isValid"
      assert filter.value == true
      assert filter.expression == "isValid eq true"
    end
  end

  describe "apply_filter function" do
    test "applies equality filter correctly" do
      filter = %{
        type: :equality,
        variable: "entity",
        value: "entity:document1"
      }

      # Should match
      result1 = %{"entity" => "entity:document1"}
      # Should not match
      result2 = %{"entity" => "entity:document2"}

      # Get access to the private apply_filter function for testing
      # This captures the function from the module
      apply_filter = Function.capture(SparqlExecutor, :apply_filter, 2)

      assert apply_filter.(result1, filter) == true
      assert apply_filter.(result2, filter) == false
    end

    test "applies inequality filter correctly" do
      filter = %{
        type: :inequality,
        variable: "version",
        value: 1
      }

      # Should match (not equal)
      result1 = %{"version" => 2}
      # Should not match (equal)
      result2 = %{"version" => 1}

      apply_filter = Function.capture(SparqlExecutor, :apply_filter, 2)

      assert apply_filter.(result1, filter) == true
      assert apply_filter.(result2, filter) == false
    end

    test "applies greater_than filter correctly" do
      filter = %{
        type: :greater_than,
        variable: "count",
        value: 5
      }

      # Should match (greater)
      result1 = %{"count" => 10}
      # Should not match (not greater)
      result2 = %{"count" => 3}

      apply_filter = Function.capture(SparqlExecutor, :apply_filter, 2)

      assert apply_filter.(result1, filter) == true
      assert apply_filter.(result2, filter) == false
    end
  end

  describe "compare_values function" do
    test "compares nil values correctly" do
      compare_values = Function.capture(SparqlExecutor, :compare_values, 2)

      # nil and nil should be equal
      assert compare_values.(nil, nil) == 0

      # nil should come before non-nil values
      assert compare_values.(nil, "anything") < 0
      assert compare_values.("anything", nil) > 0
    end

    test "compares numbers correctly" do
      compare_values = Function.capture(SparqlExecutor, :compare_values, 2)

      assert compare_values.(5, 10) < 0  # 5 < 10
      assert compare_values.(10, 5) > 0  # 10 > 5
      assert compare_values.(5, 5) == 0  # 5 = 5
    end

    test "compares strings correctly" do
      compare_values = Function.capture(SparqlExecutor, :compare_values, 2)

      assert compare_values.("apple", "banana") < 0  # apple < banana
      assert compare_values.("banana", "apple") > 0  # banana > apple
      assert compare_values.("apple", "apple") == 0  # apple = apple
    end

    test "compares datetimes correctly" do
      compare_values = Function.capture(SparqlExecutor, :compare_values, 2)

      earlier = DateTime.new!(~D[2023-01-01], ~T[10:00:00], "Etc/UTC")
      later = DateTime.new!(~D[2023-01-02], ~T[10:00:00], "Etc/UTC")

      assert compare_values.(earlier, later) < 0  # earlier < later
      assert compare_values.(later, earlier) > 0  # later > earlier
      assert compare_values.(earlier, earlier) == 0  # same datetime
    end

    test "converts different types for comparison" do
      compare_values = Function.capture(SparqlExecutor, :compare_values, 2)

      # Different types are converted to strings
      assert compare_values.(5, "5") == compare_values.("5", "5")
    end
  end

  describe "merge_bindings function" do
    test "merges compatible bindings correctly" do
      merge_bindings = Function.capture(SparqlExecutor, :merge_bindings, 3)

      binding1 = %{"s" => "entity:document1", "p" => "prov:wasGeneratedBy"}
      binding2 = %{"o" => "activity:process1", "validator" => "agent:validator1"}
      pattern = %{s: nil, p: nil, o: nil}

      merged = merge_bindings.(binding1, binding2, pattern)

      assert merged["s"] == "entity:document1"
      assert merged["p"] == "prov:wasGeneratedBy"
      assert merged["o"] == "activity:process1"
      assert merged["validator"] == "agent:validator1"
    end

    test "returns nil for conflicting bindings" do
      merge_bindings = Function.capture(SparqlExecutor, :merge_bindings, 3)

      binding1 = %{"s" => "entity:document1"}
      binding2 = %{"s" => "entity:document2"}  # Conflict
      pattern = %{s: nil, p: nil, o: nil}

      merged = merge_bindings.(binding1, binding2, pattern)

      assert merged == nil  # Conflict results in nil
    end

    test "handles nil values in bindings" do
      merge_bindings = Function.capture(SparqlExecutor, :merge_bindings, 3)

      binding1 = %{"s" => "entity:document1", "p" => nil}
      binding2 = %{"p" => "prov:wasGeneratedBy", "o" => "activity:process1"}
      pattern = %{s: nil, p: nil, o: nil}

      merged = merge_bindings.(binding1, binding2, pattern)

      assert merged["s"] == "entity:document1"
      assert merged["p"] == "prov:wasGeneratedBy"  # nil is overwritten
      assert merged["o"] == "activity:process1"
    end

    test "respects variable names from pattern" do
      merge_bindings = Function.capture(SparqlExecutor, :merge_bindings, 3)

      binding1 = %{"s" => "entity:document1"}
      binding2 = %{"o" => "activity:process1"}
      pattern = %{s: :entity, p: nil, o: :activity}

      merged = merge_bindings.(binding1, binding2, pattern)

      # Values should be mapped to the pattern's variable names
      assert merged["entity"] == "entity:document1"
      assert merged["activity"] == "activity:process1"
    end
  end
end

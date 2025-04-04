defmodule Kylix.Query.VariableMapperTest do
  use ExUnit.Case
  alias Kylix.Query.VariableMapper

  describe "apply_mappings/2 with PROV-O entities" do
    test "maps entity and activity for wasGeneratedBy relationship" do
      entity_data = %{
        subject: "entity:document123",
        predicate: "prov:wasGeneratedBy",
        object: "activity:compilation85",
        validator: "agent:validator1",
        timestamp: DateTime.utc_now()
      }

      entity_result = VariableMapper.apply_mappings(%{}, entity_data)

      # Check specific PROV-O mappings
      assert entity_result["entity"] == "entity:document123"
      assert entity_result["activity"] == "activity:compilation85"

      # Check standard RDF mappings are also present
      assert entity_result["s"] == "entity:document123"
      assert entity_result["p"] == "prov:wasGeneratedBy"
      assert entity_result["o"] == "activity:compilation85"
    end

    test "maps entity and agent for wasAttributedTo relationship" do
      agent_data = %{
        subject: "entity:document123",
        predicate: "prov:wasAttributedTo",
        object: "agent:researcher42",
        validator: "agent:validator1",
        timestamp: DateTime.utc_now()
      }

      agent_result = VariableMapper.apply_mappings(%{}, agent_data)

      # Check specific PROV-O mappings
      assert agent_result["entity"] == "entity:document123"
      assert agent_result["agent"] == "agent:researcher42"

      # Check standard RDF mappings are also present
      assert agent_result["s"] == "entity:document123"
      assert agent_result["p"] == "prov:wasAttributedTo"
      assert agent_result["o"] == "agent:researcher42"
    end

    test "maps derivedEntity and sourceEntity for wasDerivedFrom relationship" do
      derivation_data = %{
        subject: "entity:report85",
        predicate: "prov:wasDerivedFrom",
        object: "entity:dataset42",
        validator: "agent:validator1",
        timestamp: DateTime.utc_now()
      }

      derivation_result = VariableMapper.apply_mappings(%{}, derivation_data)

      # Check specific PROV-O mappings
      assert derivation_result["derivedEntity"] == "entity:report85"
      assert derivation_result["sourceEntity"] == "entity:dataset42"

      # Check entity type is inferred for both subject and object
      assert derivation_result["entity"] == "entity:dataset42"

      # Check standard RDF mappings are also present
      assert derivation_result["s"] == "entity:report85"
      assert derivation_result["p"] == "prov:wasDerivedFrom"
      assert derivation_result["o"] == "entity:dataset42"
    end

    test "works with unprefixed predicates" do
      unprefixed_data = %{
        subject: "entity:document123",
        predicate: "wasGeneratedBy",
        object: "activity:compilation85",
        validator: "agent:validator1",
        timestamp: DateTime.utc_now()
      }

      unprefixed_result = VariableMapper.apply_mappings(%{}, unprefixed_data)

      # Should work the same as with prefixed versions
      assert unprefixed_result["entity"] == "entity:document123"
      assert unprefixed_result["activity"] == "activity:compilation85"
    end
  end

  describe "extract_variable_positions/2" do
    test "maps common PROV-O variables to positions" do
      prov_vars = ["entity", "activity", "agent", "wasGeneratedBy", "used", "sourceEntity", "derivedEntity"]
      prov_positions = VariableMapper.extract_variable_positions(prov_vars)

      assert prov_positions["entity"] == "s"
      assert prov_positions["activity"] == "s"
      assert prov_positions["agent"] == "s"
      assert prov_positions["derivedEntity"] == "s"
      assert prov_positions["sourceEntity"] == "s"

      # Predicate variables typically don't get mapped to positions
      refute Map.has_key?(prov_positions, "wasGeneratedBy")
      refute Map.has_key?(prov_positions, "used")
    end

    test "correctly maps standard variable names" do
      standard_vars = ["s", "p", "o", "subject", "predicate", "object", "person", "relation", "target"]
      positions = VariableMapper.extract_variable_positions(standard_vars)

      assert positions["s"] == "s"
      assert positions["p"] == "p"
      assert positions["o"] == "o"
      assert positions["subject"] == "s"
      assert positions["predicate"] == "p"
      assert positions["object"] == "o"
      assert positions["person"] == "s"
      assert positions["relation"] == "p"
      assert positions["target"] == "o"
    end

    test "respects provided explicit mappings" do
      vars = ["customVar1", "customVar2"]
      explicit_mappings = %{"customVar1" => "s", "customVar2" => "o"}

      positions = VariableMapper.extract_variable_positions(vars, explicit_mappings)

      assert positions["customVar1"] == "s"
      assert positions["customVar2"] == "o"
    end
  end

  describe "project_variables/3" do
    test "projects specific variables from binding" do
      prov_binding = %{
        "s" => "entity:report85",
        "p" => "prov:wasDerivedFrom",
        "o" => "entity:dataset42",
        "node_id" => "tx42",
        "count_derivation" => 3
      }

      prov_query_structure = %{
        variable_positions: %{"derivedEntity" => "s", "sourceEntity" => "o"}
      }

      prov_projection = VariableMapper.project_variables(
        prov_binding,
        ["derivedEntity", "sourceEntity", "count_derivation"],
        prov_query_structure
      )

      # Should only include the requested variables
      assert map_size(prov_projection) == 3
      assert prov_projection["derivedEntity"] == "entity:report85"
      assert prov_projection["sourceEntity"] == "entity:dataset42"
      assert prov_projection["count_derivation"] == 3
    end

    test "handles aggregate function results" do
      binding = %{
        "s" => "entity:person1",
        "count_friend" => 5,
        "relationCount" => 10
      }

      projection = VariableMapper.project_variables(
        binding,
        ["count", "count_friend", "relationCount"],
        %{}
      )

      assert projection["count_friend"] == 5
      assert projection["relationCount"] == 10

      # The 'count' variable should find one of the count variables
      # In our implementation, it prioritizes relationCount
      assert projection["count"] == 10
    end

    test "uses standard mappings as fallback" do
      binding = %{
        "s" => "entity:document1",
        "p" => "prov:wasGeneratedBy",
        "o" => "activity:process1"
      }

      projection = VariableMapper.project_variables(
        binding,
        ["subject", "object", "person", "target"],
        %{}
      )

      assert projection["subject"] == "entity:document1"
      assert projection["object"] == "activity:process1"
      assert projection["person"] == "entity:document1"
      assert projection["target"] == "activity:process1"
    end

    test "includes nil for missing variables" do
      binding = %{"s" => "entity:test"}

      projection = VariableMapper.project_variables(
        binding,
        ["s", "nonExistentVar"],
        %{}
      )

      assert projection["s"] == "entity:test"
      assert projection["nonExistentVar"] == nil
    end
  end

  describe "integration with PROV-O workflow" do
    test "maps a complete PROV-O workflow with multiple relationships" do
      # Setup a small provenance graph
      # dataset -> (wasGeneratedBy) -> process
      # process -> (wasAssociatedWith) -> researcher
      # report -> (wasDerivedFrom) -> dataset
      # report -> (wasAttributedTo) -> researcher

      # First relationship: dataset generation
      generation_data = %{
        subject: "entity:dataset1",
        predicate: "prov:wasGeneratedBy",
        object: "activity:process1",
        validator: "agent:validator1"
      }

      # Second relationship: process association
      association_data = %{
        subject: "activity:process1",
        predicate: "prov:wasAssociatedWith",
        object: "agent:researcher1",
        validator: "agent:validator1"
      }

      # Third relationship: report derivation
      derivation_data = %{
        subject: "entity:report1",
        predicate: "prov:wasDerivedFrom",
        object: "entity:dataset1",
        validator: "agent:validator1"
      }

      # Fourth relationship: report attribution
      attribution_data = %{
        subject: "entity:report1",
        predicate: "prov:wasAttributedTo",
        object: "agent:researcher1",
        validator: "agent:validator1"
      }

      # Apply mappings to all relationships
      generation_result = VariableMapper.apply_mappings(%{}, generation_data)
      association_result = VariableMapper.apply_mappings(%{}, association_data)
      derivation_result = VariableMapper.apply_mappings(%{}, derivation_data)
      attribution_result = VariableMapper.apply_mappings(%{}, attribution_data)

      # Verify the graph structure through mappings
      assert generation_result["entity"] == "entity:dataset1"
      assert generation_result["activity"] == "activity:process1"

      assert association_result["activity"] == "activity:process1"
      assert association_result["agent"] == "agent:researcher1"

      assert derivation_result["derivedEntity"] == "entity:report1"
      assert derivation_result["sourceEntity"] == "entity:dataset1"

      assert attribution_result["entity"] == "entity:report1"
      assert attribution_result["agent"] == "agent:researcher1"

      # The graph connections are preserved through the entity IDs
      assert generation_result["entity"] == derivation_result["sourceEntity"]
      assert generation_result["activity"] == association_result["activity"]
      assert association_result["agent"] == attribution_result["agent"]
      assert derivation_result["derivedEntity"] == attribution_result["entity"]
    end
  end
end

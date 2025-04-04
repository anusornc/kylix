defmodule Kylix.Query.VariableMapper do
  @moduledoc """
  Handles variable mapping for SPARQL queries based on RDF position and ontology knowledge.

  This module implements:
  - Standard triple pattern position-based mappings (subject, predicate, object)
  - PROV-O ontology specific variable mappings
  - SPARQL query result variable projection
  - Custom application-defined mappings
  """

  require Logger

  @default_mappings %{
    # Basic mappings for any triple
    "subject" => :subject,
    "predicate" => :predicate,
    "object" => :object,

    # Common query variable names
    "s" => :subject,
    "p" => :predicate,
    "o" => :object,

    # Legacy mappings
    "person" => :subject,
    "relation" => :predicate,
    "target" => :object,
    "friend" => :object
  }

  @prov_o_mappings %{
    # PROV-O specific predicate mappings - predicate name to role mappings
    "prov:wasGeneratedBy" => %{subject: "entity", object: "activity"},
    "wasGeneratedBy" => %{subject: "entity", object: "activity"},

    "prov:wasAttributedTo" => %{subject: "entity", object: "agent"},
    "wasAttributedTo" => %{subject: "entity", object: "agent"},

    "prov:wasDerivedFrom" => %{subject: "derivedEntity", object: "sourceEntity"},
    "wasDerivedFrom" => %{subject: "derivedEntity", object: "sourceEntity"},

    "prov:wasInformedBy" => %{subject: "informed", object: "informant"},
    "wasInformedBy" => %{subject: "informed", object: "informant"},

    "prov:actedOnBehalfOf" => %{subject: "delegate", object: "responsible"},
    "actedOnBehalfOf" => %{subject: "delegate", object: "responsible"},

    "prov:wasAssociatedWith" => %{subject: "activity", object: "agent"},
    "wasAssociatedWith" => %{subject: "activity", object: "agent"},

    "prov:used" => %{subject: "activity", object: "entity"},
    "used" => %{subject: "activity", object: "entity"},

    # Additional PROV-O relationships
    "prov:wasStartedBy" => %{subject: "activity", object: "entity"},
    "wasStartedBy" => %{subject: "activity", object: "entity"},

    "prov:wasEndedBy" => %{subject: "activity", object: "entity"},
    "wasEndedBy" => %{subject: "activity", object: "entity"},

    "prov:wasInvalidatedBy" => %{subject: "entity", object: "activity"},
    "wasInvalidatedBy" => %{subject: "entity", object: "activity"}
  }

  # Maps of entity types to their expected roles in PROV-O
  @prov_types %{
    # Types of entities in PROV-O
    "entity:" => ["entity", "derivedEntity", "sourceEntity", "plan"],
    "activity:" => ["activity", "generation", "invalidation", "usage"],
    "agent:" => ["agent", "delegate", "responsible"]
  }

  @doc """
  Apply variable mappings to a result based on RDF data.

  ## Parameters

  - `result` - The current result map to be enhanced with variable mappings
  - `data` - The source data containing subject, predicate, object, etc.

  ## Returns

  Enhanced result map with all appropriate variable mappings applied
  """
  def apply_mappings(result, data) do
    # Get application-specific mappings
    custom_mappings = Application.get_env(:kylix, :variable_mappings, %{})

    # Apply standard position-based mappings
    result = apply_position_mappings(result, data, Map.merge(@default_mappings, custom_mappings))

    # Apply ontology-aware mappings based on predicate
    result = apply_ontology_mappings(result, data)

    # Apply inferred type mappings
    apply_type_inferred_mappings(result, data)
  end

  @doc """
  Extract subject/predicate/object positions from a SPARQL query variable map.

  ## Parameters

  - `query_variables` - List of variable names from the query
  - `variable_mappings` - Optional explicit mapping of variables to positions

  ## Returns

  A map of variable names to their inferred positions (s, p, o)
  """
  def extract_variable_positions(query_variables, variable_mappings \\ %{}) do
    # Start with any explicit mappings
    Enum.reduce(query_variables, variable_mappings, fn var, acc ->
      cond do
        # Skip if already mapped
        Map.has_key?(acc, var) ->
          acc

        # Apply default mappings if they exist
        Map.has_key?(@default_mappings, var) ->
          position = @default_mappings[var]
          position_str = case position do
            :subject -> "s"
            :predicate -> "p"
            :object -> "o"
            _ -> nil
          end
          if position_str, do: Map.put(acc, var, position_str), else: acc

        # Try to infer from variable name
        var == "s" || var == "subject" || var == "person" || var == "entity" ||
        var == "activity" || var == "agent" || String.ends_with?(var, "Entity") ->
          Map.put(acc, var, "s")

        var == "p" || var == "predicate" || var == "relation" ->
          Map.put(acc, var, "p")

        var == "o" || var == "object" || var == "target" || var == "friend" ||
        var == "value" || var == "result" ->
          Map.put(acc, var, "o")

        # Otherwise leave unmapped
        true ->
          acc
      end
    end)
  end

  @doc """
  Project specific variables from a result according to the SPARQL query structure.

  ## Parameters

  - `binding` - The current binding (result) containing all available variables
  - `variables` - The list of variables requested in the SELECT clause
  - `query_structure` - The parsed SPARQL query structure

  ## Returns

  A new map containing only the selected variables with appropriate mappings
  """
  def project_variables(binding, variables, query_structure) do
    # Get variable positions
    var_positions = Map.get(query_structure, :variable_positions, %{})

    # Extract special variable mappings from PROV-O
    prov_variables = extract_prov_variables(binding)

    # Create a projection with just the requested variables
    Enum.reduce(variables, %{}, fn var, proj ->
      cond do
        # First check if the variable exists directly in the binding
        Map.has_key?(binding, var) ->
          Map.put(proj, var, Map.get(binding, var))

        # Check if it's a PROV-O mapped variable
        Map.has_key?(prov_variables, var) ->
          Map.put(proj, var, Map.get(prov_variables, var))

        # Check for aggregate function result
        var == "count" || String.starts_with?(var, "count_") ->
          value = Map.get(binding, var) ||
                  Map.get(binding, "count_#{var}") ||
                  Map.get(binding, "relationCount") ||
                  Map.get(binding, "count_target") ||
                  Map.get(binding, "count_o")
          Map.put(proj, var, value)

        # Then check variable positions from query analysis
        Map.has_key?(var_positions, var) ->
          position = Map.get(var_positions, var)
          value = case position do
            "s" -> Map.get(binding, "s")
            "p" -> Map.get(binding, "p")
            "o" -> Map.get(binding, "o")
            _ -> nil
          end
          Map.put(proj, var, value)

        # Try standard mappings as fallback
        Map.has_key?(@default_mappings, var) ->
          position = @default_mappings[var]
          value = case position do
            :subject -> Map.get(binding, "s")
            :predicate -> Map.get(binding, "p")
            :object -> Map.get(binding, "o")
            _ -> nil
          end
          Map.put(proj, var, value)

        # Variable not found - include as nil
        true ->
          Map.put(proj, var, nil)
      end
    end)
  end

  # Apply position-based mappings (subject, predicate, object)
  defp apply_position_mappings(result, data, mappings) do
    Enum.reduce(mappings, result, fn {var_name, position}, acc ->
      value =
        case position do
          :subject -> Map.get(data, :subject)
          :predicate -> Map.get(data, :predicate)
          :object -> Map.get(data, :object)
          :timestamp -> Map.get(data, :timestamp)
          :validator -> Map.get(data, :validator)
          :hash -> Map.get(data, :hash)
          _ -> nil
        end

      if value, do: Map.put(acc, var_name, value), else: acc
    end)
  end

  # Apply ontology-aware mappings based on predicate
  defp apply_ontology_mappings(result, data) do
    predicate = Map.get(data, :predicate)
    if is_nil(predicate), do:
      result,
    else:
      do_apply_predicate_mappings(result, data, predicate)
  end

  defp do_apply_predicate_mappings(result, data, predicate) do
    # Get predicate-specific mappings if they exist
    case Map.get(@prov_o_mappings, predicate) do
      nil ->
        # Try with prov: prefix if it doesn't have one
        if !String.contains?(predicate, ":") do
          case Map.get(@prov_o_mappings, "prov:#{predicate}") do
            nil -> result  # No mapping found
            mappings -> apply_ontology_role_mappings(result, data, mappings)
          end
        else
          result  # No mapping found for this predicate
        end

      mappings ->
        # Apply the ontology-specific mappings
        apply_ontology_role_mappings(result, data, mappings)
    end
  end

  # Apply role mappings from ontology definition
  defp apply_ontology_role_mappings(result, data, mappings) do
    result =
      if Map.has_key?(mappings, :subject) do
        Map.put(result, mappings.subject, Map.get(data, :subject))
      else
        result
      end

    result =
      if Map.has_key?(mappings, :object) do
        Map.put(result, mappings.object, Map.get(data, :object))
      else
        result
      end

    result
  end

  # Apply mappings inferred from entity/activity/agent types in identifiers
  defp apply_type_inferred_mappings(result, data) do
    subject = Map.get(data, :subject)
    object = Map.get(data, :object)

    # Try to infer entity types from prefixes
    result = infer_type_from_id(result, subject, "s", true)
    infer_type_from_id(result, object, "o", false)
  end

  # Try to infer entity type from ID prefix and set appropriate variables
  defp infer_type_from_id(result, id, _position, _is_subject) do
    if is_nil(id) or !is_binary(id), do:
      result,
    else:
      Enum.reduce(@prov_types, result, fn {prefix, roles}, acc ->
        if String.starts_with?(id, prefix) do
          # Use the primary role for this entity type regardless of position
          # In future we could differentiate, but for now use the primary role
          role = Enum.at(roles, 0)
          # Add the role variable mapping if we found a matching type
          if role, do: Map.put(acc, role, id), else: acc
        else
          acc
        end
      end)
  end

  # Extract PROV-O specific variables from a binding
  defp extract_prov_variables(binding) do
    predicate = Map.get(binding, "p")

    if is_nil(predicate) do
      %{}  # No predicate to extract mappings from
    else
      # Try to get mappings for this predicate
      case Map.get(@prov_o_mappings, predicate) do
        nil ->
          # Try with prov: prefix
          if !String.contains?(predicate, ":") do
            Map.get(@prov_o_mappings, "prov:#{predicate}", %{})
          else
            %{}  # No mapping found
          end

        mappings ->
          # Convert the role mappings to a map of variable -> value
          Enum.reduce(mappings, %{}, fn {pos, role}, acc ->
            value = case pos do
              :subject -> Map.get(binding, "s")
              :object -> Map.get(binding, "o")
              _ -> nil
            end

            if value, do: Map.put(acc, role, value), else: acc
          end)
      end
    end
  end
end

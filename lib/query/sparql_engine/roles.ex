defmodule Kylix.Query.SparqlEngine.Roles do
  @moduledoc false

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

    if is_nil(predicate),
      do: result,
      else: do_apply_predicate_mappings(result, data, predicate)
  end

  defp do_apply_predicate_mappings(result, data, predicate) do
    # Get predicate-specific mappings if they exist
    case Map.get(@prov_o_mappings, predicate) do
      nil ->
        # Try with prov: prefix if it doesn't have one
        if !String.contains?(predicate, ":") do
          case Map.get(@prov_o_mappings, "prov:#{predicate}") do
            # No mapping found
            nil -> result
            mappings -> apply_ontology_role_mappings(result, data, mappings)
          end
        else
          # No mapping found for this predicate
          result
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
    if is_nil(id) or !is_binary(id),
      do: result,
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
end

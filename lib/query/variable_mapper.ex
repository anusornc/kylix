defmodule Kylix.Query.VariableMapper do
  @moduledoc """
  Handles variable mapping for SPARQL queries based on RDF position and ontology knowledge.
  """

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
    # PROV-O specific predicate mappings
    "wasGeneratedBy" => %{subject: "entity", object: "activity"},
    "wasAttributedTo" => %{subject: "entity", object: "agent"},
    "wasDerivedFrom" => %{subject: "derivedEntity", object: "sourceEntity"},
    "wasInformedBy" => %{subject: "informed", object: "informant"},
    "actedOnBehalfOf" => %{subject: "delegate", object: "responsible"},
    "wasAssociatedWith" => %{subject: "activity", object: "agent"},
    "used" => %{subject: "activity", object: "entity"}
  }

  @doc """
  Apply variable mappings to a result based on RDF data.
  """
  def apply_mappings(result, data) do
    # Get application-specific mappings
    custom_mappings = Application.get_env(:kylix, :variable_mappings, %{})

    # Apply standard position-based mappings
    result = apply_position_mappings(result, data, Map.merge(@default_mappings, custom_mappings))

    # Apply ontology-aware mappings based on predicate
    apply_ontology_mappings(result, data)
  end

  # Apply position-based mappings (subject, predicate, object)
  defp apply_position_mappings(result, data, mappings) do
    Enum.reduce(mappings, result, fn {var_name, position}, acc ->
      value =
        case position do
          :subject -> data.subject
          :predicate -> data.predicate
          :object -> data.object
          :timestamp -> Map.get(data, :timestamp)
          :validator -> Map.get(data, :validator)
          _ -> nil
        end

      if value, do: Map.put(acc, var_name, value), else: acc
    end)
  end

  # Apply ontology-aware mappings
  defp apply_ontology_mappings(result, data) do
    predicate = data.predicate

    # Get predicate-specific mappings if they exist
    case Map.get(@prov_o_mappings, predicate) do
      nil ->
        # No specific ontology mappings for this predicate
        result

      mappings ->
        # Apply the ontology-specific mappings
        result =
          if Map.has_key?(mappings, :subject) do
            Map.put(result, mappings.subject, data.subject)
          else
            result
          end

        result =
          if Map.has_key?(mappings, :object) do
            Map.put(result, mappings.object, data.object)
          else
            result
          end

        result
    end
  end
end

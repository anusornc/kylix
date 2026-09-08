defmodule Kylix.Query.SparqlEngine.Count do
  @moduledoc false

  require Logger

  def apply_aggregations(results, aggregates, group_by \\ [], var_positions \\ %{}) do
    # Log inputs for debugging
    Logger.debug("Apply aggregations with:")
    Logger.debug("Results: #{inspect(results)}")
    Logger.debug("Aggregates: #{inspect(aggregates)}")
    Logger.debug("Group by: #{inspect(group_by)}")
    Logger.debug("Variable positions: #{inspect(var_positions)}")

    if Enum.empty?(aggregates) do
      # No aggregations to apply
      results
    else
      # Group results if needed
      grouped_results =
        if Enum.empty?(group_by) do
          # No GROUP BY, treat all results as one group
          [{"__all__", results}]
        else
          # Group by the specified variables
          groups =
            Enum.group_by(results, fn result ->
              Enum.map(group_by, fn var ->
                # Use variable positions to get the correct value
                position = Map.get(var_positions, var)

                value =
                  case position do
                    "s" -> Map.get(result, "s")
                    "p" -> Map.get(result, "p")
                    "o" -> Map.get(result, "o")
                    # Fall back to direct lookup
                    _ -> Map.get(result, var)
                  end

                # Additional fallback
                value || Map.get(result, var)
              end)
            end)

          # Debug grouped results
          Logger.debug("Grouped results: #{inspect(groups)}")
          Map.to_list(groups)
        end

      # Apply aggregations to each group
      aggregated =
        Enum.map(grouped_results, fn {group_key, group_results} ->
          # Start with a result containing the group_by values
          base_result =
            if group_key == "__all__" do
              %{}
            else
              if is_list(group_key) do
                Enum.zip(group_by, group_key)
                |> Enum.into(%{})
              else
                %{Enum.at(group_by, 0) => group_key}
              end
            end

          # Add each aggregate result to the base result
          Enum.reduce(aggregates, base_result, fn agg, result ->
            # Use variable position for the aggregate variable if available
            agg_variable = agg.variable
            position = Map.get(var_positions, agg_variable)

            # If we have a position mapping, update the aggregate to use the right field
            updated_agg =
              if position do
                field =
                  case position do
                    "s" -> "s"
                    "p" -> "p"
                    "o" -> "o"
                    _ -> agg_variable
                  end

                Map.put(agg, :field, field)
              else
                agg
              end

            agg_value = compute_aggregate(updated_agg, group_results)

            # Store the aggregate value both in the alias name and in a special key
            result
            |> Map.put(agg.alias, agg_value)
            |> Map.put("count_#{agg.variable}", agg_value)
          end)
        end)

      # Debug the final aggregated results
      Logger.debug("Aggregated results: #{inspect(aggregated)}")
      aggregated
    end
  end

  @doc """
  Computes a single aggregate function over a group of results.
  """
  def compute_aggregate(aggregate, results) do
    # Extract values for the variable from results
    # Use the field from variable positions if available
    field = Map.get(aggregate, :field, aggregate.variable)

    values =
      Enum.map(results, fn result ->
        # Try field first, then fall back to variable name
        Map.get(result, field) || Map.get(result, aggregate.variable)
      end)
      |> Enum.filter(&(&1 != nil))

    # Debug the values we're aggregating
    Logger.debug("Computing #{aggregate.function} on values: #{inspect(values)}")

    # Apply the appropriate aggregate function
    case aggregate.function do
      :count ->
        if Map.get(aggregate, :distinct, false) do
          count = values |> Enum.uniq() |> Enum.count()
          Logger.debug("COUNT(DISTINCT) = #{count}")
          count
        else
          count = Enum.count(values)
          Logger.debug("COUNT = #{count}")
          count
        end

      :sum ->
        # Convert values to numbers where possible
        numeric_values =
          Enum.map(values, &convert_to_number/1)
          |> Enum.filter(&(&1 != nil))

        Enum.sum(numeric_values)

      :avg ->
        numeric_values =
          Enum.map(values, &convert_to_number/1)
          |> Enum.filter(&(&1 != nil))

        if Enum.empty?(numeric_values) do
          nil
        else
          Enum.sum(numeric_values) / Enum.count(numeric_values)
        end

      :min ->
        if Enum.empty?(values) do
          nil
        else
          Enum.min_by(values, &sort_value/1, fn -> nil end)
        end

      :max ->
        if Enum.empty?(values) do
          nil
        else
          Enum.max_by(values, &sort_value/1, fn -> nil end)
        end

      :group_concat ->
        delimiter = Map.get(aggregate, :options, %{}) |> Map.get(:separator, ",")
        values |> Enum.join(delimiter)

      _ ->
        Logger.warning("Unsupported aggregate function: #{aggregate.function}")
        nil
    end
  end

  defp convert_to_number(value) when is_number(value), do: value

  defp convert_to_number(value) when is_binary(value) do
    case Integer.parse(value) do
      {int, ""} ->
        int

      _ ->
        case Float.parse(value) do
          {float, ""} -> float
          _ -> nil
        end
    end
  end

  defp convert_to_number(_), do: nil

  defp sort_value(value) when is_number(value), do: {:number, value}

  defp sort_value(value) when is_binary(value) do
    case Float.parse(value) do
      {num, ""} -> {:number, num}
      _ -> {:string, value}
    end
  end

  defp sort_value(%DateTime{} = dt), do: {:datetime, DateTime.to_unix(dt)}
  defp sort_value(nil), do: {nil, nil}
  defp sort_value(other), do: {:other, inspect(other)}
end

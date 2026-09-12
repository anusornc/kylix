defmodule Kylix.Query.SparqlEngine do
  @moduledoc """
  Lineage suite: ask provenance questions at execute/1.
  """

  alias Kylix.Query.SparqlEngine.Parse
  alias Kylix.Query.SparqlEngine.Join

  def execute(query) do
    try do
      query = query |> ensure_utf8_encoding() |> String.trim()

      case lineage_subset(query) do
        {:error, reason} ->
          {:error, reason}

        :ok ->
          case Parse.parse(query) do
            {:ok, parsed} -> Join.execute(parsed)
            {:error, reason} -> {:error, reason}
          end
      end
    rescue
      e ->
        {:error, "Query execution error: #{Exception.message(e)}"}
    end
  end

  defp lineage_subset(query) do
    cond do
      Regex.match?(~r/\bSELECT\s+\*/i, query) ->
        {:error, "SELECT * is not in the lineage suite"}

      match =
          Regex.run(
            ~r/\b(PREFIX|BASE|CONSTRUCT|DESCRIBE|ASK|OPTIONAL|UNION|FILTER|HAVING|DELETE|INSERT|DROP|LOAD|CLEAR|ORDER BY|LIMIT|OFFSET|SUM|AVG|MIN|MAX|GROUP_CONCAT|DISTINCT)\b/i,
            query
          ) ->
        {:error, "#{hd(match)} is not in the lineage suite"}

      Regex.match?(~r/\w+:\w+\s*[+*]|\w+:\w+\s*\/\s*\w+:/, query) ->
        {:error, "property paths are not in the lineage suite"}

      not Regex.match?(~r/\ASELECT\b/, query) ->
        {:error, "not in the lineage suite"}

      true ->
        :ok
    end
  end

  defp ensure_utf8_encoding(input) when is_binary(input) do
    case :unicode.characters_to_binary(input, :utf8, :utf8) do
      converted when is_binary(converted) ->
        String.replace(converted, ~r/[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]/, "")

      _ ->
        String.replace(input, ~r/[^\x20-\x7E\n\r\t]/, "")
    end
  end

  defp ensure_utf8_encoding(input), do: "#{input}"
end

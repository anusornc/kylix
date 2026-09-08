defmodule Kylix.Query.SparqlEngine do
  @moduledoc """
  Lineage suite: ask provenance questions at execute/1.
  """

  import NimbleParsec
  require Logger

  alias Kylix.Query.SparqlEngine.Parse
  alias Kylix.Query.SparqlEngine.Join

  # --- NimbleParsec Parser Definitions ---

  whitespace = ascii_string([?\s, ?\n, ?\r, ?\t], min: 1)
  optional_whitespace = ascii_string([?\s, ?\n, ?\r, ?\t], min: 0)

  single_line_comment =
    string("#")
    |> repeat(ascii_char(not: ?\n))
    |> optional(string("\n"))
    |> replace(:comment)

  multi_line_comment =
    string("/*")
    |> repeat(lookahead_not(string("*/")) |> ascii_char([]))
    |> string("*/")
    |> replace(:comment)

  any_comment = choice([single_line_comment, multi_line_comment])

  prefix_name =
    optional(ascii_string([?a..?z, ?A..?Z, ?0..?9, ?_], min: 1))
    |> string(":")
    |> reduce({Enum, :join, [""]})

  prefix_uri =
    ignore(string("<"))
    |> ascii_string([not: ?>], min: 1)
    |> ignore(string(">"))

  prefix_decl =
    ignore(optional_whitespace)
    |> ignore(string("PREFIX"))
    |> ignore(whitespace)
    |> concat(prefix_name)
    |> ignore(whitespace)
    |> concat(prefix_uri)
    |> ignore(optional_whitespace)
    |> tag(:prefix)

  base_decl =
    ignore(optional_whitespace)
    |> ignore(string("BASE"))
    |> ignore(whitespace)
    |> ignore(string("<"))
    |> ascii_string([not: ?>], min: 1)
    |> ignore(string(">"))
    |> ignore(optional_whitespace)
    |> tag(:base)

  select_keyword = ignore(optional_whitespace) |> string("SELECT") |> replace(:select)
  construct_keyword = ignore(optional_whitespace) |> string("CONSTRUCT") |> replace(:construct)
  describe_keyword = ignore(optional_whitespace) |> string("DESCRIBE") |> replace(:describe)
  ask_keyword = ignore(optional_whitespace) |> string("ASK") |> replace(:ask)
  query_type = choice([select_keyword, construct_keyword, describe_keyword, ask_keyword])

  query_structure_validator =
    optional(repeat(choice([prefix_decl, base_decl, any_comment, whitespace |> ignore()])))
    |> concat(query_type)
    |> repeat(
      choice([
        any_comment |> ignore(),
        whitespace |> ignore(),
        utf8_string([not: ?\s, not: ?\n, not: ?\r, not: ?\t], min: 1)
      ])
    )

  defparsecp(:parse_query_structure, query_structure_validator)

  query_preprocessor =
    repeat(
      choice([
        prefix_decl,
        base_decl,
        any_comment |> ignore(),
        whitespace |> replace(" "),
        choice([
          string("SELECT"),
          string("CONSTRUCT"),
          string("DESCRIBE"),
          string("ASK"),
          string("DELETE"),
          string("INSERT"),
          string("DROP"),
          string("LOAD"),
          string("CLEAR")
        ]),
        utf8_string([not: ?\s, not: ?\n, not: ?\r, not: ?\t], min: 1)
      ])
    )

  defparsecp(:preprocess_query, query_preprocessor)

  def execute(query) do
    try do
      normalized_query = query |> ensure_utf8_encoding() |> String.trim()
      Logger.debug("Executing SPARQL query: #{inspect(normalized_query)}")

      case lineage_subset(normalized_query) do
        {:error, reason} ->
          {:error, reason}

        :ok ->
          execute_lineage_query(normalized_query)
      end
    rescue
      e ->
        Logger.error("SPARQL execution error: #{Exception.message(e)}")
        detailed_error = Exception.format(:error, e, __STACKTRACE__)
        Logger.debug("Detailed error: #{detailed_error}")
        {:error, "Query execution error: #{Exception.message(e)}"}
    end
  end

  defp execute_lineage_query(normalized_query) do
    case preprocess_sparql_query(normalized_query) do
      {:ok, preprocessed_query, prefixes} ->
        case validate_sparql_query(preprocessed_query) do
          :ok ->
            Logger.debug("Parsing SPARQL query: #{preprocessed_query}")

            case Parse.parse(preprocessed_query) do
              {:ok, parsed_query} ->
                parsed_query = Map.put(parsed_query, :prefixes, prefixes)
                Logger.debug("Parsed query structure: #{inspect(parsed_query)}")
                result = Join.execute(parsed_query)
                Logger.debug("Query execution result: #{inspect(result)}")
                result

              {:error, reason} ->
                Logger.error("SPARQL parse error: #{reason}")
                {:error, reason}
            end

          {:error, reason} ->
            Logger.error("SPARQL query validation failed: #{reason}")
            {:error, reason}
        end

      {:error, reason} ->
        Logger.error("SPARQL query preprocessing failed: #{reason}")
        {:error, reason}
    end
  end

  defp lineage_subset(query) do
    cond do
      match =
          Regex.run(
            ~r/\b(PREFIX|BASE|CONSTRUCT|DESCRIBE|ASK|OPTIONAL|UNION|FILTER|HAVING|DELETE|INSERT|DROP|LOAD|CLEAR)\b/i,
            query
          ) ->
        {:error, "#{hd(match)} is not in the lineage suite"}

      Regex.match?(~r/\w+:\w+\s*[+*]|\w+:\w+\s*\/\s*\w+:/, query) ->
        {:error, "property paths are not in the lineage suite"}

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

  defp preprocess_sparql_query(query) do
    Logger.debug("Preprocess - Input: #{inspect(query)}")

    query =
      if !String.match?(query, ~r/^\s*(#.*\n\s*)*(?:PREFIX|BASE|SELECT|CONSTRUCT|DESCRIBE|ASK)/i) do
        "SELECT " <> query
      else
        query
      end

    Logger.debug("Preprocess - After initial check: #{inspect(query)}")

    case preprocess_query(query) do
      {:ok, tokens, "", _, _, _} ->
        Logger.debug("Preprocess - Tokens: #{inspect(tokens)}")

        {prefixes, other_tokens} =
          Enum.split_with(tokens, fn
            {:prefix, _} -> true
            {:base, _} -> true
            _ -> false
          end)

        Logger.debug("Preprocess - Prefixes: #{inspect(prefixes)}")
        Logger.debug("Preprocess - Other tokens: #{inspect(other_tokens)}")

        prefix_map =
          Enum.reduce(prefixes, %{}, fn
            {:prefix, [prefix, uri]}, acc -> Map.put(acc, prefix, uri)
            {:base, [uri]}, acc -> Map.put(acc, "BASE", uri)
            _, acc -> acc
          end)

        preprocessed =
          other_tokens
          |> List.flatten()
          |> Enum.join("")
          |> String.trim()
          |> String.replace(~r/\s+/, " ")

        Logger.debug("Preprocess - Preprocessed: #{inspect(preprocessed)}")

        prefix_strings =
          Enum.map(prefix_map, fn
            {"BASE", uri} -> "BASE <#{uri}> "
            {prefix, uri} -> "PREFIX #{prefix} <#{uri}> "
          end)
          |> Enum.join("")

        Logger.debug("Preprocess - Prefix strings: #{inspect(prefix_strings)}")
        # Only the query body
        final_query = preprocessed
        Logger.debug("Preprocess - Final query: #{inspect(final_query)}")
        {:ok, final_query, prefix_map}

      {:ok, _, rest, _, _, _} ->
        Logger.error("Preprocess - Failed with remaining: #{rest}")
        {:error, "Failed to preprocess entire query. Remaining: #{rest}"}
    end
  end

  defp validate_sparql_query(query) do
    cond do
      String.contains?(query, "DELETE") ->
        {:error, "DELETE operations are not allowed"}

      String.contains?(query, "INSERT") ->
        {:error, "INSERT operations are not allowed"}

      String.contains?(query, "DROP") ->
        {:error, "DROP operations are not allowed"}

      String.contains?(query, "LOAD") ->
        {:error, "LOAD operations are not allowed"}

      String.contains?(query, "CLEAR") ->
        {:error, "CLEAR operations are not allowed"}

      true ->
        if String.match?(
             query,
             ~r/^\s*(?:(?:PREFIX|BASE)\s+.*\s+)*(?:SELECT|CONSTRUCT|DESCRIBE|ASK)/i
           ) do
          :ok
        else
          {:error,
           "Query must start with SELECT, CONSTRUCT, DESCRIBE, or ASK (optionally preceded by PREFIX or BASE)"}
        end
    end
  end
end

defmodule Kylix.API.Router do
  use Plug.Router
  require Logger

  plug(Plug.Logger, log: :debug)
  plug(:match)
  plug(Plug.Parsers, parsers: [:json], pass: ["application/json"], json_decoder: Jason)
  plug(:dispatch)

  # GET /transactions - List transactions
  get "/transactions" do
    Logger.info("Listing transactions")

    case Kylix.Storage.Coordinator.query({nil, nil, nil}) do
      {:ok, results} ->
        formatted_results = format_transaction_results(results)
        send_json_resp(conn, 200, %{status: "success", data: formatted_results})

      {:error, reason} ->
        send_json_resp(conn, 500, %{status: "error", message: "Failed to fetch transactions: #{reason}"})
    end
  end

  # POST /transactions - Submit new transaction
  post "/transactions" do
    Logger.info("Submitting transaction: #{inspect(conn.body_params)}")

    with %{"subject" => subject,
           "predicate" => predicate,
           "object" => object,
           "validator_id" => validator_id,
           "signature" => signature} <- conn.body_params,
         {:ok, tx_id} <- Kylix.add_transaction(subject, predicate, object, validator_id, signature) do

      send_json_resp(conn, 201, %{status: "success", transaction_id: tx_id})
    else
      {:error, reason} ->
        send_json_resp(conn, 400, %{status: "error", message: "Transaction failed: #{reason}"})

      _ ->
        send_json_resp(conn, 400, %{
          status: "error",
          message: "Invalid parameters. Required: subject, predicate, object, validator_id, signature"
        })
    end
  end

  # GET /query - Execute SPARQL-like queries
  get "/query" do
    query_string = conn.params["q"]

    if query_string do
      Logger.info("Executing query: #{query_string}")

      case Kylix.Query.SparqlEngine.execute(query_string) do
        {:ok, results} ->
          send_json_resp(conn, 200, %{status: "success", data: results})

        {:error, reason} ->
          send_json_resp(conn, 400, %{status: "error", message: "Query failed: #{reason}"})
      end
    else
      send_json_resp(conn, 400, %{status: "error", message: "Missing required parameter 'q'"})
    end
  end

  # GET /validators - List validators
  get "/validators" do
    validators = Kylix.get_validators()
    send_json_resp(conn, 200, %{status: "success", data: validators})
  end

  # POST /validators - Add a new validator
  post "/validators" do
    with %{"validator_id" => validator_id,
           "pubkey" => pubkey,
           "known_by" => known_by} <- conn.body_params,
         {:ok, new_validator} <- Kylix.add_validator(validator_id, pubkey, known_by) do

      send_json_resp(conn, 201, %{status: "success", validator_id: new_validator})
    else
      {:error, reason} ->
        send_json_resp(conn, 400, %{status: "error", message: "Failed to add validator: #{reason}"})

      _ ->
        send_json_resp(conn, 400, %{
          status: "error",
          message: "Invalid parameters. Required: validator_id, pubkey, known_by"
        })
    end
  end

  # Route for simple web dashboard
  get "/" do
    conn
    |> put_resp_content_type("text/html")
    |> send_resp(200, Kylix.API.Dashboard.render())
  end

  # Pattern match for all unmatched routes
  match _ do
    send_json_resp(conn, 404, %{status: "error", message: "Route not found"})
  end

  # Helper to send JSON responses
  defp send_json_resp(conn, status, data) do
    conn
    |> put_resp_content_type("application/json")
    |> send_resp(status, Jason.encode!(data))
  end

  # Format transaction results for API output
  defp format_transaction_results(results) do
    Enum.map(results, fn {node_id, data, edges} ->
      %{
        id: node_id,
        subject: data.subject,
        predicate: data.predicate,
        object: data.object,
        validator: Map.get(data, :validator, nil),
        timestamp: format_datetime(Map.get(data, :timestamp, nil)),
        edges: format_edges(edges)
      }
    end)
  end

  defp format_datetime(%DateTime{} = dt), do: DateTime.to_iso8601(dt)
  defp format_datetime(_), do: nil

  defp format_edges(edges) do
    Enum.map(edges, fn
      {from, to, label} -> %{from: from, to: to, label: label}
      {to, label} -> %{to: to, label: label}
    end)
  end
end

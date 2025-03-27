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

  # GET /metrics - Fetch performance metrics
  get "/metrics" do
    Logger.info("Fetching performance metrics")

    # Get cache metrics from the Coordinator
    cache_metrics = Kylix.Storage.Coordinator.get_cache_metrics()

    # Get basic system metrics
    {:ok, results} = Kylix.Storage.Coordinator.query({nil, nil, nil})
    node_count = length(results)

    # Count all edges
    edge_count = Enum.reduce(results, 0, fn {_, _, edges}, acc ->
      acc + length(edges)
    end)

    # Load transaction speed benchmark results from file
    benchmark_data = load_benchmark_data()

    # Combine all metrics
    metrics = %{
      cache: %{
        hits: cache_metrics.cache_hits,
        misses: cache_metrics.cache_misses,
        size: cache_metrics.cache_size,
        hit_rate: cache_metrics.hit_rate_percent
      },
      query: %{
        avg_time: cache_metrics.avg_query_time_microseconds / 1000,  # Convert to milliseconds
        total_queries: cache_metrics.cache_hits + cache_metrics.cache_misses
      },
      storage: %{
        node_count: node_count,
        edge_count: edge_count
      },
      benchmarks: benchmark_data
    }

    send_json_resp(conn, 200, %{status: "success", data: metrics})
  end

  # POST /run-benchmark - Run a transaction speed test
  post "/run-benchmark" do
    Logger.info("Running transaction speed benchmark")

    # Get benchmark parameters from request body or use defaults
    params =
      case conn.body_params do
        %{"count" => count, "concurrent" => concurrent} ->
          [count: count, concurrent: concurrent]
        %{"count" => count} ->
          [count: count]
        %{"concurrent" => concurrent} ->
          [concurrent: concurrent]
        _ ->
          [] # Use defaults
      end

    # Run the benchmark
    try do
      # Assuming Kylix.TransactionSpeed.run exists
      benchmark_result = apply(Kylix.TransactionSpeed, :run, [params])

      # Return the result
      send_json_resp(conn, 200, %{
        status: "success",
        message: "Benchmark completed successfully",
        data: benchmark_result
      })
    rescue
      e ->
        Logger.error("Benchmark error: #{Exception.message(e)}")
        send_json_resp(conn, 500, %{
          status: "error",
          message: "Failed to run benchmark: #{Exception.message(e)}"
        })
    end
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

  # Load transaction benchmark data from JSON files
  defp load_benchmark_data do
    # Path to benchmark directory
    benchmark_dir = "data/benchmark"

    # Check if directory exists
    if File.exists?(benchmark_dir) && File.dir?(benchmark_dir) do
      # List all JSON files in the directory
      files =
        File.ls!(benchmark_dir)
        |> Enum.filter(&String.ends_with?(&1, ".json"))
        |> Enum.sort()
        |> Enum.reverse() # Get newest first
        |> Enum.take(5) # Take only the 5 most recent

      # Return empty if no files
      if files == [] do
        %{
          results: [],
          latest: nil
        }
      else
        # Read and parse the most recent file
        latest_file = hd(files)
        latest_path = Path.join(benchmark_dir, latest_file)
        latest_data =
          case File.read(latest_path) do
            {:ok, content} ->
              case Jason.decode(content) do
                {:ok, parsed} -> parsed
                _ -> %{}
              end
            _ -> %{}
          end

        # Read all files for time series data
        all_results =
          Enum.map(files, fn file ->
            path = Path.join(benchmark_dir, file)
            timestamp = extract_timestamp_from_filename(file)

            case File.read(path) do
              {:ok, content} ->
                case Jason.decode(content) do
                  {:ok, parsed} ->
                    Map.put(parsed, "timestamp", timestamp)
                  _ -> nil
                end
              _ -> nil
            end
          end)
          |> Enum.filter(&(&1 != nil))

        # Return structured data
        %{
          results: all_results,
          latest: latest_data
        }
      end
    else
      # Directory doesn't exist
      %{
        results: [],
        latest: nil
      }
    end
  end

  # Extract timestamp from filename like "benchmark_2023-05-25_12-30-45.json"
  defp extract_timestamp_from_filename(filename) do
    case Regex.run(~r/benchmark_(.+)\.json$/, filename) do
      [_, timestamp_str] ->
        # Convert to friendlier format if needed
        timestamp_str
        |> String.replace("_", " ")
        |> String.replace("-", ":")
      _ ->
        # If no match, use the filename without extension
        Path.rootname(filename)
    end
  end
end

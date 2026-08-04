defmodule Kylix.API.RouterTest do
  use ExUnit.Case, async: false
  import Plug.Test
  import Plug.Conn

  alias Kylix.API.Router

  @opts Router.init([])

  setup do
    # Modules to mock
    mocks = [
      Kylix,
      Kylix.Storage.Coordinator,
      Kylix.Query.SparqlEngine,
      Kylix.Benchmark.TransactionSpeed,
      Kylix.API.Dashboard
    ]

    # Setup mocks
    Enum.each(mocks, fn mod -> :meck.new(mod, [:passthrough]) end)

    # Ensure mocks are unloaded on exit
    on_exit(fn ->
      Enum.each(mocks, fn mod ->
        try do
          :meck.unload(mod)
        rescue
          _ -> :ok
        end
      end)
    end)

    :ok
  end

  describe "GET /transactions" do
    test "returns a formatted list of transactions on success" do
      # Set up mock data
      mock_node_id = "node123"
      mock_data = %{
        subject: "sub1",
        predicate: "pred1",
        object: "obj1",
        validator: "val1",
        timestamp: ~U[2023-01-01 12:00:00Z],
        hash: "somehash"
      }
      mock_edges = [{"from1", "to1", "label1"}, {"to2", "label2"}]

      :meck.expect(Kylix.Storage.Coordinator, :query, fn {nil, nil, nil} ->
        {:ok, [{mock_node_id, mock_data, mock_edges}]}
      end)

      conn = conn(:get, "/transactions")
      conn = Router.call(conn, @opts)

      assert conn.state == :sent
      assert conn.status == 200

      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "success"

      [tx] = response["data"]
      assert tx["id"] == mock_node_id
      assert tx["subject"] == mock_data.subject
      assert tx["predicate"] == mock_data.predicate
      assert tx["object"] == mock_data.object
      assert tx["validator"] == mock_data.validator
      assert tx["hash"] == mock_data.hash

      edges = tx["edges"]
      assert length(edges) == 2
      assert %{"from" => "from1", "to" => "to1", "label" => "label1"} in edges
      assert %{"to" => "to2", "label" => "label2"} in edges
    end

    test "generates hash when missing and formats datetime" do
      mock_data = %{
        subject: "sub1",
        predicate: "pred1",
        object: "obj1"
        # missing validator, timestamp, hash
      }

      :meck.expect(Kylix.Storage.Coordinator, :query, fn _ ->
        {:ok, [{"node1", mock_data, []}]}
      end)

      conn = conn(:get, "/transactions")
      conn = Router.call(conn, @opts)

      assert conn.status == 200
      response = Jason.decode!(conn.resp_body)

      [tx] = response["data"]
      assert tx["validator"] == nil
      assert tx["timestamp"] == nil
      assert is_binary(tx["hash"])
    end

    test "returns 500 when query fails" do
      :meck.expect(Kylix.Storage.Coordinator, :query, fn _ ->
        {:error, "database down"}
      end)

      conn = conn(:get, "/transactions")
      conn = Router.call(conn, @opts)

      assert conn.state == :sent
      assert conn.status == 500

      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "error"
      assert response["message"] == "Failed to fetch transactions: database down"
    end
  end

  describe "POST /transactions" do
    test "returns 201 on successful transaction creation" do
      payload = %{
        "subject" => "sub1",
        "predicate" => "pred1",
        "object" => "obj1",
        "validator_id" => "val1",
        "signature" => "sig1"
      }

      :meck.expect(Kylix, :add_transaction, fn "sub1", "pred1", "obj1", "val1", "sig1" ->
        {:ok, "tx_id_123"}
      end)

      conn = conn(:post, "/transactions", payload)
             |> put_req_header("content-type", "application/json")
      conn = Router.call(conn, @opts)

      assert conn.state == :sent
      assert conn.status == 201

      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "success"
      assert response["transaction_id"] == "tx_id_123"
    end

    test "returns 400 when transaction fails" do
      payload = %{
        "subject" => "sub1",
        "predicate" => "pred1",
        "object" => "obj1",
        "validator_id" => "val1",
        "signature" => "sig1"
      }

      :meck.expect(Kylix, :add_transaction, fn _, _, _, _, _ ->
        {:error, "invalid signature"}
      end)

      conn = conn(:post, "/transactions", payload)
             |> put_req_header("content-type", "application/json")
      conn = Router.call(conn, @opts)

      assert conn.status == 400
      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "error"
      assert response["message"] == "Transaction failed: invalid signature"
    end

    test "returns 400 when missing parameters" do
      payload = %{
        "subject" => "sub1",
        "predicate" => "pred1"
        # Missing other params
      }

      conn = conn(:post, "/transactions", payload)
             |> put_req_header("content-type", "application/json")
      conn = Router.call(conn, @opts)

      assert conn.status == 400
      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "error"
      assert response["message"] =~ "Invalid parameters"
    end
  end

  describe "GET /query" do
    test "returns results on successful query" do
      :meck.expect(Kylix.Query.SparqlEngine, :execute, fn "SELECT *" ->
        {:ok, [%{"var1" => "value1"}]}
      end)

      conn = conn(:get, "/query?q=SELECT%20*")
      conn = Router.call(conn, @opts)

      assert conn.state == :sent
      assert conn.status == 200

      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "success"
      assert response["data"] == [%{"var1" => "value1"}]
    end

    test "returns 400 on query error" do
      :meck.expect(Kylix.Query.SparqlEngine, :execute, fn "INVALID QUERY" ->
        {:error, "syntax error"}
      end)

      conn = conn(:get, "/query?q=INVALID%20QUERY")
      conn = Router.call(conn, @opts)

      assert conn.status == 400
      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "error"
      assert response["message"] == "Query failed: syntax error"
    end

    test "returns 400 when missing q parameter" do
      conn = conn(:get, "/query")
      conn = Router.call(conn, @opts)

      assert conn.status == 400
      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "error"
      assert response["message"] == "Missing required parameter 'q'"
    end
  end

  describe "GET /validators" do
    test "returns list of validators" do
      mock_validators = [%{"id" => "v1", "pubkey" => "pk1"}, %{"id" => "v2", "pubkey" => "pk2"}]
      :meck.expect(Kylix, :get_validators, fn -> mock_validators end)

      conn = conn(:get, "/validators")
      conn = Router.call(conn, @opts)

      assert conn.state == :sent
      assert conn.status == 200

      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "success"
      assert response["data"] == mock_validators
    end
  end

  describe "GET /metrics" do
    test "returns formatted system metrics" do
      # Mock cache metrics
      mock_cache = %{
        cache_hits: 1,
        cache_misses: 0,
        cache_size: 10,
        hit_rate_percent: 100,
        avg_query_time_microseconds: 100
      }
      :meck.expect(Kylix.Storage.Coordinator, :get_cache_metrics, fn -> mock_cache end)

      # Mock nodes/edges query
      # format: {node_id, data, edges}
      mock_results = [{"node1", %{subject: "a", predicate: "b", object: "c"}, [{1, 2, "label"}]}]
      :meck.expect(Kylix.Storage.Coordinator, :query, fn {nil, nil, nil} -> {:ok, mock_results} end)

      # NOTE: For benchmark data, load_benchmark_data/0 checks if "data/benchmark" exists,
      # since it doesn't in test, it defaults to %{results: [], latest: nil} gracefully.

      conn = conn(:get, "/metrics")
      conn = Router.call(conn, @opts)

      assert conn.state == :sent
      assert conn.status == 200

      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "success"

      metrics = response["data"]
      assert metrics["cache"]["hits"] == 1
      assert metrics["cache"]["misses"] == 0
      assert metrics["cache"]["size"] == 10
      assert metrics["cache"]["hit_rate"] == 100

      assert metrics["query"]["avg_time"] == 100 / 1000
      assert metrics["query"]["total_queries"] == 1

      assert metrics["storage"]["node_count"] == 1
      assert metrics["storage"]["edge_count"] == 1

      # Default empty map due to missing folder
      assert metrics["benchmarks"]["results"] == []
      assert metrics["benchmarks"]["latest"] == nil
    end
  end

  describe "POST /run-benchmark" do
    test "runs benchmark and returns formatted results" do
      mock_benchmark = %{
        timestamp: "2023-01-01T12:00:00Z",
        total_transactions: 1000,
        total_time_ms: 500.5,
        transactions_per_second: 2000.0,
        average_tx_time_us: 500.0,
        min_tx_time_us: 100.0,
        max_tx_time_us: 1000.0,
        percentiles: %{p50: 450.0, p95: 900.0}
      }

      :meck.expect(Kylix.Benchmark.TransactionSpeed, :run_baseline_test, fn 500 ->
        mock_benchmark
      end)

      payload = %{"count" => 500}
      conn = conn(:post, "/run-benchmark", payload)
             |> put_req_header("content-type", "application/json")
      conn = Router.call(conn, @opts)

      assert conn.state == :sent
      assert conn.status == 200

      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "success"
      assert response["message"] == "Benchmark completed successfully"

      data = response["data"]
      assert data["timestamp"] == mock_benchmark.timestamp
      assert data["transaction_count"] == mock_benchmark.total_transactions
      assert data["concurrent_connections"] == 1
      assert data["total_time_ms"] == mock_benchmark.total_time_ms
      assert data["transactions_per_second"] == mock_benchmark.transactions_per_second
      assert data["avg_latency_ms"] == mock_benchmark.average_tx_time_us / 1000

      percentiles = data["latency_percentiles"]
      assert percentiles["min"] == mock_benchmark.min_tx_time_us / 1000
      assert percentiles["p25"] == 0
      assert percentiles["p50"] == mock_benchmark.percentiles.p50 / 1000
      assert percentiles["p75"] == 0
      assert percentiles["p95"] == mock_benchmark.percentiles.p95 / 1000
      assert percentiles["max"] == mock_benchmark.max_tx_time_us / 1000
    end

    test "returns 500 on benchmark crash" do
      :meck.expect(Kylix.Benchmark.TransactionSpeed, :run_baseline_test, fn _ ->
        raise "benchmark failed"
      end)

      conn = conn(:post, "/run-benchmark")
      conn = Router.call(conn, @opts)

      assert conn.status == 500
      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "error"
      assert response["message"] == "Failed to run benchmark: benchmark failed"
    end
  end

  describe "POST /validators" do
    test "returns 201 when adding a new validator" do
      payload = %{
        "validator_id" => "v1",
        "pubkey" => "pk1",
        "known_by" => "me"
      }

      :meck.expect(Kylix, :add_validator, fn "v1", "pk1", "me" ->
        {:ok, "v1"}
      end)

      conn = conn(:post, "/validators", payload)
             |> put_req_header("content-type", "application/json")
      conn = Router.call(conn, @opts)

      assert conn.state == :sent
      assert conn.status == 201

      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "success"
      assert response["validator_id"] == "v1"
    end

    test "returns 400 when adding validator fails" do
      payload = %{
        "validator_id" => "v1",
        "pubkey" => "pk1",
        "known_by" => "me"
      }

      :meck.expect(Kylix, :add_validator, fn _, _, _ ->
        {:error, "duplicate validator"}
      end)

      conn = conn(:post, "/validators", payload)
             |> put_req_header("content-type", "application/json")
      conn = Router.call(conn, @opts)

      assert conn.status == 400
      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "error"
      assert response["message"] == "Failed to add validator: duplicate validator"
    end

    test "returns 400 when parameters are missing" do
      payload = %{
        "validator_id" => "v1"
        # missing pubkey and known_by
      }

      conn = conn(:post, "/validators", payload)
             |> put_req_header("content-type", "application/json")
      conn = Router.call(conn, @opts)

      assert conn.status == 400
      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "error"
      assert response["message"] =~ "Invalid parameters"
    end
  end

  describe "GET /validator-status" do
    test "returns validator status" do
      mock_status = %{"is_syncing" => false, "peers" => 3}
      :meck.expect(Kylix, :get_validator_status, fn -> mock_status end)

      conn = conn(:get, "/validator-status")
      conn = Router.call(conn, @opts)

      assert conn.state == :sent
      assert conn.status == 200

      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "success"
      assert response["data"] == mock_status
    end
  end

  describe "GET /validator-metrics" do
    test "returns validator metrics" do
      mock_metrics = %{"cpu_usage" => "10%", "memory" => "50MB"}
      :meck.expect(Kylix, :get_validator_metrics, fn -> mock_metrics end)

      conn = conn(:get, "/validator-metrics")
      conn = Router.call(conn, @opts)

      assert conn.state == :sent
      assert conn.status == 200

      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "success"
      assert response["data"] == mock_metrics
    end
  end

  describe "GET /" do
    test "returns dashboard html" do
      mock_html = "<html>Dashboard</html>"
      :meck.expect(Kylix.API.Dashboard, :render, fn -> mock_html end)

      conn = conn(:get, "/")
      conn = Router.call(conn, @opts)

      assert conn.state == :sent
      assert conn.status == 200

      assert get_resp_header(conn, "content-type") == ["text/html; charset=utf-8"]
      assert conn.resp_body == mock_html
    end
  end

  describe "unmatched routes" do
    test "returns 404 with error message" do
      conn = conn(:get, "/unknown-route")
      conn = Router.call(conn, @opts)

      assert conn.state == :sent
      assert conn.status == 404

      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "error"
      assert response["message"] == "Route not found"
    end
  end
end

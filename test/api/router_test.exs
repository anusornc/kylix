defmodule Kylix.API.RouterTest do
  use ExUnit.Case, async: false
  import Plug.Test
  import Plug.Conn

  @opts Kylix.API.Router.init([])

  setup do
    # For meck we need to ensure the module is compiled and ready
    :ok
  end

  test "GET / - returns html dashboard" do
    conn = conn(:get, "/")
    conn = Kylix.API.Router.call(conn, @opts)

    assert conn.state == :sent
    assert conn.status == 200
    assert get_resp_header(conn, "content-type") == ["text/html; charset=utf-8"]
    assert conn.resp_body =~ "Kylix"
  end

  test "GET /transactions - lists transactions successfully" do
    :meck.new(Kylix.Storage.Coordinator, [:passthrough])

    :meck.expect(Kylix.Storage.Coordinator, :query, fn {nil, nil, nil} ->
      {:ok, [{"node1", %{subject: "s1", predicate: "p1", object: "o1"}, []}]}
    end)

    conn = conn(:get, "/transactions")
    conn = Kylix.API.Router.call(conn, @opts)

    assert conn.state == :sent
    assert conn.status == 200
    assert %{"status" => "success", "data" => data} = Jason.decode!(conn.resp_body)
    assert length(data) == 1

    :meck.unload(Kylix.Storage.Coordinator)
  end

  test "POST /transactions - returns error on missing parameters" do
    conn =
      conn(:post, "/transactions", Jason.encode!(%{"subject" => "test"}))
      |> put_req_header("content-type", "application/json")

    conn = Kylix.API.Router.call(conn, @opts)

    assert conn.state == :sent
    assert conn.status == 400
    assert %{"status" => "error", "message" => msg} = Jason.decode!(conn.resp_body)

    assert msg =~
             "Invalid parameters. Required: subject, predicate, object, validator_id, signature"
  end

  test "POST /transactions - returns success on valid parameters" do
    :meck.new(Kylix, [:passthrough])

    :meck.expect(Kylix, :add_transaction, fn _sub, _pred, _obj, _val, _sig ->
      {:ok, "tx_123"}
    end)

    body = %{
      "subject" => "entity:doc2",
      "predicate" => "prov:wasGeneratedBy",
      "object" => "activity:process2",
      "validator_id" => "valid_validator",
      "signature" => "valid_sig"
    }

    conn =
      conn(:post, "/transactions", Jason.encode!(body))
      |> put_req_header("content-type", "application/json")

    conn = Kylix.API.Router.call(conn, @opts)

    assert conn.state == :sent
    assert conn.status == 201
    assert %{"status" => "success", "transaction_id" => "tx_123"} = Jason.decode!(conn.resp_body)

    :meck.unload(Kylix)
  end

  test "GET /query - executes query and returns results" do
    :meck.new(Kylix.Query.SparqlEngine, [:passthrough])

    :meck.expect(Kylix.Query.SparqlEngine, :execute, fn _query ->
      {:ok, [%{"s" => "result1"}]}
    end)

    query = "SELECT ?s ?p ?o WHERE { ?s ?p ?o }"
    conn = conn(:get, "/query?q=#{URI.encode(query)}")
    conn = Kylix.API.Router.call(conn, @opts)

    assert conn.state == :sent
    assert conn.status == 200
    assert %{"status" => "success", "data" => data} = Jason.decode!(conn.resp_body)
    assert length(data) == 1

    :meck.unload(Kylix.Query.SparqlEngine)
  end

  test "GET /query - returns error if q param is missing" do
    conn = conn(:get, "/query")
    conn = Kylix.API.Router.call(conn, @opts)

    assert conn.state == :sent
    assert conn.status == 400

    assert %{"status" => "error", "message" => "Missing required parameter 'q'"} =
             Jason.decode!(conn.resp_body)
  end

  test "GET /validators - lists validators" do
    :meck.new(Kylix, [:passthrough])

    :meck.expect(Kylix, :get_validators, fn ->
      %{"val1" => %{pubkey: "pk1"}}
    end)

    conn = conn(:get, "/validators")
    conn = Kylix.API.Router.call(conn, @opts)

    assert conn.state == :sent
    assert conn.status == 200
    assert %{"status" => "success", "data" => data} = Jason.decode!(conn.resp_body)
    assert Map.has_key?(data, "val1")

    :meck.unload(Kylix)
  end

  test "POST /validators - returns error on missing parameters" do
    conn =
      conn(:post, "/validators", Jason.encode!(%{"validator_id" => "new_val"}))
      |> put_req_header("content-type", "application/json")

    conn = Kylix.API.Router.call(conn, @opts)

    assert conn.state == :sent
    assert conn.status == 400
    assert %{"status" => "error", "message" => msg} = Jason.decode!(conn.resp_body)
    assert msg =~ "Invalid parameters. Required: validator_id, pubkey, known_by"
  end

  test "POST /validators - successfully adds a validator" do
    :meck.new(Kylix, [:passthrough])

    :meck.expect(Kylix, :add_validator, fn _id, _pub, _known ->
      {:ok, "new_val"}
    end)

    body = %{
      "validator_id" => "new_val",
      "pubkey" => "pk123",
      "known_by" => "existing_agent"
    }

    conn =
      conn(:post, "/validators", Jason.encode!(body))
      |> put_req_header("content-type", "application/json")

    conn = Kylix.API.Router.call(conn, @opts)

    assert conn.state == :sent
    assert conn.status == 201

    :meck.unload(Kylix)
  end

  test "GET /metrics - fetches performance metrics" do
    :meck.new(Kylix.Storage.Coordinator, [:passthrough])

    :meck.expect(Kylix.Storage.Coordinator, :get_cache_metrics, fn ->
      %{
        cache_hits: 10,
        cache_misses: 5,
        cache_size: 15,
        hit_rate_percent: 66.6,
        avg_query_time_microseconds: 1000
      }
    end)

    :meck.expect(Kylix.Storage.Coordinator, :query, fn {nil, nil, nil} ->
      {:ok, []}
    end)

    conn = conn(:get, "/metrics")
    conn = Kylix.API.Router.call(conn, @opts)

    assert conn.state == :sent
    assert conn.status == 200
    assert %{"status" => "success", "data" => data} = Jason.decode!(conn.resp_body)
    assert Map.has_key?(data, "cache")
    assert Map.has_key?(data, "query")
    assert Map.has_key?(data, "storage")
    assert Map.has_key?(data, "benchmarks")

    :meck.unload(Kylix.Storage.Coordinator)
  end

  test "GET /validator-status - gets coordination status" do
    :meck.new(Kylix, [:passthrough])

    :meck.expect(Kylix, :get_validator_status, fn ->
      %{current_validator: "val1"}
    end)

    conn = conn(:get, "/validator-status")
    conn = Kylix.API.Router.call(conn, @opts)

    assert conn.state == :sent
    assert conn.status == 200
    assert %{"status" => "success", "data" => _data} = Jason.decode!(conn.resp_body)

    :meck.unload(Kylix)
  end

  test "GET /validator-metrics - gets performance metrics" do
    :meck.new(Kylix, [:passthrough])

    :meck.expect(Kylix, :get_validator_metrics, fn ->
      %{"val1" => %{blocks_mined: 5}}
    end)

    conn = conn(:get, "/validator-metrics")
    conn = Kylix.API.Router.call(conn, @opts)

    assert conn.state == :sent
    assert conn.status == 200
    assert %{"status" => "success", "data" => _data} = Jason.decode!(conn.resp_body)

    :meck.unload(Kylix)
  end

  test "POST /run-benchmark - executes test" do
    :meck.new(Kylix.Benchmark.TransactionSpeed, [:passthrough])

    :meck.expect(Kylix.Benchmark.TransactionSpeed, :run_baseline_test, fn _count ->
      %{
        timestamp: "2023",
        total_transactions: 10,
        total_time_ms: 100,
        transactions_per_second: 100.0,
        average_tx_time_us: 1000.0,
        min_tx_time_us: 500,
        percentiles: %{p50: 1000, p95: 1500},
        max_tx_time_us: 2000
      }
    end)

    body = %{"count" => 10}

    conn =
      conn(:post, "/run-benchmark", Jason.encode!(body))
      |> put_req_header("content-type", "application/json")

    conn = Kylix.API.Router.call(conn, @opts)

    assert conn.state == :sent
    assert conn.status == 200

    assert %{"status" => "success", "data" => _data} = Jason.decode!(conn.resp_body)

    :meck.unload(Kylix.Benchmark.TransactionSpeed)
  end

  test "returns 404 for unmatched routes" do
    conn = conn(:get, "/a-route-that-does-not-exist")
    conn = Kylix.API.Router.call(conn, @opts)

    assert conn.state == :sent
    assert conn.status == 404
    assert %{"status" => "error", "message" => "Route not found"} = Jason.decode!(conn.resp_body)
  end
end

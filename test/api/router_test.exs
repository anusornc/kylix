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
      Kylix.Query.SparqlEngine,
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
    test "does not dump persist" do
      conn = conn(:get, "/transactions")
      conn = Router.call(conn, @opts)

      assert conn.state == :sent
      assert conn.status == 404

      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "error"
      assert response["message"] == "Route not found"
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

      conn =
        conn(:post, "/transactions", payload)
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

      conn =
        conn(:post, "/transactions", payload)
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

      conn =
        conn(:post, "/transactions", payload)
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
      query = ~s(SELECT ?activity WHERE { "entity:fig1" prov:wasGeneratedBy ?activity . })

      :meck.expect(Kylix.Query.SparqlEngine, :execute, fn ^query ->
        {:ok, [%{"activity" => "activity:plot"}]}
      end)

      conn = conn(:get, "/query?q=#{URI.encode(query)}")
      conn = Router.call(conn, @opts)

      assert conn.state == :sent
      assert conn.status == 200

      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "success"
      assert response["data"] == [%{"activity" => "activity:plot"}]
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
    test "is not a leftover benchmark dump" do
      conn = conn(:get, "/metrics")
      conn = Router.call(conn, @opts)

      assert conn.state == :sent
      assert conn.status == 404

      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "error"
      assert response["message"] == "Route not found"
    end
  end

  describe "POST /run-benchmark" do
    test "is not a TPS door" do
      conn = conn(:post, "/run-benchmark")
      conn = Router.call(conn, @opts)

      assert conn.state == :sent
      assert conn.status == 404

      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "error"
      assert response["message"] == "Route not found"
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

      conn =
        conn(:post, "/validators", payload)
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

      conn =
        conn(:post, "/validators", payload)
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

      conn =
        conn(:post, "/validators", payload)
        |> put_req_header("content-type", "application/json")

      conn = Router.call(conn, @opts)

      assert conn.status == 400
      response = Jason.decode!(conn.resp_body)
      assert response["status"] == "error"
      assert response["message"] =~ "Invalid parameters"
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

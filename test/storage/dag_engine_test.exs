defmodule Kylix.Storage.DAGEngineTest do
  use ExUnit.Case
  alias Kylix.Storage.DAGEngine

  setup do
    # Since the application already starts the DAGEngine, we don't try to start it again
    # Instead, we'll just make sure it's running

    # Ensure the DAGEngine is running
    pid = Process.whereis(DAGEngine)
    assert is_pid(pid) and Process.alive?(pid)

    # Stop the application to reset the state
    :ok = Application.stop(:kylix)
    # Start it again
    {:ok, _} = Application.ensure_all_started(:kylix)

    :ok
  end

  describe "node operations" do
    test "add_node adds a node to the graph" do
      node_id = "test_node_1"
      data = %{key: "value", number: 42}

      assert :ok = DAGEngine.add_node(node_id, data)
      assert {:ok, ^data} = DAGEngine.get_node(node_id)
    end

    test "add_node with non-map data returns error" do
      node_id = "test_node_invalid"
      invalid_data = "not a map"

      assert {:error, :invalid_data} = DAGEngine.add_node(node_id, invalid_data)
      assert :not_found = DAGEngine.get_node(node_id)
    end

    test "get_node returns :not_found for non-existent node" do
      assert :not_found = DAGEngine.get_node("non_existent_node")
    end

    test "get_all_nodes returns all nodes in the graph" do
      # Add several nodes
      DAGEngine.add_node("node1", %{data: "value1"})
      DAGEngine.add_node("node2", %{data: "value2"})
      DAGEngine.add_node("node3", %{data: "value3"})

      # Get all nodes
      nodes = DAGEngine.get_all_nodes()

      # Check that we have the expected number of nodes
      assert length(nodes) == 3

      # Check that each node is present
      assert Enum.any?(nodes, fn {id, _} -> id == "node1" end)
      assert Enum.any?(nodes, fn {id, _} -> id == "node2" end)
      assert Enum.any?(nodes, fn {id, _} -> id == "node3" end)
    end
  end

  describe "edge operations" do
    test "add_edge creates a connection between nodes" do
      # Add nodes
      DAGEngine.add_node("source", %{type: "source"})
      DAGEngine.add_node("target", %{type: "target"})

      # Add edge
      assert :ok = DAGEngine.add_edge("source", "target", "connects")

      # Query to verify the edge was created
      {:ok, results} = DAGEngine.query({nil, nil, nil})

      # Find edges in the results
      source_node = Enum.find(results, fn {id, _, _} -> id == "source" end)
      assert source_node != nil

      {_, _, edges} = source_node

      # Verify edge exists
      assert Enum.any?(edges, fn {from, to, label} ->
        from == "source" && to == "target" && label == "connects"
      end)
    end

    test "add_edge fails if nodes don't exist" do
      # Try to add edge between non-existent nodes
      assert {:error, :node_not_found} = DAGEngine.add_edge("missing_source", "missing_target", "label")

      # Add only source node
      DAGEngine.add_node("only_source", %{})
      assert {:error, :node_not_found} = DAGEngine.add_edge("only_source", "missing_target", "label")

      # Add only target node
      DAGEngine.add_node("only_target", %{})
      assert {:error, :node_not_found} = DAGEngine.add_edge("missing_source", "only_target", "label")
    end
  end

  describe "query operations" do
    setup do
      # Create a graph with triple-like structure for testing queries
      DAGEngine.add_node("tx1", %{subject: "Alice", predicate: "knows", object: "Bob"})
      DAGEngine.add_node("tx2", %{subject: "Alice", predicate: "likes", object: "Pizza"})
      DAGEngine.add_node("tx3", %{subject: "Bob", predicate: "knows", object: "Charlie"})
      DAGEngine.add_node("tx4", %{subject: "Bob", predicate: "likes", object: "Sushi"})

      # Add some edges
      DAGEngine.add_edge("tx1", "tx2", "same_subject")
      DAGEngine.add_edge("tx3", "tx4", "same_subject")

      :ok
    end

    test "query with exact match" do
      {:ok, results} = DAGEngine.query({"Alice", "knows", "Bob"})
      assert length(results) == 1
      {node_id, data, _edges} = hd(results)
      assert node_id == "tx1"
      assert data.subject == "Alice"
      assert data.predicate == "knows"
      assert data.object == "Bob"
    end

    test "query with subject wildcard" do
      {:ok, results} = DAGEngine.query({nil, "knows", "Bob"})
      assert length(results) == 1
      {node_id, _data, _edges} = hd(results)
      assert node_id == "tx1"
    end

    test "query with predicate wildcard" do
      {:ok, results} = DAGEngine.query({"Alice", nil, "Bob"})
      assert length(results) == 1
      {node_id, _data, _edges} = hd(results)
      assert node_id == "tx1"
    end

    test "query with object wildcard" do
      {:ok, results} = DAGEngine.query({"Alice", "knows", nil})
      assert length(results) == 1
      {node_id, _data, _edges} = hd(results)
      assert node_id == "tx1"
    end

    test "query with multiple wildcards" do
      {:ok, results} = DAGEngine.query({"Alice", nil, nil})
      assert length(results) == 2

      # Results contain both Alice's nodes
      node_ids = Enum.map(results, fn {id, _, _} -> id end)
      assert Enum.member?(node_ids, "tx1")
      assert Enum.member?(node_ids, "tx2")
    end

    test "query with all wildcards" do
      {:ok, results} = DAGEngine.query({nil, nil, nil})
      assert length(results) == 4  # Should return all four nodes
    end

    test "query with no matches" do
      {:ok, results} = DAGEngine.query({"Unknown", "unknown", "Unknown"})
      assert results == []
    end
  end
end

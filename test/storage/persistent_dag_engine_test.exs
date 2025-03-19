defmodule Kylix.Storage.PersistentDAGEngineTest do
  use ExUnit.Case
  alias Kylix.Storage.PersistentDAGEngine

  @test_db_path "test/tmp/dag_test_db"

  setup do
    # Reset the application for a clean state
    :ok = Application.stop(:kylix)

    # Clean up the test database directory
    File.rm_rf!(@test_db_path)
    File.mkdir_p!(@test_db_path)

    # Set test database path in application environment
    Application.put_env(:kylix, :db_path, @test_db_path)

    # Restart the application with our test config
    {:ok, _} = Application.ensure_all_started(:kylix)

    # Verify the PersistentDAGEngine is running
    pid = Process.whereis(PersistentDAGEngine)
    assert is_pid(pid) and Process.alive?(pid)

    # Clean up after the test
    on_exit(fn ->
      File.rm_rf!(@test_db_path)
    end)

    :ok
  end

  describe "node operations" do
    test "add_node persists a node to disk" do
      node_id = "test_node_1"
      data = %{key: "value", number: 42}

      # Add the node
      assert :ok = PersistentDAGEngine.add_node(node_id, data)

      # Check that the node exists
      assert {:ok, ^data} = PersistentDAGEngine.get_node(node_id)

      # Check that the node was written to disk
      node_path = Path.join([@test_db_path, "nodes", "#{node_id}.bin"])
      assert File.exists?(node_path)

      # Verify the content of the file
      stored_data =
        node_path
        |> File.read!()
        |> :erlang.binary_to_term()

      assert stored_data == data
    end

    test "add_node with non-map data returns error" do
      node_id = "test_node_invalid"
      invalid_data = "not a map"

      assert {:error, :invalid_data} = PersistentDAGEngine.add_node(node_id, invalid_data)
      assert :not_found = PersistentDAGEngine.get_node(node_id)

      # Verify the node wasn't written to disk
      node_path = Path.join([@test_db_path, "nodes", "#{node_id}.bin"])
      refute File.exists?(node_path)
    end

    test "get_node reads from disk if not in cache" do
      node_id = "test_node_disk"
      data = %{source: "disk_stored"}

      # Add node
      assert :ok = PersistentDAGEngine.add_node(node_id, data)

      # Restart the application to clear the cache
      :ok = Application.stop(:kylix)
      {:ok, _} = Application.ensure_all_started(:kylix)

      # Get the node - should be read from disk
      assert {:ok, ^data} = PersistentDAGEngine.get_node(node_id)
    end

    test "get_all_nodes returns all nodes in the store" do
      # Add several nodes
      PersistentDAGEngine.add_node("node1", %{data: "value1"})
      PersistentDAGEngine.add_node("node2", %{data: "value2"})
      PersistentDAGEngine.add_node("node3", %{data: "value3"})

      # Get all nodes
      nodes = PersistentDAGEngine.get_all_nodes()

      # Check that we have the expected number of nodes
      assert length(nodes) == 3

      # Check that each node is present
      assert Enum.any?(nodes, fn {id, _} -> id == "node1" end)
      assert Enum.any?(nodes, fn {id, _} -> id == "node2" end)
      assert Enum.any?(nodes, fn {id, _} -> id == "node3" end)
    end
  end

  describe "edge operations" do
    test "add_edge creates a connection between nodes and persists it" do
      # Add nodes
      PersistentDAGEngine.add_node("source", %{type: "source"})
      PersistentDAGEngine.add_node("target", %{type: "target"})

      # Add edge
      assert :ok = PersistentDAGEngine.add_edge("source", "target", "connects")

      # Verify edge was stored on disk
      edge_path = Path.join([@test_db_path, "edges", "source_target.bin"])
      assert File.exists?(edge_path)

      # Read the edge from disk and verify
      stored_edge =
        edge_path
        |> File.read!()
        |> :erlang.binary_to_term()

      assert stored_edge == {"source", "target", "connects"}

      # Query to verify the edge was created
      {:ok, results} = PersistentDAGEngine.query({nil, nil, nil})

      # Find edges in the results
      source_node = Enum.find(results, fn {id, _, _} -> id == "source" end)
      assert source_node != nil

      {_, _, edges} = source_node

      # Verify edge exists in query results
      assert Enum.any?(edges, fn {from, to, label} ->
        from == "source" && to == "target" && label == "connects"
      end)
    end

    test "add_edge fails if nodes don't exist" do
      # Try to add edge between non-existent nodes
      assert {:error, :node_not_found} = PersistentDAGEngine.add_edge("missing_source", "missing_target", "label")

      # Add only source node
      PersistentDAGEngine.add_node("only_source", %{})
      assert {:error, :node_not_found} = PersistentDAGEngine.add_edge("only_source", "missing_target", "label")

      # Add only target node
      PersistentDAGEngine.add_node("only_target", %{})
      assert {:error, :node_not_found} = PersistentDAGEngine.add_edge("missing_source", "only_target", "label")
    end

    test "edges are loaded from disk on restart" do
      # Add nodes and edges
      PersistentDAGEngine.add_node("source", %{type: "source"})
      PersistentDAGEngine.add_node("target", %{type: "target"})
      PersistentDAGEngine.add_edge("source", "target", "connects")

      # Restart the application to clear the cache
      :ok = Application.stop(:kylix)
      {:ok, _} = Application.ensure_all_started(:kylix)

      # Query to verify the edge is still there
      {:ok, results} = PersistentDAGEngine.query({nil, nil, nil})

      # Find edges in the results
      edge_found = Enum.any?(results, fn {id, _, edges} ->
        id == "source" &&
        Enum.any?(edges, fn {from, to, label} ->
          from == "source" && to == "target" && label == "connects"
        end)
      end)

      assert edge_found
    end
  end

  describe "query operations" do
    setup do
      # Create a graph with triple-like structure for testing queries
      PersistentDAGEngine.add_node("tx1", %{subject: "Alice", predicate: "knows", object: "Bob"})
      PersistentDAGEngine.add_node("tx2", %{subject: "Alice", predicate: "likes", object: "Pizza"})
      PersistentDAGEngine.add_node("tx3", %{subject: "Bob", predicate: "knows", object: "Charlie"})
      PersistentDAGEngine.add_node("tx4", %{subject: "Bob", predicate: "likes", object: "Sushi"})

      # Add some edges
      PersistentDAGEngine.add_edge("tx1", "tx2", "same_subject")
      PersistentDAGEngine.add_edge("tx3", "tx4", "same_subject")

      :ok
    end

    test "query with exact match" do
      {:ok, results} = PersistentDAGEngine.query({"Alice", "knows", "Bob"})
      assert length(results) == 1
      {node_id, data, _edges} = hd(results)
      assert node_id == "tx1"
      assert data.subject == "Alice"
      assert data.predicate == "knows"
      assert data.object == "Bob"
    end

    test "query with subject wildcard" do
      {:ok, results} = PersistentDAGEngine.query({nil, "knows", "Bob"})
      assert length(results) == 1
      {node_id, _data, _edges} = hd(results)
      assert node_id == "tx1"
    end

    test "query with predicate wildcard" do
      {:ok, results} = PersistentDAGEngine.query({"Alice", nil, "Bob"})
      assert length(results) == 1
      {node_id, _data, _edges} = hd(results)
      assert node_id == "tx1"
    end

    test "query with object wildcard" do
      {:ok, results} = PersistentDAGEngine.query({"Alice", "knows", nil})
      assert length(results) == 1
      {node_id, _data, _edges} = hd(results)
      assert node_id == "tx1"
    end

    test "query with multiple wildcards" do
      {:ok, results} = PersistentDAGEngine.query({"Alice", nil, nil})
      assert length(results) == 2

      # Results contain both Alice's nodes
      node_ids = Enum.map(results, fn {id, _, _} -> id end)
      assert Enum.member?(node_ids, "tx1")
      assert Enum.member?(node_ids, "tx2")
    end

    test "query with all wildcards" do
      {:ok, results} = PersistentDAGEngine.query({nil, nil, nil})
      assert length(results) == 4  # Should return all four nodes
    end

    test "query with no matches" do
      {:ok, results} = PersistentDAGEngine.query({"Unknown", "unknown", "Unknown"})
      assert results == []
    end

    test "query results include correct edges" do
      {:ok, results} = PersistentDAGEngine.query({"Alice", nil, nil})

      # Find tx1 in results
      tx1_result = Enum.find(results, fn {id, _, _} -> id == "tx1" end)
      assert tx1_result != nil

      # Extract edges from tx1
      {_, _, edges} = tx1_result

      # Verify the edge from tx1 to tx2 exists
      assert Enum.any?(edges, fn {from, to, label} ->
        from == "tx1" && to == "tx2" && label == "same_subject"
      end)
    end
  end

  describe "persistence and recovery" do
    test "data survives process restart" do
      # Add test data
      PersistentDAGEngine.add_node("persist_test", %{key: "survival_test"})

      # Restart the application
      :ok = Application.stop(:kylix)
      {:ok, _} = Application.ensure_all_started(:kylix)

      # Verify data survived
      assert {:ok, %{key: "survival_test"}} = PersistentDAGEngine.get_node("persist_test")
    end

    test "metadata is saved and recovered" do
      # Add some nodes to increment metadata counters
      PersistentDAGEngine.add_node("meta_test1", %{data: "metadata_test"})
      PersistentDAGEngine.add_node("meta_test2", %{data: "metadata_test"})
      PersistentDAGEngine.add_edge("meta_test1", "meta_test2", "test_edge")

      # Verify metadata file exists
      metadata_path = Path.join(@test_db_path, "metadata.bin")
      assert File.exists?(metadata_path)

      # Restart the application
      :ok = Application.stop(:kylix)
      {:ok, _} = Application.ensure_all_started(:kylix)

      # Add another node and check ID sequence continues
      PersistentDAGEngine.add_node("meta_test3", %{data: "metadata_test"})

      # Query to verify all nodes exist
      {:ok, results} = PersistentDAGEngine.query({nil, nil, nil})
      assert length(results) == 3
    end
  end
end

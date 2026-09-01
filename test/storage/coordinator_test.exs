defmodule Kylix.Storage.CoordinatorTest do
  use ExUnit.Case, async: false

  alias Kylix.Storage.Coordinator
  alias Kylix.Storage.DAGEngine

  setup do
    # Stop any existing DAGEngine first to ensure we have a clean state
    if Process.whereis(DAGEngine) do
      GenServer.stop(DAGEngine)
    end

    # Delete any existing ETS tables
    Enum.each([:coordinator_query_cache, :coordinator_cache_access_times, :coordinator_metrics], fn table ->
      try do
        :ets.delete(table)
      catch
        _, _ -> :ok
      end
    end)

    # Start a fresh DAGEngine just for this test
    {:ok, pid} = DAGEngine.start_link()

    # Ensure the DAGEngine is running
    assert is_pid(pid) and Process.alive?(pid)

    # Initialize Coordinator caches and metrics
    Coordinator.init_cache()
    Coordinator.clear_all_cache()
    Coordinator.reset_cache_metrics()

    # Add cleanup on exit
    on_exit(fn ->
      # Stop the DAGEngine after the test, but handle case when it's already stopped
      try do
        if Process.whereis(DAGEngine) do
          GenServer.stop(DAGEngine)
        end
      catch
        _kind, _reason -> :ok  # Ignore errors if process is already gone
      end

      # Delete any existing ETS tables
      Enum.each([:coordinator_query_cache, :coordinator_cache_access_times, :coordinator_metrics], fn table ->
        try do
          :ets.delete(table)
        catch
          _, _ -> :ok
        end
      end)
    end)

    :ok
  end

  describe "add_node/2" do
    test "adds node to DAGEngine in test environment" do
      data = %{subject: "node1", type: "Person"}
      assert :ok = Coordinator.add_node("node1", data)

      assert {:ok, ^data} = DAGEngine.get_node("node1")
      assert {:ok, ^data} = Coordinator.get_node("node1")
    end
  end

  describe "add_edge/3" do
    test "adds edge to DAGEngine in test environment" do
      node1_data = %{subject: "node1", type: "Person"}
      node2_data = %{subject: "node2", type: "Location"}

      Coordinator.add_node("node1", node1_data)
      Coordinator.add_node("node2", node2_data)

      assert :ok = Coordinator.add_edge("node1", "node2", "lives_in")

      # Since getting edges directly isn't exposed except via query, we query to verify edge
      assert {:ok, results} = Coordinator.query({"node1", nil, nil})

      assert [{"node1", ^node1_data, edges}] = results
      assert {"node1", "node2", "lives_in"} in edges
    end
  end

  describe "get_node/1" do
    test "returns node if it exists" do
      data = %{subject: "node1"}
      Coordinator.add_node("node1", data)

      assert {:ok, ^data} = Coordinator.get_node("node1")
    end

    test "returns :not_found if node does not exist" do
      assert :not_found = Coordinator.get_node("nonexistent_node")
    end
  end

  describe "get_all_nodes/0" do
    test "returns all nodes from DAGEngine" do
      data1 = %{subject: "node1"}
      data2 = %{subject: "node2"}

      Coordinator.add_node("node1", data1)
      Coordinator.add_node("node2", data2)

      nodes = Coordinator.get_all_nodes()
      assert length(nodes) == 2
      assert {"node1", data1} in nodes
      assert {"node2", data2} in nodes
    end
  end

  describe "query/1" do
    test "caches query results and updates metrics" do
      # Add a node
      data = %{subject: "subject1", predicate: "p", object: "o"}
      Coordinator.add_node("node1", data)

      # Reset metrics just to be sure we are starting from 0
      Coordinator.reset_cache_metrics()
      metrics_before = Coordinator.get_cache_metrics()
      assert metrics_before.cache_hits == 0
      assert metrics_before.cache_misses == 0

      # First query (cache miss)
      pattern = {"subject1", nil, nil}
      assert {:ok, results} = Coordinator.query(pattern)
      assert length(results) == 1

      metrics_after_miss = Coordinator.get_cache_metrics()
      assert metrics_after_miss.cache_hits == 0
      assert metrics_after_miss.cache_misses == 1
      assert metrics_after_miss.cache_size == 1

      # Second query (cache hit)
      assert {:ok, results_hit} = Coordinator.query(pattern)
      assert results == results_hit

      metrics_after_hit = Coordinator.get_cache_metrics()
      assert metrics_after_hit.cache_hits == 1
      assert metrics_after_hit.cache_misses == 1
    end

    test "invalidates cache when node is added" do
      data = %{subject: "subject1"}
      Coordinator.add_node("node1", data)

      # Query and cache
      pattern = {"subject1", nil, nil}
      Coordinator.query(pattern)

      # Verify cache has entry
      assert Coordinator.get_cache_metrics().cache_size == 1

      # Add a new node that matches the pattern, which should invalidate the cache
      new_data = %{subject: "subject1"}
      Coordinator.add_node("node2", new_data)

      # Cache should be invalidated (size decreases or next query is a miss)
      # In the current implementation, it removes the entry from the ETS table
      assert Coordinator.get_cache_metrics().cache_size == 0
    end

    test "invalidates edge-related cache when edge is added" do
      node1_data = %{subject: "subject1"}
      node2_data = %{subject: "subject2"}

      Coordinator.add_node("node1", node1_data)
      Coordinator.add_node("node2", node2_data)

      # Query to populate cache
      pattern = {"node1", nil, nil}
      Coordinator.query(pattern)

      assert Coordinator.get_cache_metrics().cache_size == 1

      # Add edge, invalidating the cache
      Coordinator.add_edge("node1", "node2", "connected")

      assert Coordinator.get_cache_metrics().cache_size == 0
    end
  end

  describe "sync_cache/0" do
    test "returns {:ok, 0} in test environment (no-op)" do
      assert {:ok, 0} = Coordinator.sync_cache()
    end
  end
end

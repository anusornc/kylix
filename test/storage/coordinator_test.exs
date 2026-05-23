defmodule Kylix.Storage.CoordinatorTest do
  use ExUnit.Case, async: false
  alias Kylix.Storage.Coordinator
  alias Kylix.Storage.DAGEngine
  alias Kylix.Storage.PersistentDAGEngine

  setup do
    # Ensure any existing processes are stopped
    if Process.whereis(DAGEngine), do: GenServer.stop(DAGEngine)
    if Process.whereis(PersistentDAGEngine), do: GenServer.stop(PersistentDAGEngine)

    # Start DAGEngine for Coordinator testing
    case DAGEngine.start_link() do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end

    # Clean up and re-initialize ETS tables for Coordinator
    try do
      :ets.delete(:coordinator_query_cache)
      :ets.delete(:coordinator_cache_access_times)
      :ets.delete(:coordinator_metrics)
    rescue
      ArgumentError -> :ok
    end

    Coordinator.init_cache()

    # Unload mocks automatically
    on_exit(fn ->
      try do
        :ets.delete(:coordinator_query_cache)
        :ets.delete(:coordinator_cache_access_times)
        :ets.delete(:coordinator_metrics)
      rescue
        ArgumentError -> :ok
      end

      Application.delete_env(:kylix, :use_persistent_storage)

      :meck.unload()
    end)

    :ok
  end

  describe "add_node/2" do
    test "adds node only to DAGEngine when use_persistent_storage is false (test mode)" do
      # Configure for test mode logic
      Application.put_env(:kylix, :use_persistent_storage, false)

      node_id = "test_node_1"
      data = %{subject: "sub1", predicate: "pred1", object: "obj1"}

      :meck.new(Kylix.Storage.DAGEngine, [:passthrough])
      :meck.new(Kylix.Storage.PersistentDAGEngine, [:passthrough])

      :meck.expect(Kylix.Storage.DAGEngine, :add_node, fn _id, _data -> :ok end)

      # Execute
      assert :ok = Coordinator.add_node(node_id, data)

      # Verification
      assert :meck.called(Kylix.Storage.DAGEngine, :add_node, [node_id, data])
      refute :meck.called(Kylix.Storage.PersistentDAGEngine, :add_node, [node_id, data])
    end

    test "adds node to both DAGEngine and PersistentDAGEngine when use_persistent_storage is true (non-test mode)" do
      # Configure for non-test mode logic
      Application.put_env(:kylix, :use_persistent_storage, true)

      node_id = "test_node_2"
      data = %{subject: "sub2", predicate: "pred2", object: "obj2"}

      :meck.new(Kylix.Storage.DAGEngine, [:passthrough])
      :meck.new(Kylix.Storage.PersistentDAGEngine, [:passthrough])

      :meck.expect(Kylix.Storage.DAGEngine, :add_node, fn _id, _data -> :ok end)
      :meck.expect(Kylix.Storage.PersistentDAGEngine, :add_node, fn _id, _data -> :ok end)

      # Execute
      assert :ok = Coordinator.add_node(node_id, data)

      # Verification
      assert :meck.called(Kylix.Storage.DAGEngine, :add_node, [node_id, data])
      assert :meck.called(Kylix.Storage.PersistentDAGEngine, :add_node, [node_id, data])
    end
  end
end

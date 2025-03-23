defmodule Kylix.Storage.Coordinator do
  @moduledoc """
  Coordinates between in-memory DAGEngine and persistent storage.

  This module provides a unified API for Kylix's storage layer,
  utilizing the in-memory DAGEngine as a cache and the PersistentDAGEngine
  for durability in non-test environments.
  """

  require Logger

  # Check environment at compile time
  @is_test Mix.env() == :test

  @doc """
  Add a node to storage.
  In test mode, only adds to DAGEngine.
  In other environments, adds to both engines.
  """
  def add_node(node_id, data) do
    # Always add to in-memory DAGEngine
    result = Kylix.Storage.DAGEngine.add_node(node_id, data)

    # In non-test environments, also add to persistent storage
    unless @is_test do
      Kylix.Storage.PersistentDAGEngine.add_node(node_id, data)
    end

    result
  end

  @doc """
  Add an edge between nodes.
  In test mode, only adds to DAGEngine.
  In other environments, adds to both engines.
  """
  def add_edge(from_id, to_id, label) do
    # Always add to in-memory DAGEngine
    result = Kylix.Storage.DAGEngine.add_edge(from_id, to_id, label)

    # In non-test environments, also add to persistent storage
    unless @is_test do
      Kylix.Storage.PersistentDAGEngine.add_edge(from_id, to_id, label)
    end

    result
  end

  @doc """
  Get a node by ID.
  First tries DAGEngine, falls back to PersistentDAGEngine in non-test mode.
  """
  def get_node(node_id) do
    # Always try in-memory DAGEngine first
    case Kylix.Storage.DAGEngine.get_node(node_id) do
      {:ok, data} ->
        # Found in memory cache
        {:ok, data}

      :not_found ->
        # Not in memory, try persistent storage if not in test mode
        if @is_test do
          :not_found
        else
          case Kylix.Storage.PersistentDAGEngine.get_node(node_id) do
            {:ok, data} ->
              # Found in persistent, update memory cache
              Kylix.Storage.DAGEngine.add_node(node_id, data)
              {:ok, data}

            not_found ->
              not_found
          end
        end
    end
  end

  @doc """
  Get all nodes.
  In test mode, returns from DAGEngine.
  In other environments, ensures cache is populated from persistent storage.
  """
  def get_all_nodes do
    # Get nodes from in-memory cache
    in_memory_nodes = Kylix.Storage.DAGEngine.get_all_nodes()

    if !@is_test && Enum.empty?(in_memory_nodes) do
      # In non-test mode and cache is empty, populate from persistent storage
      persistent_nodes = Kylix.Storage.PersistentDAGEngine.get_all_nodes()

      # Update cache
      for {node_id, data} <- persistent_nodes do
        Kylix.Storage.DAGEngine.add_node(node_id, data)
      end

      persistent_nodes
    else
      in_memory_nodes
    end
  end

  @doc """
  Execute a query with the given pattern.
  In test mode, queries DAGEngine.
  In other environments, tries DAGEngine first, falls back to PersistentDAGEngine.
  """
  def query(pattern) do
    # Try in-memory cache first
    case Kylix.Storage.DAGEngine.query(pattern) do
      {:ok, results} when results != [] ->
        # Cache hit with results
        {:ok, results}

      _ ->
        # Cache miss or empty results
        if @is_test do
          # In test mode, just return the result from DAGEngine
          Kylix.Storage.DAGEngine.query(pattern)
        else
          # In non-test mode, try persistent storage
          Logger.debug("In-memory cache miss for query: #{inspect(pattern)}")

          # Try persistent storage
          case Kylix.Storage.PersistentDAGEngine.query(pattern) do
            {:ok, results} when results != [] ->
              # Got results from persistent storage

              # Update in-memory cache with these nodes
              for {node_id, data, edges} <- results do
                Kylix.Storage.DAGEngine.add_node(node_id, data)

                # Also add edges to cache
                for {from, to, label} <- edges do
                  Kylix.Storage.DAGEngine.add_edge(from, to, label)
                end
              end

              {:ok, results}

            other ->
              # No results or error from persistent storage
              other
          end
        end
    end
  end

  @doc """
  Sync in-memory cache with persistent storage.
  Does nothing in test mode.
  """
  def sync_cache do
    if @is_test do
      {:ok, 0}  # No-op in test mode
    else
      Logger.info("Synchronizing in-memory cache with persistent storage")

      # Get all nodes from persistent storage
      persistent_nodes = Kylix.Storage.PersistentDAGEngine.get_all_nodes()

      # Update in-memory cache
      for {node_id, data} <- persistent_nodes do
        Kylix.Storage.DAGEngine.add_node(node_id, data)
      end

      # TODO: We'd need an API to get all edges to sync those too

      {:ok, length(persistent_nodes)}
    end
  end
end

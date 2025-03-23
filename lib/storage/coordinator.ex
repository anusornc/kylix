defmodule Kylix.Storage.Coordinator do
  @moduledoc """
  Coordinates between in-memory DAGEngine and persistent storage.

  This module provides a unified API for Kylix's storage layer,
  utilizing the in-memory DAGEngine as a cache and the PersistentDAGEngine
  for durability in non-test environments.
  """

  require Logger

  # Tell Dialyzer to ignore certain false positive warnings
  @dialyzer {:no_match, [add_node: 2, add_edge: 3, get_node: 1, get_all_nodes: 0, query: 1, sync_cache: 0, clear_all_cache: 0]}

  # Cache configuration
  @cache_ttl 300 # 5 minutes in seconds
  @max_cache_size 10000 # Maximum number of cached query results
  @cache_prune_threshold 8000 # When to prune the cache

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

    # Selectively invalidate cache related to this node
    selective_cache_invalidation(data)

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

    # Invalidate cache entries related to these nodes
    invalidate_edge_related_cache(from_id, to_id)

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
  Now with query caching for repeated queries.
  """
  def query(pattern) do
    # Measure query execution time
    start_time = System.monotonic_time(:microsecond)

    result = case get_from_cache(pattern) do
      {:hit, cached_result} ->
        # Cache hit
        Logger.debug("Coordinator cache hit for query #{inspect(pattern)}")

        # Update cache hit count
        increment_metric(:cache_hits)

        # Update access time for LRU tracking
        update_cache_access_time(pattern)

        cached_result

      :miss ->
        # Cache miss, execute query
        Logger.debug("Coordinator cache miss for query #{inspect(pattern)}")

        # Update cache miss count
        increment_metric(:cache_misses)

        # Try in-memory cache first
        case Kylix.Storage.DAGEngine.query(pattern) do
          {:ok, results} when results != [] ->
            # Cache hit with results
            result = {:ok, results}
            store_in_cache(pattern, result)
            result

          other_result ->
            # Cache miss or empty results
            if @is_test do
              # In test mode, just return the result from DAGEngine
              result = other_result
              store_in_cache(pattern, result)
              result
            else
              # In non-test mode, try persistent storage
              Logger.debug("In-memory cache miss for query: #{inspect(pattern)}")

              # Try persistent storage
              result = Kylix.Storage.PersistentDAGEngine.query(pattern)

              case result do
                {:ok, results} when results != [] ->
                  # Got results from persistent storage
                  # Update in-memory cache with these nodes
                  for {node_id, data, edges} <- results do
                    Kylix.Storage.DAGEngine.add_node(node_id, data)

                    # Also add edges to cache
                    for {to_id, label} <- edges do
                      Kylix.Storage.DAGEngine.add_edge(node_id, to_id, label)
                    end
                  end

                  # Store in cache
                  store_in_cache(pattern, result)
                  result

                other ->
                  # No results or error from persistent storage
                  store_in_cache(pattern, other)
                  other
              end
            end
        end
    end

    # Calculate and store query execution time
    end_time = System.monotonic_time(:microsecond)
    execution_time = end_time - start_time
    update_execution_time(execution_time)

    result
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

      # Also clear the query cache
      clear_all_cache()

      {:ok, length(persistent_nodes)}
    end
  end

  @doc """
  Get cache metrics
  """
  def get_cache_metrics do
    hits = get_metric(:cache_hits)
    misses = get_metric(:cache_misses)
    cache_entries = count_cache_entries()

    hit_rate = if hits + misses > 0, do: hits / (hits + misses) * 100, else: 0
    avg_query_time = get_metric(:avg_query_time)

    %{
      cache_hits: hits,
      cache_misses: misses,
      cache_size: cache_entries,
      hit_rate_percent: hit_rate,
      avg_query_time_microseconds: avg_query_time
    }
  end

  @doc """
  Reset cache metrics
  """
  def reset_cache_metrics do
    :ets.insert(:coordinator_metrics, {:cache_hits, 0})
    :ets.insert(:coordinator_metrics, {:cache_misses, 0})
    :ets.insert(:coordinator_metrics, {:query_time_sum, 0})
    :ets.insert(:coordinator_metrics, {:query_count, 0})
    :ets.insert(:coordinator_metrics, {:avg_query_time, 0})
    :ok
  end

  # Initialize ETS tables for query cache and metrics
  @doc false
  def init_cache do
    # Create cache table if it doesn't exist
    if :ets.whereis(:coordinator_query_cache) == :undefined do
      :ets.new(:coordinator_query_cache, [:set, :named_table, :public])
    end

    # Create cache access time table for LRU
    if :ets.whereis(:coordinator_cache_access_times) == :undefined do
      :ets.new(:coordinator_cache_access_times, [:ordered_set, :named_table, :public])
    end

    # Create metrics table
    if :ets.whereis(:coordinator_metrics) == :undefined do
      :ets.new(:coordinator_metrics, [:set, :named_table, :public])
      reset_cache_metrics()
    end

    :ok
  end

  # Get a query result from cache
  defp get_from_cache(pattern) do
    # Create a cache key
    cache_key = :erlang.term_to_binary(pattern)

    # Try to get from ETS
    case :ets.lookup(:coordinator_query_cache, cache_key) do
      [] ->
        :miss

      [{^cache_key, {result, timestamp}}] ->
        # Check if still valid
        now = System.system_time(:second)
        if now - timestamp <= @cache_ttl do
          {:hit, result}
        else
          # Expired, remove it
          :ets.delete(:coordinator_query_cache, cache_key)
          :ets.match_delete(:coordinator_cache_access_times, {{:_, :_}, pattern})
          :miss
        end
    end
  end

  # Store a query result in cache
  defp store_in_cache(pattern, result) do
    # Check if cache needs pruning
    prune_cache_if_needed()

    # Create cache key and store result
    cache_key = :erlang.term_to_binary(pattern)
    now = System.system_time(:second)
    :ets.insert(:coordinator_query_cache, {cache_key, {result, now}})

    # Update access time for LRU
    :ets.insert(:coordinator_cache_access_times, {{now, cache_key}, pattern})

    :ok
  end

  # Update access time for a cached query
  defp update_cache_access_time(pattern) do
    # Delete old access time entry
    :ets.match_delete(:coordinator_cache_access_times, {{:_, :_}, pattern})

    # Add new entry with current timestamp
    cache_key = :erlang.term_to_binary(pattern)
    now = System.system_time(:second)
    :ets.insert(:coordinator_cache_access_times, {{now, cache_key}, pattern})

    :ok
  end

  # Prune cache if it's too large
  defp prune_cache_if_needed do
    # Get current cache size
    current_size = count_cache_entries()

    if current_size >= @cache_prune_threshold do
      Logger.info("Pruning cache (size: #{current_size})")

      # Get oldest entries
      entries_to_remove = current_size - div(@max_cache_size, 2)

      # Find the oldest entries based on access time
      oldest_entries = :ets.select(:coordinator_cache_access_times,
        [{{{:"$1", :"$2"}, :"$3"}, [], [{{"$1", "$3"}}]}],
        entries_to_remove)

      # Remove them from both tables
      Enum.each(oldest_entries, fn {timestamp, pattern} ->
        cache_key = :erlang.term_to_binary(pattern)
        :ets.delete(:coordinator_query_cache, cache_key)
        :ets.delete(:coordinator_cache_access_times, {{timestamp, :_}, pattern})
      end)

      Logger.info("Pruned #{entries_to_remove} entries from cache")
    end
  end

  # Count cache entries
  defp count_cache_entries do
    :ets.info(:coordinator_query_cache, :size)
  end

  # Clear all cache data
  defp clear_all_cache do
    :ets.delete_all_objects(:coordinator_query_cache)
    :ets.delete_all_objects(:coordinator_cache_access_times)
    :ok
  end

  # Selective cache invalidation based on the data
  defp selective_cache_invalidation(data) do
    # Extract components that affect query invalidation
    subject = Map.get(data, :subject)
    predicate = Map.get(data, :predicate)
    object = Map.get(data, :object)

    # Find cache entries that could be affected by this change
    all_patterns = :ets.tab2list(:coordinator_cache_access_times)
    |> Enum.map(fn {_, pattern} -> pattern end)

    affected_patterns = Enum.filter(all_patterns, fn pattern ->
      # Invalidate if any of these match:
      # 1. Query is for this subject
      # 2. Query is for this predicate
      # 3. Query is for this object
      # 4. Query is a wildcard for any of these fields
      {pattern_s, pattern_p, pattern_o} = pattern
      (pattern_s == subject || pattern_s == nil) ||
      (pattern_p == predicate || pattern_p == nil) ||
      (pattern_o == object || pattern_o == nil)
    end)

    # Invalidate affected patterns
    invalidate_patterns(affected_patterns)

    Logger.debug("Selectively invalidated #{length(affected_patterns)} cache entries")
  end

  # Invalidate cache entries related to an edge
  defp invalidate_edge_related_cache(from_id, to_id) do
    # Find cache entries that could be affected by this edge
    all_patterns = :ets.tab2list(:coordinator_cache_access_times)
    |> Enum.map(fn {_, pattern} -> pattern end)

    # For edges, we mainly care about patterns that involve either node
    affected_patterns = Enum.filter(all_patterns, fn pattern ->
      {pattern_s, _pattern_p, _pattern_o} = pattern
      pattern_s == from_id || pattern_s == to_id || pattern_s == nil
    end)

    # Invalidate affected patterns
    invalidate_patterns(affected_patterns)

    Logger.debug("Selectively invalidated #{length(affected_patterns)} edge-related cache entries")
  end

  # Invalidate specific patterns from the cache
  defp invalidate_patterns(patterns) do
    Enum.each(patterns, fn pattern ->
      cache_key = :erlang.term_to_binary(pattern)

      # Remove from query cache
      :ets.delete(:coordinator_query_cache, cache_key)

      # Remove from access times
      :ets.match_delete(:coordinator_cache_access_times, {{:_, :_}, pattern})
    end)
  end

  # Metrics handling
  defp get_metric(key) do
    case :ets.lookup(:coordinator_metrics, key) do
      [{^key, value}] -> value
      [] -> 0
    end
  end

  defp increment_metric(key) do
    current = get_metric(key)
    :ets.insert(:coordinator_metrics, {key, current + 1})
  end

  defp update_execution_time(time) do
    sum = get_metric(:query_time_sum) + time
    count = get_metric(:query_count) + 1
    avg = sum / count

    :ets.insert(:coordinator_metrics, {:query_time_sum, sum})
    :ets.insert(:coordinator_metrics, {:query_count, count})
    :ets.insert(:coordinator_metrics, {:avg_query_time, avg})
  end
end

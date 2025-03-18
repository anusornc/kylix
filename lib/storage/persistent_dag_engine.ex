defmodule Kylix.Storage.PersistentDAGEngine do
  use GenServer

  # Constants
  @db_dir "data/dag_storage"
  @metadata_file "metadata.bin"
  @nodes_dir "nodes"
  @edges_dir "edges"

  # State structure
  # %{
  #   db_path: String.t(),
  #   metadata: %{
  #     last_node_id: String.t(),
  #     node_count: integer(),
  #     edge_count: integer(),
  #     last_checkpoint: DateTime.t()
  #   },
  #   cache: %{
  #     nodes: %{node_id => node_data},
  #     edges: %{from_id => [{to_id, label}]}
  #   }
  # }

  # Start the persistent DAG engine
  def start_link(opts \\ []) do
    db_path = Keyword.get(opts, :db_path, @db_dir)
    GenServer.start_link(__MODULE__, [db_path: db_path], name: __MODULE__)
  end

  # API functions (similar to your current DAGEngine)
  def add_node(node_id, data), do: GenServer.call(__MODULE__, {:add_node, node_id, data})
  def add_edge(from_id, to_id, label), do: GenServer.call(__MODULE__, {:add_edge, from_id, to_id, label})
  def get_node(node_id), do: GenServer.call(__MODULE__, {:get_node, node_id})
  def get_all_nodes(), do: GenServer.call(__MODULE__, :get_all_nodes)
  def query(pattern), do: GenServer.call(__MODULE__, {:query, pattern})

  # Initialize the storage engine
  @impl true
  def init(opts) do
    db_path = Keyword.get(opts, :db_path, @db_dir)

    # Create directories if they don't exist
    File.mkdir_p!(Path.join(db_path, @nodes_dir))
    File.mkdir_p!(Path.join(db_path, @edges_dir))

    # Load or initialize metadata
    metadata = load_metadata(db_path)

    # Initialize empty cache
    cache = %{
      nodes: %{},
      edges: %{}
    }

    # Load recent nodes and edges into cache for faster access
    cache = load_recent_data(db_path, metadata, cache)

    {:ok, %{db_path: db_path, metadata: metadata, cache: cache}}
  end

  # Handle node addition
  @impl true
  def handle_call({:add_node, node_id, data}, _from, state) do
    require Logger
    Logger.info("Adding node #{node_id} with data: #{inspect(data)}")

    unless is_map(data) do
      Logger.error("Data for node #{node_id} is not a map: #{inspect(data)}")
      {:reply, {:error, :invalid_data}, state}
    else
      # Save node to disk
      node_path = Path.join([state.db_path, @nodes_dir, "#{node_id}.bin"])
      serialized_data = :erlang.term_to_binary(data)
      :ok = File.write!(node_path, serialized_data)

      # Update cache
      new_cache = put_in(state.cache.nodes[node_id], data)

      # Update metadata
      new_metadata = %{
        state.metadata |
        node_count: state.metadata.node_count + 1,
        last_node_id: node_id
      }

      # Save updated metadata
      save_metadata(state.db_path, new_metadata)

      # Update state
      new_state = %{state | metadata: new_metadata, cache: new_cache}

      Logger.info("Node #{node_id} persisted to disk")
      {:reply, :ok, new_state}
    end
  end

  # Handle edge addition
  @impl true
  def handle_call({:add_edge, from_id, to_id, label}, _from, state) do
    # Check if nodes exist
    from_exists = node_exists?(state, from_id)
    to_exists = node_exists?(state, to_id)

    if from_exists and to_exists do
      # Create edge record
      edge_data = {from_id, to_id, label}
      edge_id = "#{from_id}_#{to_id}"
      edge_path = Path.join([state.db_path, @edges_dir, "#{edge_id}.bin"])

      # Save edge to disk
      serialized_edge = :erlang.term_to_binary(edge_data)
      :ok = File.write!(edge_path, serialized_edge)

      # Update cache
      edges_from = Map.get(state.cache.edges, from_id, [])
      new_edges_from = [{to_id, label} | edges_from]
      new_cache = put_in(state.cache.edges[from_id], new_edges_from)

      # Update metadata
      new_metadata = %{state.metadata | edge_count: state.metadata.edge_count + 1}
      save_metadata(state.db_path, new_metadata)

      # Update state
      new_state = %{state | metadata: new_metadata, cache: new_cache}

      {:reply, :ok, new_state}
    else
      {:reply, {:error, :node_not_found}, state}
    end
  end

  # Implement other handle_call functions for get_node, get_all_nodes, query...

  # Helper functions for persistence
  defp load_metadata(db_path) do
    metadata_path = Path.join(db_path, @metadata_file)

    if File.exists?(metadata_path) do
      # Load existing metadata
      metadata_path
      |> File.read!()
      |> :erlang.binary_to_term()
    else
      # Initialize new metadata
      %{
        last_node_id: nil,
        node_count: 0,
        edge_count: 0,
        last_checkpoint: DateTime.utc_now()
      }
    end
  end

  defp save_metadata(db_path, metadata) do
    metadata_path = Path.join(db_path, @metadata_file)
    serialized = :erlang.term_to_binary(metadata)
    File.write!(metadata_path, serialized)
  end

  defp node_exists?(state, node_id) do
    # Check cache first
    case Map.has_key?(state.cache.nodes, node_id) do
      true -> true
      false ->
        # Check disk
        node_path = Path.join([state.db_path, @nodes_dir, "#{node_id}.bin"])
        File.exists?(node_path)
    end
  end

  defp load_recent_data(db_path, _metadata, cache) do
    # Load recent nodes (could be optimized to load only most recent N nodes)
    nodes_dir = Path.join(db_path, @nodes_dir)
    nodes =
      nodes_dir
      |> File.ls!()
      |> Enum.take(100)  # Limit to recent files
      |> Enum.reduce(cache.nodes, fn file, acc ->
        node_id = Path.rootname(file)
        node_data =
          Path.join(nodes_dir, file)
          |> File.read!()
          |> :erlang.binary_to_term()
        Map.put(acc, node_id, node_data)
      end)

    # Similar approach for edges
    edges_dir = Path.join(db_path, @edges_dir)
    edges =
      if File.exists?(edges_dir) do
        edges_dir
        |> File.ls!()
        |> Enum.take(100)
        |> Enum.reduce(cache.edges, fn file, acc ->
          {from_id, to_id, label} =
            Path.join(edges_dir, file)
            |> File.read!()
            |> :erlang.binary_to_term()

          edges_from = Map.get(acc, from_id, [])
          Map.put(acc, from_id, [{to_id, label} | edges_from])
        end)
      else
        %{}
      end

    %{nodes: nodes, edges: edges}
  end

  # Implement a checkpoint mechanism to compact storage periodically
  def checkpoint(state) do
    # Logic to create a checkpoint of the current state
    # This could involve compacting the database, removing old data files, etc.
    # Update the last_checkpoint timestamp in metadata
    new_metadata = %{state.metadata | last_checkpoint: DateTime.utc_now()}
    save_metadata(state.db_path, new_metadata)
    %{state | metadata: new_metadata}
  end
end

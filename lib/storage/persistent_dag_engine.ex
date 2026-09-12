defmodule Kylix.Storage.PersistentDAGEngine do
  use GenServer
  require Logger

  @db_dir "data/dag_storage"
  @metadata_file "metadata.bin"
  @nodes_dir "nodes"
  @edges_dir "edges"

  def start_link(opts \\ []) do
    db_path = Keyword.get(opts, :db_path, @db_dir)
    GenServer.start_link(__MODULE__, [db_path: db_path], name: __MODULE__)
  end

  def add_node(node_id, data), do: GenServer.call(__MODULE__, {:add_node, node_id, data})

  def add_edge(from_id, to_id, label),
    do: GenServer.call(__MODULE__, {:add_edge, from_id, to_id, label})

  def query(pattern), do: GenServer.call(__MODULE__, {:query, pattern})

  @impl true
  def init(opts) do
    db_path = Keyword.get(opts, :db_path, @db_dir)

    File.mkdir_p!(Path.join(db_path, @nodes_dir))
    File.mkdir_p!(Path.join(db_path, @edges_dir))

    metadata = load_metadata(db_path)
    {nodes, edges} = load_graph(db_path)

    {:ok, %{db_path: db_path, metadata: metadata, nodes: nodes, edges: edges}}
  end

  defp load_metadata(db_path) do
    metadata_path = Path.join(db_path, @metadata_file)

    if File.exists?(metadata_path) do
      metadata_path
      |> File.read!()
      |> :erlang.binary_to_term([:safe])
    else
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

  defp load_graph(db_path) do
    {load_nodes(db_path), load_edges(db_path)}
  end

  defp load_nodes(db_path) do
    dir = Path.join(db_path, @nodes_dir)

    list_bin_files(dir)
    |> Enum.reduce(%{}, fn file, acc ->
      node_id = Path.rootname(file)

      node_data =
        dir
        |> Path.join(file)
        |> File.read!()
        |> :erlang.binary_to_term([:safe])

      Map.put(acc, node_id, node_data)
    end)
  end

  defp load_edges(db_path) do
    dir = Path.join(db_path, @edges_dir)

    list_bin_files(dir)
    |> Enum.reduce(%{}, fn file, acc ->
      edge_data =
        dir
        |> Path.join(file)
        |> File.read!()
        |> :erlang.binary_to_term([:safe])

      case edge_data do
        {from_id, to_id, label} ->
          edges_from = Map.get(acc, from_id, [])
          Map.put(acc, from_id, [{to_id, label} | edges_from])

        _ ->
          acc
      end
    end)
  end

  defp list_bin_files(dir) do
    if File.dir?(dir) do
      dir
      |> File.ls!()
      |> Enum.filter(&String.ends_with?(&1, ".bin"))
    else
      []
    end
  end

  def checkpoint(state) do
    new_metadata = %{state.metadata | last_checkpoint: DateTime.utc_now()}
    save_metadata(state.db_path, new_metadata)
    %{state | metadata: new_metadata}
  end

  @impl true
  def handle_call({:add_node, node_id, data}, _from, state) do
    Logger.info("Adding node #{node_id} with data: #{inspect(data)}")

    unless is_map(data) do
      Logger.error("Data for node #{node_id} is not a map: #{inspect(data)}")
      {:reply, {:error, :invalid_data}, state}
    else
      node_path = Path.join([state.db_path, @nodes_dir, "#{node_id}.bin"])
      serialized_data = :erlang.term_to_binary(data)
      :ok = File.write!(node_path, serialized_data)

      new_metadata = %{
        state.metadata
        | node_count: state.metadata.node_count + 1,
          last_node_id: node_id
      }

      save_metadata(state.db_path, new_metadata)

      new_state = %{
        state
        | metadata: new_metadata,
          nodes: Map.put(state.nodes, node_id, data)
      }

      Logger.info("Node #{node_id} persisted to disk")
      {:reply, :ok, new_state}
    end
  end

  @impl true
  def handle_call({:add_edge, from_id, to_id, label}, _from, state) do
    if Map.has_key?(state.nodes, from_id) and Map.has_key?(state.nodes, to_id) do
      edge_data = {from_id, to_id, label}
      edge_id = "#{from_id}_#{to_id}"
      edge_path = Path.join([state.db_path, @edges_dir, "#{edge_id}.bin"])

      serialized_edge = :erlang.term_to_binary(edge_data)
      :ok = File.write!(edge_path, serialized_edge)

      edges_from = Map.get(state.edges, from_id, [])
      new_metadata = %{state.metadata | edge_count: state.metadata.edge_count + 1}
      save_metadata(state.db_path, new_metadata)

      new_state = %{
        state
        | metadata: new_metadata,
          edges: Map.put(state.edges, from_id, [{to_id, label} | edges_from])
      }

      {:reply, :ok, new_state}
    else
      {:reply, {:error, :node_not_found}, state}
    end
  end

  @impl true
  def handle_call({:query, pattern}, _from, state) do
    {s, p, o} = pattern

    Logger.info("Querying with pattern {#{inspect(s)}, #{inspect(p)}, #{inspect(o)}}")

    matches =
      state.nodes
      |> Enum.filter(fn {_node_id, data} ->
        is_map(data) and
          (s == nil or Map.get(data, :subject) == s) and
          (p == nil or Map.get(data, :predicate) == p) and
          (o == nil or Map.get(data, :object) == o)
      end)

    results =
      Enum.map(matches, fn {node_id, data} ->
        edges = Map.get(state.edges, node_id, [])
        {node_id, data, edges}
      end)

    Logger.info("Query results: #{inspect(results)}")
    {:reply, {:ok, results}, state}
  end
end

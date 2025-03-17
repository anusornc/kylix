defmodule Kylix.Storage.DAGEngine do
  use GenServer

  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(_opts \\ []), do: GenServer.start_link(__MODULE__, %{}, name: __MODULE__)

  def add_node(node_id, data), do: GenServer.call(__MODULE__, {:add_node, node_id, data})
  def add_edge(from_id, to_id, label), do: GenServer.call(__MODULE__, {:add_edge, from_id, to_id, label})
  def get_node(node_id), do: GenServer.call(__MODULE__, {:get_node, node_id})
  def get_all_nodes(), do: GenServer.call(__MODULE__, :get_all_nodes)
  def query(pattern), do: GenServer.call(__MODULE__, {:query, pattern})

  @impl true
  def init(_args) do
    {:ok, %{nodes: %{}, edges: %{}}}
  end

  @impl true
  def handle_call({:add_node, node_id, data}, _from, state) do
    require Logger
    Logger.info("Adding node #{node_id} with data: #{inspect(data)}")
    new_nodes = Map.put(state.nodes, node_id, data)
    new_state = %{state | nodes: new_nodes}
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:add_edge, from_id, to_id, label}, _from, state) do
    # ตรวจสอบว่าโหนดต้นทางและปลายทางมีอยู่
    if Map.has_key?(state.nodes, from_id) and Map.has_key?(state.nodes, to_id) do
      new_edges = Map.update(state.edges, from_id, [{to_id, label}], fn existing ->
        [{to_id, label} | existing]
      end)
      new_state = %{state | edges: new_edges}
      {:reply, :ok, new_state}
    else
      {:reply, {:error, :node_not_found}, state}
    end
  end

  @impl true
  def handle_call({:get_node, node_id}, _from, state) do
    case Map.get(state.nodes, node_id) do
      nil -> {:reply, :not_found, state}
      data -> {:reply, {:ok, data}, state}
    end
  end

  @impl true
  def handle_call(:get_all_nodes, _from, state) do
    nodes = Map.to_list(state.nodes)
    require Logger
    Logger.info("All nodes in DAG: #{inspect(nodes)}")
    {:reply, nodes, state}
  end

  @impl true
  def handle_call({:query, {s, p, o}}, _from, state) do
    require Logger
    Logger.info("Querying nodes: #{inspect(Map.keys(state.nodes))} with pattern {#{s}, #{p}, #{o}}")
    matches = Enum.filter(Map.keys(state.nodes), fn node_id ->
      case Map.get(state.nodes, node_id) do
        data when is_map(data) ->
          s1 = Map.get(data, :subject)
          p1 = Map.get(data, :predicate)
          o1 = Map.get(data, :object)
          Logger.info("Comparing node #{node_id}: s1=#{inspect(s1)}, p1=#{inspect(p1)}, o1=#{inspect(o1)} with s=#{inspect(s)}, p=#{inspect(p)}, o=#{inspect(o)}")
          (s == nil or s == s1) and (p == nil or p == p1) and (o == nil or o == o1)
        _ ->
          Logger.info("Node #{node_id} data is not a map")
          false
      end
    end)
    Logger.info("Matched nodes: #{inspect(matches)}")
    results = Enum.map(matches, fn id ->
      {id, data} = {id, Map.get(state.nodes, id)}
      edges = Map.get(state.edges, id, []) |> Enum.map(fn {to_id, label} -> {id, to_id, label} end)
      {id, data, edges}
    end)
    Logger.info("Query results: #{inspect(results)}")
    {:reply, {:ok, results}, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :ok
  end
end

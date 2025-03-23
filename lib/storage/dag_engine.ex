defmodule Kylix.Storage.DAGEngine do
  # GenServer สำหรับจัดการกราฟแบบ DAG (Directed Acyclic Graph)
  use GenServer

  # ชื่อตารางสำหรับเก็บโหนดและเส้นเชื่อม
  @table :dag_nodes
  @edge_table :dag_edges

  # เริ่มต้น GenServer สำหรับ DAG Engine
  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  # เพิ่มโหนดใหม่เข้าไปในกราฟ โดยระบุ node_id และข้อมูล
  def add_node(node_id, data), do: GenServer.call(__MODULE__, {:add_node, node_id, data})

  # เพิ่มเส้นเชื่อมระหว่างโหนด from_id ไปยัง to_id พร้อมกำหนดป้ายกำกับ
  def add_edge(from_id, to_id, label),
    do: GenServer.call(__MODULE__, {:add_edge, from_id, to_id, label})

  # ดึงข้อมูลของโหนดตาม node_id ที่ระบุ
  def get_node(node_id), do: GenServer.call(__MODULE__, {:get_node, node_id})
  # ดึงข้อมูลของโหนดทั้งหมดในกราฟ
  def get_all_nodes(), do: GenServer.call(__MODULE__, :get_all_nodes)

  # ค้นหาโหนดตามรูปแบบที่กำหนด (pattern matching)
  def query(pattern), do: GenServer.call(__MODULE__, {:query, pattern})

  @impl true
  def init(_args) do
    # สร้างตาราง ets สำหรับโหนดและขอบ
    :ets.new(@table, [:set, :named_table, :protected])
    :ets.new(@edge_table, [:bag, :named_table, :protected])

    # Add indexes for common query patterns
    :ets.new(:subject_index, [:bag, :named_table, :protected])
    :ets.new(:predicate_index, [:bag, :named_table, :protected])
    :ets.new(:object_index, [:bag, :named_table, :protected])

    {:ok, %{}}
  end

  @impl true
  def handle_call({:add_node, node_id, data}, _from, state) do
    require Logger
    Logger.info("Adding node #{node_id} with data: #{inspect(data)}")

    unless is_map(data) do
      Logger.error("Data for node #{node_id} is not a map: #{inspect(data)}")
      {:reply, {:error, :invalid_data}, state}
    else
      :ets.insert(@table, {node_id, data})

      # Update indexes
      if Map.has_key?(data, :subject) do
        :ets.insert(:subject_index, {data.subject, node_id})
      end

      if Map.has_key?(data, :predicate) do
        :ets.insert(:predicate_index, {data.predicate, node_id})
      end

      if Map.has_key?(data, :object) do
        :ets.insert(:object_index, {data.object, node_id})
      end

      Logger.info("After insert, node #{node_id} data: #{inspect(:ets.lookup(@table, node_id))}")
      {:reply, :ok, state}
    end
  end

  @impl true
  def handle_call({:add_edge, from_id, to_id, label}, _from, state) do
    if :ets.member(@table, from_id) and :ets.member(@table, to_id) do
      :ets.insert(@edge_table, {{from_id, to_id}, label})
      {:reply, :ok, state}
    else
      {:reply, {:error, :node_not_found}, state}
    end
  end

  @impl true
  def handle_call({:get_node, node_id}, _from, state) do
    case :ets.lookup(@table, node_id) do
      [{^node_id, data}] -> {:reply, {:ok, data}, state}
      [] -> {:reply, :not_found, state}
    end
  end

  @impl true
  def handle_call(:get_all_nodes, _from, state) do
    nodes = :ets.tab2list(@table)
    require Logger
    Logger.info("All nodes in DAG: #{inspect(nodes)}")
    {:reply, nodes, state}
  end

  @impl true
  def handle_call({:query, pattern}, _from, state) do
    require Logger

    # Extract the components of the pattern properly
    {s, p, o} = pattern

    Logger.info("Querying with pattern {#{inspect(s)}, #{inspect(p)}, #{inspect(o)}}")

    # Get matched nodes using indexes when possible
    matched_nodes = cond do
      # Subject is specified - use subject index (most selective)
      s != nil ->
        :ets.lookup(:subject_index, s)
        |> Enum.map(fn {_, node_id} ->
          case :ets.lookup(@table, node_id) do
            [{^node_id, data}] -> {node_id, data}
            _ -> nil
          end
        end)
        |> Enum.filter(&(&1 != nil))

      # Object is specified - use object index (second most selective)
      o != nil ->
        :ets.lookup(:object_index, o)
        |> Enum.map(fn {_, node_id} ->
          case :ets.lookup(@table, node_id) do
            [{^node_id, data}] -> {node_id, data}
            _ -> nil
          end
        end)
        |> Enum.filter(&(&1 != nil))

      # Predicate is specified - use predicate index (least selective)
      p != nil ->
        :ets.lookup(:predicate_index, p)
        |> Enum.map(fn {_, node_id} ->
          case :ets.lookup(@table, node_id) do
            [{^node_id, data}] -> {node_id, data}
            _ -> nil
          end
        end)
        |> Enum.filter(&(&1 != nil))

      # No components specified - use full scan
      true ->
        :ets.tab2list(@table)
    end

    Logger.info("All matched nodes: #{inspect(matched_nodes)}")

    # Filter nodes that match the pattern
    matches =
      matched_nodes
      |> Enum.filter(fn {_node_id, data} ->
        is_map(data) and
          (s == nil or Map.get(data, :subject) == s) and
          (p == nil or Map.get(data, :predicate) == p) and
          (o == nil or Map.get(data, :object) == o)
      end)

    Logger.info("Matched nodes: #{inspect(matches)}")

    # Build the result tuples with edges
    results =
      Enum.map(matches, fn {node_id, data} ->
        # Find all edges where this node is the source
        edges =
          :ets.match_object(@edge_table, {{node_id, :_}, :_})
          |> Enum.map(fn {{from, to}, label} -> {from, to, label} end)

        {node_id, data, edges}
      end)

    Logger.info("Query results: #{inspect(results)}")
    {:reply, {:ok, results}, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :ets.delete(@table)
    :ets.delete(@edge_table)
    :ets.delete(:subject_index)
    :ets.delete(:predicate_index)
    :ets.delete(:object_index)
    :ok
  end
end

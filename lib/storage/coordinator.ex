defmodule Kylix.Storage.Coordinator do
  @moduledoc """
  Graph of accepted Transactions.

  Accept writes this graph; lineage reads it. The adapter is chosen at
  runtime from application config — one adapter at a time.
  """

  @doc false
  def adapter_module do
    case Application.fetch_env!(:kylix, :persist_adapter) do
      :memory -> Kylix.Storage.DAGEngine
      :disk -> Kylix.Storage.PersistentDAGEngine
    end
  end

  def add_node(node_id, data) do
    adapter_module().add_node(node_id, data)
  end

  def add_edge(from_id, to_id, label) do
    adapter_module().add_edge(from_id, to_id, label)
  end

  def get_node(node_id) do
    adapter_module().get_node(node_id)
  end

  def get_all_nodes do
    adapter_module().get_all_nodes()
  end

  def query(pattern) do
    adapter_module().query(pattern)
  end
end

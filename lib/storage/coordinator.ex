defmodule Kylix.Storage.Coordinator do
  @moduledoc """
  In-memory graph of accepted Transactions.

  Accept writes this graph; lineage reads it.
  """

  def add_node(node_id, data) do
    Kylix.Storage.DAGEngine.add_node(node_id, data)
  end

  def add_edge(from_id, to_id, label) do
    Kylix.Storage.DAGEngine.add_edge(from_id, to_id, label)
  end

  def get_node(node_id) do
    Kylix.Storage.DAGEngine.get_node(node_id)
  end

  def get_all_nodes do
    Kylix.Storage.DAGEngine.get_all_nodes()
  end

  def query(pattern) do
    Kylix.Storage.DAGEngine.query(pattern)
  end
end

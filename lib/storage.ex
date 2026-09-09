defmodule Kylix.Storage do
  @moduledoc false

  @doc false
  def adapter_module do
    case Application.fetch_env!(:kylix, :persist_adapter) do
      :memory -> Kylix.Storage.DAGEngine
      :disk -> Kylix.Storage.PersistentDAGEngine
    end
  end

  @doc false
  def add_node(node_id, data), do: adapter_module().add_node(node_id, data)

  @doc false
  def add_edge(from_id, to_id, label), do: adapter_module().add_edge(from_id, to_id, label)

  @doc false
  def query(pattern), do: adapter_module().query(pattern)
end


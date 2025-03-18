defmodule Kylix.Application do
  use Application

  def start(_type, _args) do
    # Get config values
    db_path = Application.get_env(:kylix, :db_path, "data/dag_storage")
    port = Application.get_env(:kylix, :port, 4040)
    node_id = Application.get_env(:kylix, :node_id, "kylix-node")
    validators_dir = Application.get_env(:kylix, :validators_dir, "config/validators")

    # Create required directories if not in test environment
    unless Mix.env() == :test do
      File.mkdir_p!(db_path)
    end

    # Load validators (but in test mode, this will be overridden by hardcoded values)
    validators =
      validators_dir
      |> File.ls!()
      |> Enum.filter(&String.ends_with?(&1, ".pub"))
      |> Enum.map(&Path.rootname/1)

    # Choose different children based on environment
    children = if Mix.env() == :test do
      [
        # For tests, use DAGEngine
        {Kylix.Storage.DAGEngine, []},
        {Kylix.BlockchainServer, [validators: validators, config_dir: validators_dir]},
        {Kylix.Network.ValidatorNetwork, [port: port, node_id: node_id]}
      ]
    else
      [
        {Kylix.Storage.PersistentDAGEngine, [db_path: db_path]},
        {Kylix.BlockchainServer, [validators: validators, config_dir: validators_dir]},
        {Kylix.Network.ValidatorNetwork, [port: port, node_id: node_id]}
      ]
    end

    opts = [strategy: :rest_for_one, name: Kylix.Supervisor]
    Supervisor.start_link(children, opts)
  end
end

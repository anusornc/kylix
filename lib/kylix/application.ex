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

    # Start both storage engines in all environments
    children = [
      # Always start the in-memory DAGEngine for fast queries/caching
      {Kylix.Storage.DAGEngine, []},

      # In non-test mode, also start the persistent engine
      if Mix.env() != :test do
        {Kylix.Storage.PersistentDAGEngine, [db_path: db_path]}
      end,

      # Common services across all environments
      {Kylix.BlockchainServer, [validators: validators, config_dir: validators_dir]},
      {Kylix.Network.ValidatorNetwork, [port: port, node_id: node_id]}
    ]
    |> Enum.filter(&(&1 != nil)) # Filter out nil entries from the if condition

    opts = [strategy: :rest_for_one, name: Kylix.Supervisor]
    Supervisor.start_link(children, opts)
  end
end

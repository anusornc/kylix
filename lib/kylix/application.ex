defmodule Kylix.Application do
  use Application

  def start(_type, _args) do
    config = load_config()

    setup_environment(config.db_path)
    validators = load_validators(config.validators_dir)

    Kylix.Storage.Coordinator.init_cache()

    opts = [strategy: :rest_for_one, name: Kylix.Supervisor]
    Supervisor.start_link(children(config, validators), opts)
  end

  defp load_config do
    %{
      db_path: Application.get_env(:kylix, :db_path, "data/dag_storage"),
      port: Application.get_env(:kylix, :port, 4040),
      node_id: Application.get_env(:kylix, :node_id, "kylix-node"),
      validators_dir: Application.get_env(:kylix, :validators_dir, "config/validators"),
      api_port: Application.get_env(:kylix, :api_port, 4000)
    }
  end

  defp setup_environment(db_path) do
    unless Mix.env() == :test do
      File.mkdir_p!(db_path)
    end
  end

  defp load_validators(validators_dir) do
    if Mix.env() == :test or !File.dir?(validators_dir) do
      []
    else
      validators_dir
      |> File.ls!()
      |> Enum.filter(&String.ends_with?(&1, ".pub"))
      |> Enum.map(&Path.rootname/1)
    end
  end

  defp children(config, validators) do
    [
      {Kylix.Storage.DAGEngine, []},
      if Mix.env() != :test do
        {Kylix.Storage.PersistentDAGEngine, [db_path: config.db_path]}
      end,
      {Kylix.Consensus.ValidatorCoordinator,
       [validators: validators, config_dir: config.validators_dir]},
      {Kylix.BlockchainServer, [validators: validators, config_dir: config.validators_dir]},
      {Kylix.Network.ValidatorNetwork, [port: config.port, node_id: config.node_id]},
      {Kylix.Server.TransactionQueue, []},
      {Kylix.Storage.CacheSyncJob, []},
      if Mix.env() != :test do
        {Kylix.API.Server, [port: config.api_port]}
      end
    ]
    |> Enum.filter(&(&1 != nil))
  end
end

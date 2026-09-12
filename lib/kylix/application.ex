defmodule Kylix.Application do
  use Application

  def start(_type, _args) do
    config = load_config()

    setup_environment(config.db_path)
    validators = load_validators(config.validators_dir)

    opts = [strategy: :rest_for_one, name: Kylix.Supervisor]
    Supervisor.start_link(children(config, validators), opts)
  end

  defp load_config do
    %{
      db_path: Application.get_env(:kylix, :db_path, "data/dag_storage"),
      validators_dir: Application.get_env(:kylix, :validators_dir, "config/validators"),
      api_port: Application.get_env(:kylix, :api_port, 4000)
    }
  end

  defp setup_environment(db_path) do
    case Kylix.Storage.adapter_module() do
      Kylix.Storage.PersistentDAGEngine -> File.mkdir_p!(db_path)
      _ -> :ok
    end
  end

  defp load_validators(validators_dir) do
    if File.dir?(validators_dir) do
      validators_dir
      |> File.ls!()
      |> Enum.filter(&String.ends_with?(&1, ".pub"))
      |> Enum.map(&Path.rootname/1)
    else
      []
    end
  end

  defp children(config, validators) do
    [
      persist_child(config),
      {Kylix.BlockchainServer, [validators: validators, config_dir: config.validators_dir]},
      {Kylix.Server.TransactionQueue, []},
      if Mix.env() != :test do
        {Kylix.API.Server, [port: config.api_port]}
      end
    ]
    |> Enum.filter(&(&1 != nil))
  end

  defp persist_child(config) do
    module = Kylix.Storage.adapter_module()

    case module do
      Kylix.Storage.PersistentDAGEngine -> {module, [db_path: config.db_path]}
      _ -> {module, []}
    end
  end
end

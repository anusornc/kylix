defmodule Kylix.Application do
  use Application

  def start(_type, _args) do
    validators = [
      {"agent1", "pubkey1"},
      {"agent2", "pubkey2"},
      {"agent3", "pubkey3"}
    ]
    children = [
      {Kylix.Storage.DAGEngine, []},
      {Kylix.BlockchainServer, [validators: validators]}
    ]
    # Change strategy to :rest_for_one
    opts = [strategy: :rest_for_one, name: Kylix.Supervisor]
    Supervisor.start_link(children, opts)
  end
end

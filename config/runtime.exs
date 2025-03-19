import Config

if config_env() == :prod do
  # Allow runtime configuration in production
  # This will let you set configuration via environment variables

  # Example: DATABASE_PATH=xxx mix run
  if db_path = System.get_env("DATABASE_PATH") do
    config :kylix, db_path: db_path
  end

  if network_peers = System.get_env("NETWORK_PEERS") do
    peers = String.split(network_peers, ",")
    config :kylix, :network, peers: peers
  end
end

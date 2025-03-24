import Config

config :kylix,
  # Production settings
  db_path: "/data/dag_storage",
  port: System.get_env("PORT", "4040") |> String.to_integer(),
  node_id: System.get_env("NODE_ID", "kylix-node")

# Production logging
config :logger, level: :info

# Additional production settings
config :kylix, :performance,
  cache_size: 1000,
  checkpoint_interval: 3600  # in seconds

# config/dev.exs
import Config

config :kylix,
  db_path: "data/dev/dag_storage",
  # Development port
  port: 4040,
  node_id: "kylix-dev-node",
  validators_dir: "config/validators",
  clientwallet: "config/client_wallets"

# Development logging configuration
config :logger, :console,
  format: "[$level] $message\n",
  level: :debug

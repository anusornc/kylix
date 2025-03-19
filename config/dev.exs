# config/dev.exs
import Config

config :kylix,
  db_path: "data/dev/dag_storage",
  port: 4040,  # Development port
  node_id: "kylix-dev-node",
  validators_dir: "config/validators"

# Development logging configuration
config :logger, :console,
  format: "[$level] $message\n",
  level: :debug

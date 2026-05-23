# config/test.exs
import Config

config :kylix,
  db_path: "data/test/dag_storage",
  # Different port for tests
  port: 4050,
  node_id: "kylix-test-node",
  validators_dir: "config/validators",
  clientwallet: "config/client_wallets"

# Quiet logging in tests
config :logger, level: :warning

# config/test.exs
import Config

config :kylix,
  db_path: "data/test/dag_storage",
  port: 4050,  # Different port for tests
  node_id: "kylix-test-node",
  validators_dir: "config/validators"

# Quiet logging in tests
config :logger, level: :warning

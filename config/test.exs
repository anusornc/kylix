# config/test.exs
import Config

config :kylix,
  persist_adapter: :memory,
  db_path: "data/test/dag_storage",
  validators_dir: "config/validators",
  clientwallet: "config/client_wallets"

# Quiet logging in tests
config :logger, level: :warning

import Config

config :kylix,
  db_path: "data/dag_storage",
  validators_dir: System.get_env("VALIDATORS_DIR", "config/validators")

# Production logging
config :logger, level: :info

# Additional production settings
config :kylix, :performance,
  cache_size: 1000,
  checkpoint_interval: 3600  # in seconds

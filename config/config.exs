# config/config.exs

import Config

config :kylix, persist_adapter: :disk

# Import environment specific config
import_config "#{config_env()}.exs"

# config/config.exs

import Config

# Common configuration for all environments

# Import environment specific config
import_config "#{config_env()}.exs"

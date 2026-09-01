defmodule Kylix.API.Dashboard do
  @moduledoc """
  Simple HTML dashboard for Kylix blockchain explorer
  """

  require EEx

  @external_resource "priv/templates/dashboard.html.eex"
  EEx.function_from_file(:def, :render, "priv/templates/dashboard.html.eex", [])
end

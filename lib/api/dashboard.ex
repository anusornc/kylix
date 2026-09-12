defmodule Kylix.API.Dashboard do
  @moduledoc """
  HTML dashboard: accept a Transaction and ask a lineage question.
  """

  require EEx

  @external_resource "priv/templates/dashboard.html.eex"
  EEx.function_from_file(:def, :render, "priv/templates/dashboard.html.eex", [])
end

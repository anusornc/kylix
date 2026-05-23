defmodule Kylix.API.Dashboard do
  @moduledoc """
  Simple HTML dashboard for Kylix blockchain explorer
  """

  @external_resource Path.join(__DIR__, "dashboard.html")
  @html File.read!(Path.join(__DIR__, "dashboard.html"))

  def render do
    @html
  end
end

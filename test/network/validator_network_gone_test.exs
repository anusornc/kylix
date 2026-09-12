defmodule Kylix.Network.ValidatorNetworkGoneTest do
  use ExUnit.Case, async: true

  test "network module is not loaded" do
    refute Code.ensure_loaded?(Kylix.Network.ValidatorNetwork)
  end
end

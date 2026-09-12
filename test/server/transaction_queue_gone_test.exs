defmodule Kylix.Server.TransactionQueueGoneTest do
  use ExUnit.Case, async: true

  test "queue module is not loaded" do
    refute Code.ensure_loaded?(Kylix.Server.TransactionQueue)
  end

  test "async facade is not public" do
    refute function_exported?(Kylix, :add_transaction_async, 5)
    refute function_exported?(Kylix, :get_queue_status, 0)
  end
end

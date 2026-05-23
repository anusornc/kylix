defmodule Kylix.Storage.CacheSyncJobTest do
  use ExUnit.Case, async: false

  # Ensure the mecks are removed when tests finish
  setup do
    on_exit(fn ->
      :meck.unload()
    end)
    :ok
  end

  test "sync cache called in non-test mode" do
    # We will use the approach of stubbing Mix in a safe way and using send
    # The instructions specifically state: "Testing requires sending a message to the GenServer and mocking Kylix.Storage.Coordinator.sync_cache() to assert it gets called."

    # Mock Coordinator to verify it gets called
    :meck.new(Kylix.Storage.Coordinator, [:passthrough])
    :meck.expect(Kylix.Storage.Coordinator, :sync_cache, fn -> {:ok, 10} end)

    # Mock Mix.env() to simulate non-test environment
    :meck.new(Mix, [:passthrough])
    :meck.expect(Mix, :env, fn -> :prod end)

    # Send the sync message to the GenServer
    send(Kylix.Storage.CacheSyncJob, :sync)

    # Synchronize with the GenServer to wait for it to process the message
    # This avoids arbitrary Process.sleep() and ensures test stability
    :sys.get_state(Kylix.Storage.CacheSyncJob)

    # Verify that sync_cache was called
    assert :meck.validate(Kylix.Storage.Coordinator)
    assert :meck.called(Kylix.Storage.Coordinator, :sync_cache, [])
  end

  test "sync cache is skipped in test mode" do
    # Verify Mix.env() is :test (which is the default in ExUnit)
    assert Mix.env() == :test

    # Mock Coordinator to verify it DOES NOT get called
    :meck.new(Kylix.Storage.Coordinator, [:passthrough])
    :meck.expect(Kylix.Storage.Coordinator, :sync_cache, fn -> {:ok, 10} end)

    # Send the sync message to the GenServer
    send(Kylix.Storage.CacheSyncJob, :sync)

    # Synchronize with the GenServer to wait for it to process the message
    :sys.get_state(Kylix.Storage.CacheSyncJob)

    # Verify that sync_cache was NOT called
    assert :meck.validate(Kylix.Storage.Coordinator)
    refute :meck.called(Kylix.Storage.Coordinator, :sync_cache, [])
  end
end

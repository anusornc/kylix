defmodule Kylix.Storage.CacheSyncJobTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog
  require Logger

  alias Kylix.Storage.CacheSyncJob

  setup do
    # Only try to unload if it was actually mocked by this test to prevent :not_mocked error
    try do
      :meck.unload(Kylix.Storage.Coordinator)
    catch
      _, _ -> :ok
    end

    :meck.new(Kylix.Storage.Coordinator, [:passthrough])
    :meck.expect(Kylix.Storage.Coordinator, :sync_cache, fn -> {:ok, 42} end)

    # Store original logger level
    orig_level = Logger.level()
    Logger.configure(level: :info)

    on_exit(fn ->
      try do
        :meck.unload(Kylix.Storage.Coordinator)
      catch
        _, _ -> :ok
      end
      Application.delete_env(:kylix, :enable_cache_sync)
      Logger.configure(level: orig_level)
    end)

    :ok
  end

  test "init/1 schedules initial sync after 1000ms" do
    assert {:ok, %{}} = CacheSyncJob.init([])
    assert_receive :sync, 1500
  end

  test "handle_info(:sync, state) logs sync output and reschedules when sync is enabled" do
    Application.put_env(:kylix, :enable_cache_sync, true)

    log = capture_log(fn ->
      assert {:noreply, %{}} = CacheSyncJob.handle_info(:sync, %{})
    end)

    assert log =~ "Running scheduled cache synchronization"
    assert log =~ "Synchronized 42 nodes to in-memory cache"
  end

  test "handle_info(:sync, state) does not run sync when sync is disabled" do
    Application.put_env(:kylix, :enable_cache_sync, false)

    log = capture_log(fn ->
      assert {:noreply, %{}} = CacheSyncJob.handle_info(:sync, %{})
    end)

    refute log =~ "Running scheduled cache synchronization"
    refute log =~ "Synchronized 42 nodes to in-memory cache"
  end
end

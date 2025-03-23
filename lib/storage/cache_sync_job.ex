defmodule Kylix.Storage.CacheSyncJob do
  use GenServer
  require Logger

  @sync_interval 5 * 60 * 1000 # 5 minutes
  #@is_test Mix.env() == :test

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @impl true
  def init(_) do
    # Initial sync after startup (1 second delay)
    Process.send_after(self(), :sync, 1000)
    {:ok, %{}}
  end

  @impl true
  def handle_info(:sync, state) do
    # Skip sync in test environment
    unless Mix.env() == :test do
      Logger.info("Running scheduled cache synchronization")
      case Kylix.Storage.Coordinator.sync_cache() do
        {:ok, count} -> Logger.info("Synchronized #{count} nodes to in-memory cache")
      end
    end

    # Schedule next sync
    Process.send_after(self(), :sync, @sync_interval)
    {:noreply, state}
  end
end

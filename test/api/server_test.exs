defmodule Kylix.API.ServerTest do
  use ExUnit.Case, async: false
  require Logger

  alias Kylix.API.Server

  setup do
    # Ensure any lingering processes are stopped before the test
    stop_server()
    on_exit(&stop_server/0)
    :ok
  end

  defp stop_server do
    # Call the module's stop method to shutdown cowboy gracefully
    try do
      Server.stop()
    catch
      _, _ -> :ok
    end

    if pid = Process.whereis(Kylix.API.Server) do
      if Process.alive?(pid), do: GenServer.stop(pid, :normal, 1000)
    end

    if pid = Process.whereis(Kylix.API.Supervisor) do
      if Process.alive?(pid), do: GenServer.stop(pid, :normal, 1000)
    end
  end

  describe "init/1" do
    test "initializes with the given port" do
      port = 4040
      # Using a task to isolate the linked Supervisor to the task process
      # so it gets killed when the task finishes and doesn't pollute the test process
      Task.async(fn ->
        assert {:ok, %{port: ^port}} = Server.init(port)
      end)
      |> Task.await()
    end
  end

  describe "start_link/1" do
    test "starts the server with default port" do
      assert {:ok, pid} = Server.start_link()
      assert Process.alive?(pid)

      # The server process should be registered
      assert Process.whereis(Kylix.API.Server) == pid

      # The supervisor should also be running
      assert Process.whereis(Kylix.API.Supervisor) != nil
    end

    test "starts the server with specified port" do
      assert {:ok, pid} = Server.start_link(port: 4050)
      assert Process.alive?(pid)

      # Verify the state contains the port
      state = :sys.get_state(pid)
      assert state.port == 4050
    end
  end
end

defmodule Kylix.API.ServerTest do
  use ExUnit.Case, async: false

  alias Kylix.API.Server

  setup do
    stop_server()

    on_exit(fn ->
      stop_server()
    end)

    :ok
  end

  defp stop_server do
    if pid = Process.whereis(Kylix.API.Server) do
      try do
        GenServer.stop(pid, :normal, 1000)
      catch
        :exit, _ -> :ok
      end
    end

    if pid = Process.whereis(Kylix.API.Supervisor) do
      try do
        Supervisor.stop(pid, :normal, 1000)
      catch
        :exit, _ -> :ok
      end
    end

    try do
      Server.stop()
    catch
      _, _ -> :ok
    end
  end

  test "start_link/1 starts the server with default port" do
    assert {:ok, pid} = Server.start_link()
    assert is_pid(pid)
    assert Process.whereis(Server) == pid
    assert Process.whereis(Kylix.API.Supervisor)

    assert :sys.get_state(pid) == %{port: 4000}
  end

  test "start_link/1 starts the server with custom port" do
    assert {:ok, pid} = Server.start_link(port: 4001)
    assert is_pid(pid)
    assert Process.whereis(Server) == pid
    assert Process.whereis(Kylix.API.Supervisor)

    assert :sys.get_state(pid) == %{port: 4001}
  end

  test "init/1 initializes state and starts Supervisor" do
    assert {:ok, %{port: 4002}} = Server.init(4002)

    sup_pid = Process.whereis(Kylix.API.Supervisor)
    assert is_pid(sup_pid)

    children = Supervisor.which_children(sup_pid)
    assert length(children) == 1
    [{_, _, :supervisor, [:ranch_embedded_sup]}] = children
  end

  test "stop/0 shuts down the Cowboy listener" do
    # we need to make sure the server is fully started
    {:ok, pid} = Server.start_link(port: 4003)

    # Wait for the initialization to complete
    _ = :sys.get_state(pid)

    # Call stop
    res = Server.stop()
    assert res == :ok or res == {:error, :not_found}
  end
end

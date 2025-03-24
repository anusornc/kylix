defmodule Kylix.API.Server do
  use GenServer
  require Logger

  @default_port 4000

  def start_link(opts \\ []) do
    port = Keyword.get(opts, :port, @default_port)
    GenServer.start_link(__MODULE__, port, name: __MODULE__)
  end

  @impl true
  def init(port) do
    Logger.info("Starting Kylix API server on port #{port}")

    children = [
      {Plug.Cowboy, scheme: :http, plug: Kylix.API.Router, options: [port: port]}
    ]

    opts = [strategy: :one_for_one, name: Kylix.API.Supervisor]
    Supervisor.start_link(children, opts)

    {:ok, %{port: port}}
  end

  def stop() do
    Plug.Cowboy.shutdown(Kylix.API.Router.HTTP)
  end
end

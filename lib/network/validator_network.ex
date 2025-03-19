defmodule Kylix.Network.ValidatorNetwork do
  @moduledoc """
  Manages communication between validators in the network.
  """

  use GenServer
  require Logger

  @default_port 4040

  # Start the network service
  def start_link(opts \\ []) do
    port = Keyword.get(opts, :port, @default_port)
    node_id = Keyword.get(opts, :node_id, node_name())

    GenServer.start_link(__MODULE__, [port: port, node_id: node_id], name: __MODULE__)
  end

  @impl true
  def init(opts) do
    port = Keyword.get(opts, :port, @default_port)
    node_id = Keyword.get(opts, :node_id)

    # Start TCP listener
    {:ok, listen_socket} = :gen_tcp.listen(port, [
      :binary,
      packet: :line,
      active: false,
      reuseaddr: true
    ])

    # Start acceptor process
    spawn_link(fn -> accept_connections(listen_socket) end)

    Logger.info("Validator network started on port #{port} with node ID #{node_id}")

    {:ok, %{
      port: port,
      node_id: node_id,
      listen_socket: listen_socket,
      connections: %{},  # Map of node_id to socket
      peer_latencies: %{} # Map of node_id to latency measurements
    }}
  end

  # Broadcast a transaction to other validators
  def broadcast_transaction(tx_data) do
    GenServer.cast(__MODULE__, {:broadcast, :transaction, tx_data})
  end

  # Connect to another validator
  def connect(host, port) do
    GenServer.call(__MODULE__, {:connect, host, port})
  end

  # Get list of connected validators
  def get_peers() do
    GenServer.call(__MODULE__, :get_peers)
  end

  # Monitor an active connection
  defp monitor_connection(socket, peer_id) do
    # Set socket to active mode for this process
    :inet.setopts(socket, [active: true])

    # Periodically send pings to measure latency
    schedule_ping(socket, peer_id)

    receive do
      {:tcp, ^socket, data} ->
        # Process message
        GenServer.cast(__MODULE__, {:message, peer_id, data})
        monitor_connection(socket, peer_id)

      {:tcp_closed, ^socket} ->
        Logger.info("Connection to #{peer_id} closed")
        GenServer.cast(__MODULE__, {:connection_closed, peer_id})

      {:tcp_error, ^socket, reason} ->
        Logger.error("Connection error with #{peer_id}: #{reason}")
        GenServer.cast(__MODULE__, {:connection_closed, peer_id})

      :ping_time ->
        send_ping(socket, peer_id)
        schedule_ping(socket, peer_id)
        monitor_connection(socket, peer_id)
    end
  end

  defp schedule_ping(_socket, _peer_id) do
    # Send a ping every 30 seconds
    Process.send_after(self(), :ping_time, 30_000)
  end

  defp send_ping(socket, _peer_id) do
    ping = Jason.encode!(%{
      type: "ping",
      node_id: node_name(),
      timestamp: DateTime.utc_now() |> DateTime.to_iso8601()
    })
    :gen_tcp.send(socket, ping <> "\n")
  end

  defp measure_latency(socket) do
    # Send ping and measure time until pong
    start_time = System.monotonic_time(:millisecond)
    send_ping(socket, nil)

    # Wait for pong
    case :gen_tcp.recv(socket, 0, 5000) do
      {:ok, data} ->
        response = Jason.decode!(data)
        if response["type"] == "pong" do
          end_time = System.monotonic_time(:millisecond)
          end_time - start_time
        else
          1000 # Assume high latency if wrong response
        end

      {:error, _} ->
        2000 # Assume very high latency on timeout
    end
  end

  defp calculate_latency({:ok, timestamp, _offset}) do
    current_time = DateTime.utc_now()
    DateTime.diff(current_time, timestamp, :millisecond)
  end

  defp calculate_latency(_error) do
    1000 # Default latency if timestamp parsing fails
  end

  defp node_name do
    # Generate a node name if not explicitly provided
    System.get_env("NODE_ID") || "node-#{:rand.uniform(100000)}"
  end

  # Accept incoming connections
  defp accept_connections(listen_socket) do
    case :gen_tcp.accept(listen_socket) do
      {:ok, client_socket} ->
        # Spawn a new process to handle this client
        spawn_link(fn -> handle_client(client_socket) end)
        # Continue accepting connections
        accept_connections(listen_socket)

      {:error, reason} ->
        Logger.error("Failed to accept connection: #{reason}")
        # Retry after a delay
        Process.sleep(1000)
        accept_connections(listen_socket)
    end
  end



  @impl true
  def handle_call({:connect, host, port}, _from, state) do
    case :gen_tcp.connect(to_charlist(host), port, [:binary, packet: :line, active: false]) do
      {:ok, socket} ->
        # Send handshake with our node_id
        handshake = Jason.encode!(%{type: "handshake", node_id: state.node_id})
        :ok = :gen_tcp.send(socket, handshake <> "\n")

        # Receive response
        {:ok, response} = :gen_tcp.recv(socket, 0)
        peer_info = Jason.decode!(response)
        peer_id = peer_info["node_id"]

        # Start monitoring this connection
        spawn_link(fn -> monitor_connection(socket, peer_id) end)

        # Update connections
        new_connections = Map.put(state.connections, peer_id, socket)

        # Measure initial latency
        latency = measure_latency(socket)
        new_latencies = Map.put(state.peer_latencies, peer_id, latency)

        Logger.info("Connected to validator #{peer_id} with latency #{latency}ms")

        {:reply, {:ok, peer_id}, %{state | connections: new_connections, peer_latencies: new_latencies}}

      {:error, reason} ->
        Logger.error("Failed to connect to #{host}:#{port}: #{reason}")
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:get_peers, _from, state) do
    peers =
      state.connections
      |> Map.keys()
      |> Enum.map(fn peer_id ->
        {peer_id, Map.get(state.peer_latencies, peer_id, 0)}
      end)

    {:reply, peers, state}
  end

  @impl true
  def handle_cast({:broadcast, :transaction, tx_data}, state) do
    message = Jason.encode!(%{
      type: "transaction",
      data: tx_data,
      timestamp: DateTime.utc_now() |> DateTime.to_iso8601()
    })

    Enum.each(state.connections, fn {peer_id, socket} ->
      case :gen_tcp.send(socket, message <> "\n") do
        :ok ->
          Logger.debug("Transaction broadcast to #{peer_id}")
        {:error, reason} ->
          Logger.error("Failed to broadcast to #{peer_id}: #{reason}")
          # Consider removing this peer if connection is broken
      end
    end)

    {:noreply, state}
  end

  @impl true
  def handle_info({:tcp, socket, data}, state) do
    # Process incoming message
    message = Jason.decode!(data)

    case message["type"] do
      "transaction" ->
        # Forward to blockchain server
        tx_data = message["data"]
        spawn(fn ->
          Kylix.BlockchainServer.receive_transaction(tx_data)
        end)

      "ping" ->
        # Respond to ping with pong
        response = Jason.encode!(%{type: "pong", timestamp: DateTime.utc_now() |> DateTime.to_iso8601()})
        :gen_tcp.send(socket, response <> "\n")

      "pong" ->
        # Update latency measurement
        sender = message["node_id"]
        timestamp = DateTime.from_iso8601(message["timestamp"])
        latency = calculate_latency(timestamp)
        new_latencies = Map.put(state.peer_latencies, sender, latency)
        {:noreply, %{state | peer_latencies: new_latencies}}

      _ ->
        Logger.warning("Unknown message type: #{message["type"]}")
    end

    {:noreply, state}
  end

  # Handle a new client connection
  defp handle_client(socket) do
    # Receive handshake
    case :gen_tcp.recv(socket, 0) do
      {:ok, data} ->
        handshake = Jason.decode!(data)
        peer_id = handshake["node_id"]

        # Send our handshake response
        response = Jason.encode!(%{
          type: "handshake_response",
          node_id: node_name(),
          status: "accepted"
        })
        :gen_tcp.send(socket, response <> "\n")

        # Update connections in the GenServer
        GenServer.cast(__MODULE__, {:new_connection, peer_id, socket})

        # Monitor this connection
        monitor_connection(socket, peer_id)

      {:error, reason} ->
        Logger.error("Handshake failed: #{reason}")
        :gen_tcp.close(socket)
    end
  end
end

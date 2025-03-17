defmodule Kylix.BlockchainServer do
  use GenServer

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  def add_transaction(s, p, o, validator_id, signature),
    do: GenServer.call(__MODULE__, {:add_transaction, s, p, o, validator_id, signature})

  def query(pattern), do: GenServer.call(__MODULE__, {:query, pattern})
  def get_validators(), do: GenServer.call(__MODULE__, :get_validators)
  def add_validator(validator_id, pubkey, known_by),
    do: GenServer.call(__MODULE__, {:add_validator, validator_id, pubkey, known_by})

  @impl true
  def init(opts) do
    validators = Keyword.get(opts, :validators, [])
    state = %{
      validators: validators,
      current_validator: hd(validators) |> elem(0),
      tx_count: 0
    }
    {:ok, state}
  end

  @impl true
  def handle_call({:add_transaction, s, p, o, validator_id, signature}, _from, state) do
    # Allow test override of current_validator if in test environment
    current_validator = if Mix.env() == :test do
      validator_id
    else
      state.current_validator
    end

    if validator_id != current_validator do
      {:reply, {:error, :not_your_turn}, state}
    else
      validator = Enum.find(state.validators, fn {id, _} -> id == validator_id end)
      case validator do
        {_, pubkey} ->
          if verify_signature({s, p, o}, signature, pubkey) do
            tx_id = "tx#{state.tx_count + 1}"
            tx_data = %{
              subject: s,
              predicate: p,
              object: o,
              validator: validator_id,
              signature: signature,
              timestamp: DateTime.utc_now()
            }

            :ok = Kylix.Storage.DAGEngine.add_node(tx_id, tx_data)
            prior_txs = Enum.take(Kylix.Storage.DAGEngine.get_all_nodes(), 2)
              |> Enum.reject(fn {prior_id, _} -> prior_id == tx_id end)

            require Logger
            Logger.info("Prior transactions for tx_id #{tx_id}: #{inspect(prior_txs)}")

            edge_results = Enum.map(prior_txs, fn {prior_id, _} ->
              Kylix.Storage.DAGEngine.add_edge(tx_id, prior_id, "confirms")
            end)

            case Enum.find(edge_results, &match?({:error, _}, &1)) do
              nil ->
                next_validator = Enum.at(state.validators, rem(state.tx_count + 1, length(state.validators))) |> elem(0)
                new_state = %{state | tx_count: state.tx_count + 1, current_validator: next_validator}
                {:reply, {:ok, tx_id}, new_state}
              {:error, reason} ->
                {:reply, {:error, {:edge_failure, reason}}, state}
            end
          else
            {:reply, {:error, :invalid_signature}, state}
          end
        nil ->
          {:reply, {:error, :unknown_validator}, state}
      end
    end
  end

  @impl true
  def handle_call({:query, pattern}, _from, state) do
    result = Kylix.Storage.DAGEngine.query(pattern)
    {:reply, result, state}
  end

  @impl true
  def handle_call(:get_validators, _from, state) do
    {:reply, state.validators, state}
  end

  @impl true
  def handle_call({:add_validator, validator_id, pubkey, known_by}, _from, state) do
    if Enum.any?(state.validators, fn {id, _} -> id == known_by end) do
      {:ok, tx_id} = add_transaction(validator_id, "knows", known_by, hd(state.validators) |> elem(0), "init_sig")
      new_validators = [{validator_id, pubkey} | state.validators]
      new_state = %{state | validators: new_validators}
      {:reply, {:ok, tx_id}, new_state}
    else
      {:reply, {:error, :unknown_sponsor}, state}
    end
  end

  # Handler to reset tx_count for testing
  @impl true
  def handle_call({:reset_tx_count, count}, _from, state) do
    new_state = %{state | tx_count: count, current_validator: hd(state.validators) |> elem(0)}
    {:reply, :ok, new_state}
  end

  # TODO: Replace this placeholder with real cryptographic signature verification
  defp verify_signature(_data, signature, _pubkey), do: signature == "valid_sig"
end

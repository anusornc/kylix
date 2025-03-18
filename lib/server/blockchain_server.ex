defmodule Kylix.BlockchainServer do
  use GenServer

  # Start the Blockchain Server with given options
  # The server will manage transactions and validator information
  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts \\ []) do
    validators = Keyword.get(opts, :validators, ["agent1", "agent2"])
    GenServer.start_link(__MODULE__, [validators: validators], name: __MODULE__)
  end

  # Add a new transaction to the blockchain with the given subject, predicate, object
  # Requires a valid validator_id and signature to be accepted
  # Returns {:ok, tx_id} if successful or an error tuple
  @spec add_transaction(any(), any(), any(), String.t(), String.t()) ::
        {:ok, String.t()} | {:error, atom()}
  def add_transaction(s, p, o, validator_id, signature) do
    GenServer.call(__MODULE__, {:add_transaction, s, p, o, validator_id, signature})
  end

  # Query the blockchain for transactions matching the given pattern
  # Pattern is a tuple in the form {subject, predicate, object} where nil acts as a wildcard
  # Returns {:ok, results} where results is a list of matching transactions
  @spec query({any(), any(), any()}) :: {:ok, [tuple()]}
  def query(pattern) do
    GenServer.call(__MODULE__, {:query, pattern})
  end

  # Get the list of current validators in the blockchain
  # Returns a list of validator identifiers
  @spec get_validators() :: [String.t()]
  def get_validators() do
    GenServer.call(__MODULE__, :get_validators)
  end

  # Add a new validator to the blockchain
  # validator_id: unique identifier for the new validator
  # pubkey: public key of the new validator
  # known_by: existing validator that vouches for the new one
  # Returns {:ok, validator_id} if successful or an error tuple
  @spec add_validator(String.t(), String.t(), String.t()) ::
        {:ok, String.t()} | {:error, atom()}
  def add_validator(validator_id, pubkey, known_by) do
    GenServer.call(__MODULE__, {:add_validator, validator_id, pubkey, known_by})
  end

  # Reset the transaction counter to a specific value (used for testing)
  # This is not part of the public API and should only be used in test scenarios
  @spec reset_tx_count(non_neg_integer()) :: :ok
  def reset_tx_count(count) do
    GenServer.call(__MODULE__, {:reset_tx_count, count})
  end

  # Initialize the server state with transaction count and validators
  @impl true
  def init(opts) do
    validators = Keyword.get(opts, :validators, ["agent1", "agent2"])
    {:ok, %{tx_count: 0, validators: validators}}
  end

  # Handle transaction addition request
  # Validates the requesting validator and adds the transaction to the DAG
  @impl true
  def handle_call({:add_transaction, s, p, o, validator_id, signature}, _from, state) do
    current_validator = Enum.at(state.validators, rem(state.tx_count, length(state.validators)))
    case Enum.find(state.validators, fn v -> v == validator_id end) do
      nil ->
        # Validator is not recognized
        {:reply, {:error, :unknown_validator}, state}
      ^current_validator ->
        # Valid validator and it's their turn
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

        # Link to previous transaction if not the first one
        if state.tx_count > 0 do
          :ok = Kylix.Storage.DAGEngine.add_edge(tx_id, "tx#{state.tx_count}", "confirms")
        end

        new_state = %{state | tx_count: state.tx_count + 1}
        {:reply, {:ok, tx_id}, new_state}
      _ ->
        # Valid validator but not their turn
        {:reply, {:error, :not_your_turn}, state}
    end
  end

  # Handle query request
  # Forwards the query to the DAG Engine and returns results
  @impl true
  def handle_call({:query, pattern}, _from, state) do
    {:reply, Kylix.Storage.DAGEngine.query(pattern), state}
  end

  # Return the list of current validators
  @impl true
  def handle_call(:get_validators, _from, state) do
    {:reply, state.validators, state}
  end

  # Add a new validator if vouched for by an existing validator
  @impl true
  def handle_call({:add_validator, validator_id, _pubkey, known_by}, _from, state) do
    # Verify that known_by is an existing validator
    case Enum.find(state.validators, fn v -> v == known_by end) do
      nil ->
        # The vouching validator doesn't exist
        {:reply, {:error, :unknown_validator}, state}
      _ ->
        # Add the new validator to the list
        new_validators = [validator_id | state.validators]
        new_state = %{state | validators: new_validators}
        {:reply, {:ok, validator_id}, new_state}
    end
  end

  # Reset the transaction counter (for testing purposes)
  @impl true
  def handle_call({:reset_tx_count, count}, _from, state) do
    new_state = %{state | tx_count: count}
    {:reply, :ok, new_state}
  end
end

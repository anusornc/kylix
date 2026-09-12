defmodule Kylix.BlockchainServer do
  use GenServer
  require Logger

  @config_dir "config/validators"
  # Hardcode test validators
  @test_validators ["agent1", "agent2"]

  # Start the Blockchain Server with given options
  # The server will manage transactions and validator information
  @spec start_link(keyword()) :: {:ok, pid()} | {:error, term()}
  def start_link(opts \\ []) do
    validators = Keyword.get(opts, :validators, [])
    config_dir = Keyword.get(opts, :config_dir, @config_dir)

    GenServer.start_link(__MODULE__, [validators: validators, config_dir: config_dir],
      name: __MODULE__
    )
  end

  # Add a new transaction to the blockchain with the given subject, predicate, object
  # Requires a valid validator_id and signature to be accepted
  # Returns {:ok, tx_id} if successful or an error tuple
  @spec add_transaction(any(), any(), any(), String.t(), String.t()) ::
          {:ok, String.t()} | {:error, atom()}
  def add_transaction(s, p, o, validator_id, signature) do
    GenServer.call(__MODULE__, {:add_transaction, s, p, o, validator_id, signature})
  end

  # Get the current transaction count (equivalent to the current round)
  @spec get_tx_count() :: non_neg_integer()
  def get_tx_count() do
    GenServer.call(__MODULE__, :get_tx_count)
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

  @impl true
  def handle_call({:add_transaction, s, p, o, validator_id, signature}, _from, state) do
    {result, new_state} = do_add_transaction(s, p, o, validator_id, signature, state)
    {:reply, result, new_state}
  end

  @impl true
  def handle_call(:get_tx_count, _from, state) do
    {:reply, state.tx_count, state}
  end

  @impl true
  def handle_call({:add_validator, validator_id, pubkey, known_by}, _from, state) do
    # Check if the known_by validator exists using MapSet for O(1) lookup
    if MapSet.member?(state.validator_set, known_by) do
      # Add the new validator to the list (for O(N) indexing later if needed)
      new_validators = [validator_id | state.validators]
      # Add to the validator set for O(1) membership checks
      new_validator_set = MapSet.put(state.validator_set, validator_id)
      # Add the public key
      new_public_keys = Map.put(state.public_keys, validator_id, pubkey)
      # Update state
      new_state = %{
        state
        | validators: new_validators,
          validator_set: new_validator_set,
          public_keys: new_public_keys
      }

      {:reply, {:ok, validator_id}, new_state}
    else
      {:reply, {:error, :unknown_validator}, state}
    end
  end

  @impl true
  def handle_call(:get_validators, _from, state) do
    {:reply, state.validators, state}
  end

  # Initialize the server state with transaction count and validators
  @impl true
  def init(opts) do
    validators = Keyword.get(opts, :validators, [])
    config_dir = Keyword.get(opts, :config_dir, @config_dir)

    # If we're in test mode, keep the Mix.env plant and also take start opts.
    final_validators =
      if Mix.env() == :test do
        Enum.uniq(validators ++ @test_validators)
      else
        validators
      end

    # Ensure config directory exists
    File.mkdir_p!(config_dir)

    public_keys = Kylix.Auth.SignatureVerifier.load_public_keys(config_dir)

    {:ok,
     %{
       tx_count: 0,
       validators: final_validators,
       validator_set: MapSet.new(final_validators),
       public_keys: public_keys,
       last_block_time: DateTime.utc_now()
     }}
  end

  # Shared implementation for adding transactions
  defp do_add_transaction(s, p, o, validator_id, signature, state) do
    if !MapSet.member?(state.validator_set, validator_id) do
      {{:error, :unknown_validator}, state}
    else
      # Validate RDF structure
      case validate_rdf_triple(s, p, o) do
        {:error, reason} ->
          {{:error, reason}, state}

        :ok ->
          # Validate data size to prevent DOS attacks
          if exceeds_max_size?(s, p, o) do
            {{:error, :data_too_large}, state}
          else
            # Check for duplicate transactions - using direct access to storage
            # rather than making a recursive call
            duplicate = check_duplicate_direct(s, p, o)

            if duplicate do
              # Always return duplicate_transaction error regardless of test/prod mode
              {{:error, :duplicate_transaction}, state}
            else
              # Check for PROV-O relationship validity if applicable
              case validate_prov_o_relationship(s, p, o) do
                {:error, reason} ->
                  {{:error, reason}, state}

                :ok ->
                  timestamp = DateTime.utc_now()
                  public_key = public_key_for(validator_id, state)
                  tx_hash = Kylix.Auth.SignatureVerifier.hash_transaction(s, p, o, validator_id)

                  case Kylix.Auth.SignatureVerifier.verify(tx_hash, signature, public_key) do
                    :ok ->
                      accept_verified_transaction(
                        s,
                        p,
                        o,
                        validator_id,
                        signature,
                        timestamp,
                        tx_hash,
                        state
                      )

                    {:error, reason} ->
                      Logger.warning(
                        "Invalid signature from validator #{validator_id}: #{reason}"
                      )

                      {{:error, :invalid_signature}, state}
                  end
              end
            end
          end
      end
    end
  end

  defp public_key_for(validator_id, state) do
    Map.get(state.public_keys, validator_id)
  end

  defp accept_verified_transaction(s, p, o, validator_id, signature, timestamp, tx_hash, state) do
    tx_id = "tx#{state.tx_count + 1}"

    tx_data = %{
      subject: s,
      predicate: p,
      object: o,
      validator: validator_id,
      signature: signature,
      timestamp: timestamp,
      hash: Base.encode16(tx_hash)
    }

    :ok = Kylix.Storage.add_node(tx_id, tx_data)

    if state.tx_count > 0 do
      prev_tx_id = "tx#{state.tx_count}"
      :ok = Kylix.Storage.add_edge(prev_tx_id, tx_id, "confirms")
    end

    new_state = %{state | tx_count: state.tx_count + 1, last_block_time: timestamp}
    {{:ok, tx_id}, new_state}
  end

  # Check for duplicate transactions
  defp check_duplicate_direct(s, p, o) do
    case Kylix.Storage.query({s, p, o}) do
      {:ok, []} -> false
      {:ok, _results} -> true
      # Assume no duplicates on error
      {:error, _reason} -> false
    end
  end

  # Enhancement: Add function to check for large data to prevent DOS attacks
  defp exceeds_max_size?(s, p, o) do
    # 1MB limit per field
    max_size = 1_000_000
    byte_size(s) > max_size || byte_size(p) > max_size || byte_size(o) > max_size
  end

  # Enhancement: Add validation for RDF structure
  defp validate_rdf_triple(s, p, o) do
    # Basic validation that all components exist and are strings
    valid_subject = is_binary(s) && String.length(s) > 0
    valid_predicate = is_binary(p) && String.length(p) > 0
    valid_object = is_binary(o) && String.length(o) > 0

    cond do
      !valid_subject -> {:error, :invalid_subject}
      !valid_predicate -> {:error, :invalid_predicate}
      !valid_object -> {:error, :invalid_object}
      true -> :ok
    end
  end

  # Enhancement: Add PROV-O validation for common provenance patterns
  defp validate_prov_o_relationship(s, p, o) do
    # Only validate if it's a PROV-O predicate, otherwise skip
    if String.starts_with?(p, "prov:") do
      case p do
        "prov:wasGeneratedBy" ->
          # Validate that subject is an entity and object is an activity (simplified check)
          if !String.starts_with?(s, "entity:") or !String.starts_with?(o, "activity:") do
            {:error, :invalid_provenance_relationship}
          else
            :ok
          end

        "prov:wasAttributedTo" ->
          # Validate that subject is an entity and object is
          if !String.starts_with?(s, "entity:") or !String.starts_with?(o, "agent:") do
            {:error, :invalid_provenance_relationship}
          else
            :ok
          end

        _ ->
          :ok
      end
    else
      :ok
    end
  end
end

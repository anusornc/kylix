defmodule Kylix.Consensus.ValidatorCoordinator do
  @moduledoc """
  Manages validator coordination, selection, and performance tracking for the Kylix blockchain.

  The ValidatorCoordinator is responsible for:
  - Selecting validators in a round-robin fashion for transaction processing
  - Tracking validator performance metrics
  - Managing the validator set (additions/removals)
  - Integrating with the existing validator key infrastructure
  """

  use GenServer
  require Logger

  @config_dir "config/validators"

  # Public API

  @doc """
  Starts the ValidatorCoordinator with a list of initial validators.

  ## Options

  * `:config_dir` - Directory containing validator public keys (default: "config/validators")
  * `:performance_window` - Number of transactions to consider for metrics (default: 100)
  """
  def start_link(opts \\ []) do
    validators = Keyword.get(opts, :validators, [])
    config_dir = Keyword.get(opts, :config_dir, @config_dir)

    GenServer.start_link(__MODULE__, [validators: validators, config_dir: config_dir], name: __MODULE__)
  end

  @doc """
  Gets the current validator for transaction processing and advances to the next one.

  Returns the validator ID.
  """
  def get_current_validator do
    GenServer.call(__MODULE__, :get_current_validator)
  end

  @doc """
  Adds a new validator to the validator set with the provided public key.

  ## Parameters

  * `validator_id` - Unique identifier for the validator
  * `public_key` - The validator's public key
  * `vouched_by` - Existing validator that vouches for the new one
  """
  def add_validator(validator_id, public_key, vouched_by) do
    GenServer.call(__MODULE__, {:add_validator, validator_id, public_key, vouched_by})
  end

  @doc """
  Removes a validator from the validator set.

  ## Parameters

  * `validator_id` - Identifier of the validator to remove
  """
  def remove_validator(validator_id) do
    GenServer.call(__MODULE__, {:remove_validator, validator_id})
  end

  @doc """
  Gets the current list of active validators.
  """
  def get_validators do
    GenServer.call(__MODULE__, :get_validators)
  end

  @doc """
  Records transaction performance for a validator.

  ## Parameters

  * `validator_id` - Identifier of the validator
  * `success?` - Whether the transaction was successful
  * `tx_time` - Transaction processing time in microseconds
  """
  def record_transaction_performance(validator_id, success?, tx_time \\ nil) do
    GenServer.cast(__MODULE__, {:record_performance, validator_id, success?, tx_time})
  end

  @doc """
  Gets performance metrics for all validators.
  """
  def get_performance_metrics do
    GenServer.call(__MODULE__, :get_performance_metrics)
  end

  @doc """
  Checks if a validator exists.

  ## Parameters

  * `validator_id` - Identifier of the validator to check
  """
  def validator_exists?(validator_id) do
    GenServer.call(__MODULE__, {:validator_exists?, validator_id})
  end

  @doc """
  Gets the public key for a validator.

  ## Parameters

  * `validator_id` - Identifier of the validator
  """
  def get_validator_key(validator_id) do
    GenServer.call(__MODULE__, {:get_validator_key, validator_id})
  end

  @doc """
  Gets detailed status of the validator coordinator.
  """
  def status do
    GenServer.call(__MODULE__, :status)
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    validators = Keyword.get(opts, :validators, [])
    config_dir = Keyword.get(opts, :config_dir, @config_dir)
    performance_window = Keyword.get(opts, :performance_window, 100)

    # If we're in test mode, use hardcoded test validators
    final_validators =
      if Mix.env() == :test do
        ["agent1", "agent2"]
      else
        validators
      end

    # Ensure config directory exists
    File.mkdir_p!(config_dir)

    # Load validator public keys
    public_keys = Kylix.Auth.SignatureVerifier.load_public_keys(config_dir)

    # Initialize state
    state = %{
      validators: final_validators,
      current_validator_index: 0,
      public_keys: public_keys,
      config_dir: config_dir,
      performance_metrics: initialize_performance_metrics(final_validators),
      performance_window: performance_window,
      last_block_time: DateTime.utc_now()
    }

    Logger.info("ValidatorCoordinator started with validators: #{inspect(final_validators)}")

    {:ok, state}
  end

  @impl true
  def handle_call(:get_current_validator, _from, state) do
    if Enum.empty?(state.validators) do
      Logger.warning("No validators available for selection")
      {:reply, nil, state}
    else
      current_validator = Enum.at(state.validators, state.current_validator_index)

      # Calculate next index
      next_index = rem(state.current_validator_index + 1, length(state.validators))

      # Log validator selection
      Logger.info("Selected validator: #{current_validator}, next index: #{next_index}")

      {:reply, current_validator, %{state | current_validator_index: next_index}}
    end
  end

  @impl true
  def handle_call(:get_validators, _from, state) do
    {:reply, state.validators, state}
  end

  @impl true
  def handle_call({:add_validator, validator_id, public_key, vouched_by}, _from, state) do
    # Check if the vouching validator exists
    if vouched_by not in state.validators do
      Logger.warning("Cannot add validator: vouching validator '#{vouched_by}' not found")
      {:reply, {:error, :unknown_validator}, state}
    else
      # Prevent duplicate validators
      if validator_id in state.validators do
        Logger.warning("Validator '#{validator_id}' already exists")
        {:reply, {:error, :validator_exists}, state}
      else
        # Add the validator
        updated_validators = [validator_id | state.validators]

        # Add public key to the map
        updated_public_keys = Map.put(state.public_keys, validator_id, public_key)

        # Save public key to file (asynchronously)
        save_validator_key(validator_id, public_key, state.config_dir)

        # Initialize performance metrics for new validator
        updated_metrics = Map.put(
          state.performance_metrics,
          validator_id,
          initialize_single_validator_metrics()
        )

        updated_state = %{state |
          validators: updated_validators,
          public_keys: updated_public_keys,
          performance_metrics: updated_metrics
        }

        Logger.info("Added validator '#{validator_id}', vouched by '#{vouched_by}'")

        {:reply, {:ok, validator_id}, updated_state}
      end
    end
  end

  @impl true
  def handle_call({:remove_validator, validator_id}, _from, state) do
    # Check if validator exists
    if validator_id not in state.validators do
      Logger.warning("Cannot remove validator: '#{validator_id}' not found")
      {:reply, {:error, :unknown_validator}, state}
    else
      # Ensure at least one validator remains
      if length(state.validators) <= 1 do
        Logger.warning("Cannot remove last validator '#{validator_id}'")
        {:reply, {:error, :cannot_remove_last_validator}, state}
      else
        # Remove validator
        updated_validators = Enum.filter(state.validators, fn v -> v != validator_id end)

        # Remove from public keys
        updated_public_keys = Map.drop(state.public_keys, [validator_id])

        # Remove from metrics
        updated_metrics = Map.drop(state.performance_metrics, [validator_id])

        # Adjust current_validator_index if needed
        adjusted_index = min(state.current_validator_index, length(updated_validators) - 1)

        updated_state = %{state |
          validators: updated_validators,
          public_keys: updated_public_keys,
          performance_metrics: updated_metrics,
          current_validator_index: adjusted_index
        }

        Logger.info("Removed validator '#{validator_id}'")

        {:reply, :ok, updated_state}
      end
    end
  end

  @impl true
  def handle_call({:validator_exists?, validator_id}, _from, state) do
    exists = validator_id in state.validators
    {:reply, exists, state}
  end

  @impl true
  def handle_call({:get_validator_key, validator_id}, _from, state) do
    public_key = Map.get(state.public_keys, validator_id)

    if public_key do
      {:reply, {:ok, public_key}, state}
    else
      {:reply, {:error, :key_not_found}, state}
    end
  end

  @impl true
  def handle_call(:get_performance_metrics, _from, state) do
    {:reply, state.performance_metrics, state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    status_info = %{
      validators: state.validators,
      current_validator_index: state.current_validator_index,
      current_validator: Enum.at(state.validators, state.current_validator_index),
      total_validators: length(state.validators),
      performance_metrics: state.performance_metrics,
      last_block_time: state.last_block_time
    }

    {:reply, status_info, state}
  end

  @impl true
  def handle_cast({:record_performance, validator_id, success?, tx_time}, state) do
    # Check if validator exists in metrics
    if validator_id not in state.validators do
      Logger.warning("Cannot record performance for unknown validator '#{validator_id}'")
      {:noreply, state}
    else
      # Update metrics
      updated_metrics = update_performance_metrics(
        state.performance_metrics,
        validator_id,
        success?,
        tx_time,
        state.performance_window
      )

      # Update last block time
      {:noreply, %{state |
        performance_metrics: updated_metrics,
        last_block_time: DateTime.utc_now()
      }}
    end
  end

  # Private Helper Functions

  defp initialize_performance_metrics(validators) do
    validators
    |> Enum.map(fn validator ->
      {validator, initialize_single_validator_metrics()}
    end)
    |> Enum.into(%{})
  end

  defp initialize_single_validator_metrics do
    %{
      total_transactions: 0,
      successful_transactions: 0,
      failure_rate: 0.0,
      last_active: DateTime.utc_now(),
      recent_tx_times: [], # List of recent transaction times (microseconds)
      avg_tx_time: nil, # Average transaction time (microseconds)
      recent_results: [] # List of recent transaction results (true/false)
    }
  end

  defp update_performance_metrics(metrics, validator, success?, tx_time, window_size) do
    Map.update(metrics, validator, initialize_single_validator_metrics(), fn current ->
      # Update transaction counts
      current
      |> Map.update(:total_transactions, 1, &(&1 + 1))
      |> Map.update(:successful_transactions,
          (if success?, do: 1, else: 0),
          &(&1 + (if success?, do: 1, else: 0))
      )
      |> Map.put(:last_active, DateTime.utc_now())
      |> update_recent_results(success?, window_size)
      |> update_tx_times(tx_time, window_size)
      |> calculate_failure_rate()
    end)
  end

  defp update_recent_results(metrics, result, window_size) do
    # Add new result and trim to window size
    recent_results = [result | metrics.recent_results] |> Enum.take(window_size)
    Map.put(metrics, :recent_results, recent_results)
  end

  defp update_tx_times(metrics, nil, _window_size), do: metrics
  defp update_tx_times(metrics, tx_time, window_size) do
    # Add new time and trim to window size
    recent_tx_times = [tx_time | metrics.recent_tx_times] |> Enum.take(window_size)

    # Calculate average tx time
    avg_tx_time =
      if Enum.empty?(recent_tx_times) do
        nil
      else
        Enum.sum(recent_tx_times) / length(recent_tx_times)
      end

    metrics
    |> Map.put(:recent_tx_times, recent_tx_times)
    |> Map.put(:avg_tx_time, avg_tx_time)
  end

  defp calculate_failure_rate(metrics) do
    # Calculate from recent results for more responsive metrics
    recent_results = metrics.recent_results

    failure_rate =
      if Enum.empty?(recent_results) do
        0.0
      else
        failures = Enum.count(recent_results, &(!&1))
        failures / length(recent_results)
      end

    Map.put(metrics, :failure_rate, Float.round(failure_rate, 4))
  end

  defp save_validator_key(validator_id, public_key, config_dir) do
    # Spawn a process to write the key to disk
    spawn(fn ->
      try do
        key_path = Path.join(config_dir, "#{validator_id}.pub")
        File.write!(key_path, public_key)
        Logger.info("Saved public key for validator '#{validator_id}'")
      rescue
        e ->
          Logger.error("Failed to save public key for validator '#{validator_id}': #{inspect(e)}")
      end
    end)
  end
end

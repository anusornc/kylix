# ValidatorCoordinator for Kylix Blockchain

## Overview

The `ValidatorCoordinator` is a centralized component for managing validator selection, performance tracking, and validator lifecycle in the Kylix blockchain. It implements a Provenance-based Authority Consensus (PAC) mechanism where validators are selected in a round-robin fashion to process transactions.

## Key Features

- **Round-robin Validator Selection**: Ensures fair and deterministic transaction processing across validators
- **Performance Metrics Tracking**: Monitors validator performance, including success rates and transaction times
- **Dynamic Validator Management**: Allows adding/removing validators during runtime
- **Integration with Key Infrastructure**: Works with the existing key management system

## Architecture

The `ValidatorCoordinator` is implemented as a GenServer that maintains the following state:

- List of active validators
- Current validator index for round-robin selection
- Performance metrics for each validator
- Public keys for validators

## Integration with Kylix

The `ValidatorCoordinator` integrates with the existing Kylix blockchain system:

1. It's started before the `BlockchainServer` in the application supervision tree
2. The `BlockchainServer` uses it to determine which validator is allowed to commit transactions
3. Transaction performance is recorded to build up validator metrics
4. The API server exposes endpoints for checking validator status and metrics

## Using the ValidatorCoordinator

### Getting the Current Validator

```elixir
# Gets the current validator and advances to the next one
validator_id = Kylix.get_current_validator()
```

### Adding a New Validator

```elixir
# Add a validator that is vouched for by an existing validator
{:ok, validator_id} = Kylix.add_validator("new_validator", public_key, "existing_validator")
```

### Checking Validator Metrics

```elixir
# Get metrics for all validators
metrics = Kylix.get_validator_metrics()

# Get detailed status of the consensus system
status = Kylix.get_validator_status()
```

## Dashboard Integration

The `ValidatorCoordinator` integrates with the Kylix dashboard to show:

- Current active validator
- List of all validators with performance metrics
- Success rates and average transaction times

## Benefits for Kylix

1. **Improved Coordination**: Centralizes validator selection logic for better coordination
2. **Performance Insights**: Provides visibility into validator performance
3. **Flexible Management**: Makes it easy to add/remove validators
4. **Enhanced Reliability**: Helps identify problematic validators
5. **Better Modularity**: Separates consensus concerns from blockchain logic

## Extending the ValidatorCoordinator

The `ValidatorCoordinator` can be extended in several ways:

1. Implement different validator selection strategies (weighted, stake-based, etc.)
2. Add more sophisticated performance metrics
3. Implement automatic validator exclusion based on poor performance
4. Add Byzantine fault tolerance mechanisms
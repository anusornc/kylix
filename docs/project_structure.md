# Kylix Project Structure

## Overview

The Kylix project is a blockchain application built with Elixir, utilizing a Directed Acyclic Graph (DAG) for transaction storage and advanced querying capabilities. The project is structured to provide modularity, scalability, and maintainability.

## Directory Layout

```
kylix/
├── config/           # Configuration files
├── lib/              # Core application logic
│   ├── auth/         # Authentication and security
│   ├── network/      # Validator network communication
│   ├── query/        # SPARQL query processing
│   ├── server/       # Blockchain transaction management
│   └── storage/      # Data storage engines
└── test/             # Comprehensive test suite
```

## Key Directories and Their Responsibilities

### `config/`
Contains environment-specific configuration files that control application settings:
- `config.exs`: Base configuration
- `dev.exs`: Development environment settings
- `prod.exs`: Production environment settings
- `runtime.exs`: Runtime configuration
- `test.exs`: Test environment configuration

### `lib/`

#### `auth/`
Handles cryptographic and authentication-related functionality:
- Signature verification
- Public key management
- Security protocol implementations

#### `network/`
Manages communication between blockchain validators:
- Peer-to-peer transaction broadcasting
- Network connection management
- Latency measurement

#### `query/`
Implements the SPARQL query processing engine:
- `sparql_parser.ex`: Parses SPARQL query strings
- `sparql_executor.ex`: Executes parsed queries
- `sparql_optimizer.ex`: Optimizes query performance
- `sparql_aggregator.ex`: Handles query aggregation functions

#### `server/`
Core blockchain transaction processing:
- Transaction validation
- Blockchain state management
- Consensus mechanism implementation

#### `storage/`
Provides flexible storage solutions:
- `dag_engine.ex`: In-memory DAG storage for testing
- `persistent_dag_engine.ex`: Disk-based persistent storage

### `test/`
Comprehensive test suite with a structure mirroring `lib/`:
- Unit tests for individual components
- Integration tests
- Security and performance tests

## Design Principles

1. **Modularity**: Each directory and module has a specific, well-defined responsibility
2. **Separation of Concerns**: Clear boundaries between different application components
3. **Flexibility**: Support for multiple environments (dev, test, production)
4. **Testability**: Extensive test coverage across all modules

## Technology Stack

- **Language**: Elixir
- **Concurrency**: GenServer, OTP principles
- **Storage**: ETS (Erlang Term Storage)
- **Cryptography**: SHA-256, RSA signature verification

## Getting Started

1. Ensure you have Elixir installed
2. Clone the repository
3. Run `mix deps.get` to install dependencies
4. Configure your environment in the `config/` directory
5. Run tests with `mix test`

## Contribution Guidelines

- Follow existing code structure
- Maintain clear module responsibilities
- Write comprehensive tests for new features
- Document any significant changes

## Performance Considerations

- Efficient DAG storage mechanism
- SPARQL query optimization
- Validator-based transaction processing
- Minimal overhead in network communication

## Security Features

- Cryptographic signature verification
- Transaction replay attack prevention
- Validator authentication
- Secure network communication

## Future Roadmap

- Enhanced query capabilities
- More advanced consensus mechanisms
- Improved network resilience
- Performance optimizations
```

The documentation provides a comprehensive overview of the Kylix project's structure, design principles, and key components. It serves as a guide for developers to understand the project's architecture and contribute effectively.
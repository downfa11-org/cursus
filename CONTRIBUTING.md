# Contributing

Go-broker is an open source project licensed under **Apache 2.0**.

Even though it is currently maintained by a single developer, contributions are welcome via GitHub pull requests. 

This document outlines development setup, conventions, and tips to help you contribute efficiently.

## Contact

If you have any questions, feel free to reach out via GitHub Issues or Discussions.

## Development Workflow

A suggested workflow for contributing:

1. Fork the repository and clone it locally.

2. Set up your environment for building and testing Go-broker.

3. Create a topic branch from main for your contribution.

4. Implement changes in logical commits.
   - Ensure tests pass and add new tests as needed.

   - Follow the commit message conventions below.

5. Push your topic branch to your fork.

6. Submit a pull request to the main repository.

7. Address review comments.

8. Once approved, your PR will be merged.

## Setting up Development Environment

### Requirements

- Go 1.25+

- Docker (for integration testing)

### Build & Test
```
make tools   # install dependencies
make build   # executable: ./bin/go-broker
```

Run integration tests locally:

```
docker compose -f manifests/docker-compose.yml up --build -d
make test
```

Check code formatting and linting:
```
make fmt
make lint
```

> Note: Keep `golangci-lint` and other tools updated by running `make tools` periodically.

## Commit Conventions

Use clear, descriptive commit messages:

```
Implement topic partitioning for single-node broker

Logical separation of data per topic is introduced.
This ensures messages are correctly routed and persisted
even in single-node setups.
```

- Subject line ≤ 70 characters

- Blank line after subject

- Body lines wrapped at 80 characters

## Testing

Testing is the responsibility of contributors:

- **Unit tests**: Verify individual functions

- **Integration tests**: Verify interactions across topics and broker components

- **Benchmark tests**: Measure performance of core broker features

Run code coverage locally:

```
make coverage
```

## Code Review

All contributions are reviewed before merging. 

Even though the project is small, pull requests should include:

- Clear description of the change

- Relevant tests

- Correct formatting
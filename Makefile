APP_NAME := go-broker
CLI_NAME := go-broker-cli
BENCH_NAME := go-broker-bench

GO := go
GOLINT := golangci-lint
TEST_FLAGS := -v -race -cover
BUILD_FLAGS := -ldflags="-s -w"

.PHONY: all
all: build

.PHONY: lint
lint: tools
	@echo "[MAKE] Running linter..."
	$(GOLINT) run ./...

.PHONY: test
test:
	@echo "[MAKE] Running unit tests..."
	$(GO) test $(TEST_FLAGS) ./...

.PHONY: bench
bench:
	@echo "[MAKE] Running benchmark..."
	go run ./cmd/bench

.PHONY: build
build: build-api build-cli build-bench

.PHONY: build-api
build-api:
	@echo "[MAKE] Building API server..."
	CGO_ENABLED=0 GOOS=linux $(GO) build $(BUILD_FLAGS) -o bin/$(APP_NAME) ./cmd/broker/main.go

.PHONY: build-cli
build-cli:
	@echo "[MAKE] Building CLI..."
	CGO_ENABLED=0 GOOS=linux $(GO) build $(BUILD_FLAGS) -o bin/$(CLI_NAME) ./cmd/cli/main.go

.PHONY: build-bench
build-bench:
	@echo "[MAKE] Building Benchmark..."
	CGO_ENABLED=0 GOOS=linux $(GO) build $(BUILD_FLAGS) -o bin/$(BENCH_NAME) ./cmd/bench/main.go

.PHONY: clean
clean:
	@echo "[MAKE] Cleaning build artifacts..."
	rm -rf bin/* pkg/topic/test-topic/* pkg/topic/topic1/* pkg/server/testconn/*

.PHONY: run
run:
	@echo "[MAKE] Running broker..."
	$(GO) run ./cmd/broker/main.go

.PHONY: cli
cli:
	@echo "[MAKE] Running CLI..."
	$(GO) run ./cmd/cli/main.go

.PHONY: docker
docker:
	@echo "[MAKE] Building docker image..."
	docker build -t $(APP_NAME):latest .

.PHONY: compose-up
compose-up:
	@echo "[MAKE] Starting docker-compose..."
	docker-compose -f manifests/docker-compose.yaml up -d

.PHONY: compose-down
compose-down:
	@echo "[MAKE] Stopping docker-compose..."
	docker-compose -f manifests/docker-compose.yaml down

.PHONY: tools
tools:
	@echo "[MAKE] Installing/updating tools..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

.PHONY: fmt
fmt:
	@echo "[MAKE] Formatting code..."
	$(GO) fmt ./...

.PHONY: coverage
coverage:
	@echo "[MAKE] Running tests with coverage..."
	$(GO) test $(TEST_FLAGS) -coverprofile=coverage.out ./...
	@echo "Coverage report saved to coverage.out"

.PHONY: help
help:
	@echo "Makefile commands:"
	@echo "  make lint          Run linter"
	@echo "  make test          Run unit tests"
	@echo "  make bench         Run benchmarks"
	@echo "  make build         Build all binaries (api, cli, bench)"
	@echo "  make clean         Remove build artifacts"
	@echo "  make run           Run broker in dev mode"
	@echo "  make cli           Run CLI in dev mode"
	@echo "  make docker        Build Docker image"
	@echo "  make compose-up    Start docker-compose stack"
	@echo "  make compose-down  Stop docker-compose stack"
	@echo "  make tools         Install or update development tools"
	@echo "  make fmt           Format code according to Go standards"
	@echo "  make coverage      Run tests with coverage report"

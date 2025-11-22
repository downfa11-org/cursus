APP_NAME := go-broker  
CLI_NAME := go-broker-cli  
BENCH_NAME := go-broker-bench  
  
GO := go  
GOLINT := golangci-lint  
TEST_FLAGS := -v -race -cover  
BUILD_FLAGS := -ldflags="-s -w"  
E2E_COMPOSE_FILE := test/docker-compose.yml  

.PHONY: all  
all: build  
  
.PHONY: lint  
lint: tools  
	@echo "[MAKE] Running linter..."  
	$(GOLINT) run ./...  
  
.PHONY: test  
test:  
	@echo "[MAKE] Running unit tests..."  
	$(GO) test $(TEST_FLAGS) $(shell go list ./... | grep -v /test/e2e)

.PHONY: e2e    
e2e: e2e-build e2e-up  
	@echo "[MAKE] Running E2E tests..."    
	$(GO) test -v -timeout 10m ./test/e2e/... || (make e2e-logs && make e2e-clean && exit 1)  
	@make e2e-clean  
  
.PHONY: e2e-up    
e2e-up:    
	@echo "[MAKE] Starting E2E test containers..."    
	docker compose -f $(E2E_COMPOSE_FILE) up -d    
	@echo "[MAKE] Waiting for broker health check..."    
	@bash -c 'for i in {1..30}; do if curl -f http://localhost:9080/health 2>/dev/null; then echo "Broker is healthy"; exit 0; fi; if [ $$i -eq 30 ]; then echo "Broker failed to start"; exit 1; fi; sleep 2; done'

.PHONY: e2e-verbose  
e2e-verbose: e2e-build  
	@echo "[MAKE] Running E2E tests (verbose)..."  
	$(GO) test -v -race -timeout 10m ./test/e2e/...   

.PHONY: e2e-build    
e2e-build:    
	@echo "[MAKE] Building E2E test images..."    
	docker compose -f $(E2E_COMPOSE_FILE) build

.PHONY: e2e-clean  
e2e-clean:  
	@echo "[MAKE] Cleaning E2E test environment..."  
	docker compose -f $(E2E_COMPOSE_FILE) down -v  # docker-compose → docker compose  
	rm -rf test/logs/*   
  
.PHONY: e2e-logs  
e2e-logs:  
	@echo "[MAKE] Showing E2E test logs..."  
	@echo "=== Broker logs ==="  
	docker logs broker 2>&1 || echo "Broker container not found"  
	@echo "\n=== Publisher logs ==="  
	docker logs broker-publisher 2>&1 || echo "Publisher container not found"  
	@echo "\n=== Consumer logs ==="  
	docker logs broker-consumer 2>&1 || echo "Consumer container not found"  
  
.PHONY: e2e-coverage  
e2e-coverage: e2e-build  
	@echo "[MAKE] Running E2E tests with coverage..."  
	$(GO) test -v -timeout 10m -coverprofile=e2e-coverage.out ./test/e2e/...  
	@echo "E2E coverage report saved to e2e-coverage.out"  
  
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
	rm -f coverage.out e2e-coverage.out  
  
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
	docker compose -f manifests/docker-compose.yml up -d
  
.PHONY: compose-down  
compose-down:  
	@echo "[MAKE] Stopping docker-compose..."  
	docker compose -f manifests/docker-compose.yml down  

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
	$(GO) test $(TEST_FLAGS) -coverprofile=coverage.out $(shell go list ./... | grep -v /test/e2e)    
	@echo "Coverage report saved to coverage.out"  
  
.PHONY: help  
help:  
	@echo "Makefile commands:"  
	@echo "  make lint            Run linter"  
	@echo "  make test            Run unit tests"  
	@echo "  make e2e             Run E2E tests (builds images first)"  
	@echo "  make e2e-verbose     Run E2E tests with race detection" 
	@echo "  make e2e-logs        Show E2E test container logs" 
	@echo "  make bench           Run benchmarks"  
	@echo "  make build           Build all binaries (api, cli, bench)"  
	@echo "  make clean           Remove build artifacts"  
	@echo "  make run             Run broker in dev mode" 
	@echo "  make tools           Install or update development tools"  
	@echo "  make fmt             Format code according to Go standards"  
	@echo "  make coverage        Run tests with coverage report"
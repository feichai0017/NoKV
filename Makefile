# NoKV Makefile
# Provides standardized commands for development workflow

.PHONY: help build test test-short test-race test-coverage lint fmt clean docker-up docker-down bench install-tools

GOLANGCI_LINT_VERSION ?= v2.9.0
PROJECT_GO_VERSION ?= $(shell awk '/^go /{print $$2}' go.mod)
PROTOC_GEN_GO_VERSION ?= $(shell go list -m -f '{{.Version}}' google.golang.org/protobuf)
PROTOC_GEN_GO_GRPC_VERSION ?= v1.6.1
PROTOC_VERSION ?= 33.4

# Default target
help:
	@echo "NoKV Development Commands:"
	@echo ""
	@echo "  make build              - Build all binaries"
	@echo "  make test               - Run all tests"
	@echo "  make test-short         - Run tests in short mode"
	@echo "  make test-race          - Run tests with race detector"
	@echo "  make test-coverage      - Run tests with coverage report"
	@echo "  make lint               - Run golangci-lint (requires installation)"
	@echo "  make fmt                - Format code with gofmt and tidy modules"
	@echo "  make bench              - Run benchmarks"
	@echo "  make install-tools      - Install development tools"
	@echo "  make docker-up          - Start Docker Compose cluster"
	@echo "  make docker-down        - Stop Docker Compose cluster"
	@echo "  make clean              - Remove build artifacts and test data"
	@echo ""

# Build all binaries
build:
	@echo "Building NoKV binaries..."
	go build -v ./...
	go build -o build/nokv ./cmd/nokv
	go build -o build/nokv-redis ./cmd/nokv-redis
	go build -o build/nokv-config ./cmd/nokv-config
	go build -o build/nokv-pd ./cmd/nokv-pd
	go build -o build/nokv-tso ./scripts/tso
	@echo "✓ Build complete: binaries in build/"

# Run all tests
test:
	@echo "Running all tests..."
	go test -v ./...

# Run tests in short mode (faster, skips some long-running tests)
test-short:
	@echo "Running tests in short mode..."
	go test -short -v ./...

# Run tests with race detector
test-race:
	@echo "Running tests with race detector..."
	go test -race -v ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	go test -v -coverprofile=coverage.out -covermode=atomic ./...
	@echo "✓ Coverage report generated: coverage.out"
	@echo "  View with: go tool cover -html=coverage.out"

# Run linter (requires golangci-lint to be installed)
lint:
	@echo "Running golangci-lint..."
	@which golangci-lint > /dev/null || (echo "golangci-lint not found. Run 'make install-tools' first." && exit 1)
	golangci-lint run ./...

# Format code and tidy dependencies
fmt:
	@echo "Formatting code..."
	gofmt -w -s .
	go mod tidy
	@echo "✓ Code formatted"

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	./scripts/run_benchmarks.sh

# Install development tools
install-tools:
	@echo "Installing development tools (pinned versions)..."
	GOTOOLCHAIN=go$(PROJECT_GO_VERSION).0 go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	go install google.golang.org/protobuf/cmd/protoc-gen-go@$(PROTOC_GEN_GO_VERSION)
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@$(PROTOC_GEN_GO_GRPC_VERSION)
	@if ! command -v protoc >/dev/null 2>&1; then \
		echo "WARN: protoc not found. Please install libprotoc $(PROTOC_VERSION)."; \
	else \
		installed="$$(protoc --version | awk '{print $$2}')"; \
		if [ "$$installed" != "$(PROTOC_VERSION)" ]; then \
			echo "WARN: expected libprotoc $(PROTOC_VERSION), got $$installed"; \
		fi; \
	fi
	@echo "✓ Tools installed"

# Start Docker Compose cluster
docker-up:
	@echo "Starting Docker Compose cluster..."
	docker compose up --build

# Stop Docker Compose cluster
docker-down:
	@echo "Stopping Docker Compose cluster..."
	docker compose down -v

# Clean build artifacts and test data
clean:
	@echo "Cleaning build artifacts and test data..."
	rm -rf ./work_test
	rm -rf ./artifacts
	rm -rf ./build
	rm -rf ./testdata
	rm -f coverage.out
	rm -f *.pprof
	rm -f benchmark.test
	@echo "✓ Clean complete"

# Development helpers
.PHONY: local-cluster local-cluster-stop

# Start local cluster (without Docker)
local-cluster:
	@echo "Starting local cluster..."
	./scripts/run_local_cluster.sh --config ./raft_config.example.json

# Stop local cluster
local-cluster-stop:
	@echo "Stopping local cluster..."
	pkill -f "nokv.*store-" || true
	@echo "✓ Local cluster stopped"

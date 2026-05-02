# NoKV Makefile
# Provides standardized commands for development workflow

.PHONY: help build test test-short test-race test-coverage test-contract-smoke test-raftstore-contract-smoke test-correctness-smoke lint fmt clean docker-up docker-dev-up docker-down bench install-tools
.PHONY: proto proto-check proto-breaking-check

GOLANGCI_LINT_VERSION ?= v2.9.0
BUF_VERSION ?= 1.66.0
PROJECT_GO_VERSION ?= 1.26.2
GO_TEST_P ?= 1

# Default target
help:
	@echo "NoKV Development Commands:"
	@echo ""
	@echo "  make build              - Build all binaries"
	@echo "  make test               - Run all tests"
	@echo "  make test-short         - Run tests in short mode"
	@echo "  make test-race          - Run tests with race detector"
	@echo "  make test-coverage      - Run tests with coverage report"
	@echo "  make test-contract-smoke - Run seeded fsmeta contract model smoke tests"
	@echo "  make test-raftstore-contract-smoke - Run seeded fsmeta contract tests on real raftstore"
	@echo "  make test-correctness-smoke - Run distributed correctness smoke tests"
	@echo "  make lint               - Run golangci-lint (requires installation)"
	@echo "  make fmt                - Run go fix, format code with gofmt, and tidy modules"
	@echo "  make proto              - Format .proto files and regenerate protobuf Go code"
	@echo "  make proto-check        - Verify proto format, lint, and generated code"
	@echo "  make proto-breaking-check - Run Buf breaking checks against main"
	@echo "  make bench              - Run benchmarks"
	@echo "  make install-tools      - Install development tools"
	@echo "  make docker-up          - Start Docker Compose cluster"
	@echo "  make docker-dev-up      - Build local image and start Docker Compose cluster"
	@echo "  make docker-down        - Stop Docker Compose cluster"
	@echo "  make clean              - Remove build artifacts and test data"
	@echo ""

# Build all binaries
build:
	@echo "Building NoKV binaries..."
	go build -v ./...
	go build -o build/nokv ./cmd/nokv
	go build -o build/nokv-config ./cmd/nokv-config
	go build -o build/nokv-fsmeta ./cmd/nokv-fsmeta
	@echo "✓ Build complete: binaries in build/"

# Run all tests
test:
	@echo "Running all tests..."
	go test -p $(GO_TEST_P) -v ./...

# Run tests in short mode (faster, skips some long-running tests)
test-short:
	@echo "Running tests in short mode..."
	go test -p $(GO_TEST_P) -short -v ./...

# Run tests with race detector
test-race:
	@echo "Running tests with race detector..."
	go test -p $(GO_TEST_P) -race -v ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	go test -p $(GO_TEST_P) -v -coverprofile=coverage.out -covermode=atomic $$(go list ./... | grep -v '/integration$$')
	@echo "✓ Coverage report generated: coverage.out"
	@echo "  View with: go tool cover -html=coverage.out"

# Run seeded contract tests against the fsmeta executor model.
test-contract-smoke:
	@echo "Running fsmeta contract smoke tests..."
	NOKV_CONTRACT_SEEDS=$${NOKV_CONTRACT_SEEDS:-64} NOKV_CONTRACT_STEPS=$${NOKV_CONTRACT_STEPS:-120} go test ./fsmeta/contract -run TestFSMetaExecutorModelContract -count=1 -v

# Run seeded contract scripts against the real raftstore-backed fsmeta runner.
test-raftstore-contract-smoke:
	@echo "Running raftstore-backed fsmeta contract smoke tests..."
	NOKV_RAFTSTORE_CONTRACT_SEEDS=$${NOKV_RAFTSTORE_CONTRACT_SEEDS:-2} NOKV_RAFTSTORE_CONTRACT_STEPS=$${NOKV_RAFTSTORE_CONTRACT_STEPS:-40} go test ./fsmeta/integration -run TestRaftstoreRunnerFSMetaContractOnSplitCluster -count=1 -v

# Run the highest-signal distributed correctness suites before the full package sweep.
test-correctness-smoke: test-contract-smoke test-raftstore-contract-smoke
	@echo "Running distributed correctness smoke tests..."
	go test -p $(GO_TEST_P) ./percolator/... ./raftstore/client ./raftstore/mvcc ./raftstore/store ./raftstore/integration ./coordinator/integration ./meta/root/integration -count=1

# Run linter (requires golangci-lint to be installed)
lint:
	@echo "Running golangci-lint..."
	@which golangci-lint > /dev/null || (echo "golangci-lint not found. Run 'make install-tools' first." && exit 1)
	golangci-lint run ./...

# Format code and tidy dependencies
fmt:
	@echo "Formatting code..."
	go fix ./...
	@files=$$(git ls-files '*.go'); \
	for f in $$files; do \
		[ -f "$$f" ] && printf '%s\n' "$$f"; \
	done | xargs -r gofmt -w -s
	buf format -w
	go mod tidy
	@echo "✓ Code formatted"

proto:
	@echo "Formatting .proto files and generating protobuf Go code..."
	buf format -w
	./scripts/gen.sh
	@echo "✓ Protobufs formatted and generated"

proto-check:
	@echo "Checking proto format, lint, and generated code..."
	buf format -d --exit-code
	buf lint
	@set -e; \
	before="$$(find pb -type f \( -name '*.pb.go' -o -name '*_grpc.pb.go' \) | sort | xargs sha256sum)"; \
	./scripts/gen.sh; \
	after="$$(find pb -type f \( -name '*.pb.go' -o -name '*_grpc.pb.go' \) | sort | xargs sha256sum)"; \
	test "$$before" = "$$after"
	@echo "✓ Proto checks passed"

proto-breaking-check:
	@echo "Checking proto breaking changes against main..."
	@set -e; \
	base_ref="refs/remotes/origin/main"; \
	if ! git show-ref --verify --quiet "$$base_ref"; then \
		base_ref="refs/heads/main"; \
	fi; \
	buf breaking --against ".git#ref=$$base_ref,subdir=pb"
	@echo "✓ Proto breaking checks passed"

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	./scripts/run_benchmarks.sh

# Install development tools
install-tools:
	@echo "Installing development tools (pinned versions)..."
	GOTOOLCHAIN=go$(PROJECT_GO_VERSION) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	go install github.com/bufbuild/buf/cmd/buf@v$(BUF_VERSION)
	@echo "✓ Tools installed"

# Start Docker Compose cluster
docker-up:
	@echo "Starting Docker Compose cluster..."
	@if docker compose pull --policy always --ignore-buildable; then \
		docker compose up -d; \
	else \
		echo "Published image unavailable; falling back to local build..."; \
		docker compose up -d --build; \
	fi

# Build local image and start Docker Compose cluster
docker-dev-up:
	@echo "Building local image and starting Docker Compose cluster..."
	docker compose up -d --build

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
	./scripts/dev/cluster.sh --config ./raft_config.example.json

# Stop local cluster
local-cluster-stop:
	@echo "Stopping local cluster..."
	pkill -f "nokv.*store-" || true
	@echo "✓ Local cluster stopped"

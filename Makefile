# NoKV Makefile
# Provides standardized commands for development workflow

.PHONY: help build test test-short test-race test-coverage lint fmt clean docker-up docker-down bench install-tools install-tla-tools
.PHONY: proto proto-check proto-breaking-check
.PHONY: tlc-succession tlc-successionmultidim tlc-mountlifecycle tlc-subtreeauthority tlc-leaseonly-counterexample tlc-leasestart-counterexample apalache-typecheck apalache-check-succession apalache-check-successionmultidim

GOLANGCI_LINT_VERSION ?= v2.9.0
BUF_VERSION ?= 1.66.0
PROJECT_GO_VERSION ?= 1.26.2

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
	@echo "  make fmt                - Run go fix, format code with gofmt, and tidy modules"
	@echo "  make proto              - Format .proto files and regenerate protobuf Go code"
	@echo "  make proto-check        - Verify proto format, lint, and generated code"
	@echo "  make proto-breaking-check - Run Buf breaking checks against main"
	@echo "  make bench              - Run benchmarks"
	@echo "  make install-tools      - Install development tools"
	@echo "  make install-tla-tools  - Install pinned TLC and Apalache locally under third_party/"
	@echo "  make tlc-succession            - Run TLC on spec/Succession.tla"
	@echo "  make tlc-successionmultidim    - Run TLC on spec/SuccessionMultiDim.tla"
	@echo "  make tlc-mountlifecycle        - Run TLC on spec/MountLifecycle.tla"
	@echo "  make tlc-subtreeauthority      - Run TLC on spec/SubtreeAuthority.tla"
	@echo "  make tlc-leaseonly-counterexample - Run TLC and expect a counterexample for spec/LeaseOnly.tla"
	@echo "  make tlc-leasestart-counterexample - Run TLC and expect a counterexample for spec/LeaseStartOnly.tla"
	@echo "  make apalache-typecheck - Run Apalache typecheck on current specs"
	@echo "  make apalache-check-succession - Run bounded Apalache check on Succession invariants"
	@echo "  make apalache-check-successionmultidim - Run bounded Apalache check on SuccessionMultiDim invariants"
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
	go build -o build/nokv-fsmeta ./cmd/nokv-fsmeta
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

install-tla-tools:
	@echo "Installing pinned TLA+ tools locally..."
	./scripts/tla/setup.sh

tlc-succession:
	@echo "Running TLC on spec/Succession.tla..."
	./scripts/tla/tlc.sh spec/Succession.tla

tlc-successionmultidim:
	@echo "Running TLC on spec/SuccessionMultiDim.tla..."
	./scripts/tla/tlc.sh spec/SuccessionMultiDim.tla

tlc-mountlifecycle:
	@echo "Running TLC on spec/MountLifecycle.tla..."
	./scripts/tla/tlc.sh spec/MountLifecycle.tla

tlc-subtreeauthority:
	@echo "Running TLC on spec/SubtreeAuthority.tla..."
	./scripts/tla/tlc.sh spec/SubtreeAuthority.tla

tlc-leaseonly-counterexample:
	@echo "Running TLC on spec/LeaseOnly.tla (expecting counterexample)..."
	@if ./scripts/tla/tlc.sh spec/LeaseOnly.tla; then \
		echo "expected TLC to find a counterexample for LeaseOnly, but it succeeded"; \
		exit 1; \
	else \
		echo "✓ TLC found the expected counterexample for LeaseOnly"; \
	fi

tlc-leasestart-counterexample:
	@echo "Running TLC on spec/LeaseStartOnly.tla (expecting counterexample)..."
	@if ./scripts/tla/tlc.sh spec/LeaseStartOnly.tla; then \
		echo "expected TLC to find a counterexample for LeaseStartOnly, but it succeeded"; \
		exit 1; \
	else \
		echo "✓ TLC found the expected counterexample for LeaseStartOnly"; \
	fi

tlc-chubbyfenced-counterexample:
	@echo "Running TLC on spec/ChubbyFencedLease.tla (expecting coverage counterexample)..."
	@if ./scripts/tla/tlc.sh spec/ChubbyFencedLease.tla; then \
		echo "expected TLC to find a counterexample for ChubbyFencedLease, but it succeeded"; \
		exit 1; \
	else \
		echo "✓ TLC found the expected counterexample for ChubbyFencedLease"; \
	fi

tlc-tokenonly-counterexample:
	@echo "Running TLC on spec/TokenOnly.tla (expecting stale-delivery counterexample)..."
	@if ./scripts/tla/tlc.sh spec/TokenOnly.tla; then \
		echo "expected TLC to find a counterexample for TokenOnly, but it succeeded"; \
		exit 1; \
	else \
		echo "✓ TLC found the expected counterexample for TokenOnly"; \
	fi

tlc-contrast-models: tlc-leaseonly-counterexample tlc-tokenonly-counterexample tlc-chubbyfenced-counterexample tlc-leasestart-counterexample

record-tlc-succession:
	@echo "Recording TLC output for Succession..."
	@if ./scripts/tla/record_tlc.sh spec/Succession.tla spec/artifacts/tlc-succession.out; then \
		echo "✓ Recorded TLC output for Succession"; \
	else \
		echo "expected Succession to succeed under TLC, but recording failed"; \
		exit 1; \
	fi

record-tlc-successionmultidim:
	@echo "Recording TLC output for SuccessionMultiDim..."
	@if ./scripts/tla/record_tlc.sh spec/SuccessionMultiDim.tla spec/artifacts/tlc-successionmultidim.out; then \
		echo "✓ Recorded TLC output for SuccessionMultiDim"; \
	else \
		echo "expected SuccessionMultiDim to succeed under TLC, but recording failed"; \
		exit 1; \
	fi

record-tlc-mountlifecycle:
	@echo "Recording TLC output for MountLifecycle..."
	@if ./scripts/tla/record_tlc.sh spec/MountLifecycle.tla spec/artifacts/tlc-mountlifecycle.out; then \
		echo "✓ Recorded TLC output for MountLifecycle"; \
	else \
		echo "expected MountLifecycle to succeed under TLC, but recording failed"; \
		exit 1; \
	fi

record-tlc-subtreeauthority:
	@echo "Recording TLC output for SubtreeAuthority..."
	@if ./scripts/tla/record_tlc.sh spec/SubtreeAuthority.tla spec/artifacts/tlc-subtreeauthority.out; then \
		echo "✓ Recorded TLC output for SubtreeAuthority"; \
	else \
		echo "expected SubtreeAuthority to succeed under TLC, but recording failed"; \
		exit 1; \
	fi

record-tlc-leaseonly:
	@echo "Recording TLC counterexample for LeaseOnly..."
	@if ./scripts/tla/record_tlc.sh spec/LeaseOnly.tla spec/artifacts/tlc-leaseonly.out; then \
		echo "expected LeaseOnly recording to fail with counterexample, but it succeeded"; \
		exit 1; \
	else \
		echo "✓ Recorded TLC counterexample for LeaseOnly"; \
	fi

record-tlc-tokenonly:
	@echo "Recording TLC counterexample for TokenOnly..."
	@if ./scripts/tla/record_tlc.sh spec/TokenOnly.tla spec/artifacts/tlc-tokenonly.out; then \
		echo "expected TokenOnly recording to fail with counterexample, but it succeeded"; \
		exit 1; \
	else \
		echo "✓ Recorded TLC counterexample for TokenOnly"; \
	fi

record-tlc-chubbyfenced:
	@echo "Recording TLC counterexample for ChubbyFencedLease..."
	@if ./scripts/tla/record_tlc.sh spec/ChubbyFencedLease.tla spec/artifacts/tlc-chubbyfenced.out; then \
		echo "expected ChubbyFencedLease recording to fail with counterexample, but it succeeded"; \
		exit 1; \
	else \
		echo "✓ Recorded TLC counterexample for ChubbyFencedLease"; \
	fi

record-tlc-leasestart:
	@echo "Recording TLC counterexample for LeaseStartOnly..."
	@if ./scripts/tla/record_tlc.sh spec/LeaseStartOnly.tla spec/artifacts/tlc-leasestart.out; then \
		echo "expected LeaseStartOnly recording to fail with counterexample, but it succeeded"; \
		exit 1; \
	else \
		echo "✓ Recorded TLC counterexample for LeaseStartOnly"; \
	fi

record-apalache-succession:
	@echo "Recording Apalache bounded-check output for Succession..."
	./scripts/tla/record_apalache_check.sh spec/Succession.tla spec/Succession.cfg G1_Succession,G2_Primacy,G2_PrimacyInductive,G3_Silence,G4_Finality 8 spec/artifacts/apalache-succession.out
	@echo "✓ Recorded Apalache output for Succession"

record-apalache-successionmultidim:
	@echo "Recording Apalache bounded-check output for SuccessionMultiDim..."
	./scripts/tla/record_apalache_check.sh spec/SuccessionMultiDim.tla spec/SuccessionMultiDim.cfg NoWriteBehindServedRead 6 spec/artifacts/apalache-successionmultidim.out
	@echo "✓ Recorded Apalache output for SuccessionMultiDim"

record-formal-artifacts: record-tlc-succession record-tlc-successionmultidim record-tlc-mountlifecycle record-tlc-subtreeauthority record-tlc-leaseonly record-tlc-tokenonly record-tlc-chubbyfenced record-tlc-leasestart record-apalache-succession record-apalache-successionmultidim

apalache-typecheck:
	@echo "Running Apalache typecheck on current specs..."
	./scripts/tla/apalache.sh typecheck --features=no-rows spec/Succession.tla
	./scripts/tla/apalache.sh typecheck --features=no-rows spec/SuccessionMultiDim.tla
	./scripts/tla/apalache.sh typecheck --features=no-rows spec/LeaseOnly.tla
	./scripts/tla/apalache.sh typecheck --features=no-rows spec/LeaseStartOnly.tla
	./scripts/tla/apalache.sh typecheck --features=no-rows spec/ChubbyFencedLease.tla
	./scripts/tla/apalache.sh typecheck --features=no-rows spec/TokenOnly.tla

apalache-check-succession:
	@echo "Running bounded Apalache check on Succession..."
	./scripts/tla/apalache.sh --features=no-rows check --config=spec/Succession.cfg --no-deadlock --length=8 --inv=G1_Succession,G2_Primacy,G2_PrimacyInductive,G3_Silence,G4_Finality spec/Succession.tla

apalache-check-successionmultidim:
	@echo "Running bounded Apalache check on SuccessionMultiDim..."
	./scripts/tla/apalache.sh --features=no-rows check --config=spec/SuccessionMultiDim.cfg --no-deadlock --length=6 --inv=NoWriteBehindServedRead spec/SuccessionMultiDim.tla

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
	./scripts/dev/cluster.sh --config ./raft_config.example.json

# Stop local cluster
local-cluster-stop:
	@echo "Stopping local cluster..."
	pkill -f "nokv.*store-" || true
	@echo "✓ Local cluster stopped"

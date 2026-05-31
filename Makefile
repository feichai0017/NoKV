# Copyright 2024-2026 The NoKV Authors.
# SPDX-License-Identifier: Apache-2.0

GOLANGCI_LINT_VERSION ?= v2.9.0
BUF_VERSION ?= 1.66.0
PROJECT_GO_VERSION ?= 1.26.2
GO_TEST_P ?= 1
RAFTSTORE_MANIFEST := raftstore/Cargo.toml

.PHONY: help build test test-go test-rust test-short test-race test-coverage verify
.PHONY: lint lint-nokv test-lint fmt clean
.PHONY: proto proto-check proto-breaking-check install-tools
.PHONY: install-tla-tools test-tla-smoke test-tla-nightly
.PHONY: tlc-eunomia tlc-eunomiamultidim tlc-mountlifecycle tlc-subtreeauthority tlc-root-replay-watch tlc-fsmeta-namespace
.PHONY: tlc-leaseonly-counterexample tlc-leasestart-counterexample tlc-tokenonly-counterexample tlc-chubbyfenced-counterexample tlc-subtreewithoutfrontiercoverage-counterexample tlc-subtreewithoutseal-counterexample tlc-contrast-models
.PHONY: docker-up docker-dev-up docker-down fsmeta-bench

help:
	@echo "NoKV Development Commands:"
	@echo ""
	@echo "  make build              - Build Go fsmeta tools and Rust raftstore"
	@echo "  make test               - Run Go and Rust tests"
	@echo "  make test-go            - Run Go tests"
	@echo "  make test-rust          - Run raftstore tests"
	@echo "  make test-short         - Run Go tests in short mode"
	@echo "  make test-race          - Run Go tests with race detector"
	@echo "  make test-coverage      - Run Go tests with coverage report"
	@echo "  make lint               - Run golangci-lint with NoKV analyzers"
	@echo "  make test-lint          - Run analyzer unit tests under tools/lint"
	@echo "  make verify             - Run build + lint + test-lint + test"
	@echo "  make proto              - Format .proto files and regenerate Go protobuf code"
	@echo "  make proto-check        - Verify proto format, lint, and generated code"
	@echo "  make fsmeta-bench       - Run the local fsmeta benchmark matrix"
	@echo "  make docker-up          - Start the local fsmeta demo container"
	@echo "  make docker-dev-up      - Build and start the local fsmeta demo container"
	@echo "  make docker-down        - Stop Docker Compose and remove volumes"
	@echo "  make clean              - Remove build artifacts and benchmark scratch data"

build:
	@echo "Building NoKV binaries..."
	mkdir -p build
	go build -v ./...
	go build -o build/nokv ./cmd/nokv
	go build -o build/nokv-fsmeta ./cmd/nokv-fsmeta
	go build -o build/nokv-fsmeta-history ./cmd/nokv-fsmeta-history
	go build -o build/nokv-fsmeta-scrub ./cmd/nokv-fsmeta-scrub
	go build -o build/nokv-fsmeta-soak ./cmd/nokv-fsmeta-soak
	cargo build --manifest-path $(RAFTSTORE_MANIFEST) --workspace
	@echo "✓ Build complete"

test: test-go test-rust

test-go:
	@echo "Running Go tests..."
	go test -p $(GO_TEST_P) -v ./...

test-rust:
	@echo "Running Rust tests..."
	cargo test --manifest-path $(RAFTSTORE_MANIFEST) --workspace

test-short:
	@echo "Running Go tests in short mode..."
	go test -p $(GO_TEST_P) -short -v ./...

test-race:
	@echo "Running Go tests with race detector..."
	go test -p $(GO_TEST_P) -race -v ./...

test-coverage:
	@echo "Running Go tests with coverage..."
	go test -p $(GO_TEST_P) -v -coverprofile=coverage.out -covermode=atomic ./...
	@echo "✓ Coverage report generated: coverage.out"

verify: build lint test-lint test
	@echo ""
	@echo "✓ verify: build + lint + test-lint + test all passed"

NOKV_LINT_BIN := ./bin/custom-gcl

lint: $(NOKV_LINT_BIN)
	@echo "Running custom-gcl (golangci-lint + nokvcontract)..."
	$(NOKV_LINT_BIN) run ./...

$(NOKV_LINT_BIN): .custom-gcl.yml tools/lint/go.mod $(shell find tools/lint -name '*.go' -not -path '*/testdata/*' 2>/dev/null)
	@which golangci-lint > /dev/null || (echo "golangci-lint not found. Run 'make install-tools' first." && exit 1)
	golangci-lint custom

lint-nokv:
	go run github.com/feichai0017/NoKV/tools/lint/cmd/nokvlint ./...

test-lint:
	@echo "Running analyzer unit tests..."
	cd tools/lint && go test ./...

fmt:
	@echo "Formatting code..."
	go fix ./...
	@files=$$(git ls-files '*.go'); \
	for f in $$files; do \
		[ -f "$$f" ] && printf '%s\n' "$$f"; \
	done | xargs -r gofmt -w -s
	buf format -w
	go mod tidy
	cargo fmt --manifest-path $(RAFTSTORE_MANIFEST) --all
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

install-tools:
	@echo "Installing development tools (pinned versions)..."
	GOTOOLCHAIN=go$(PROJECT_GO_VERSION) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	go install github.com/bufbuild/buf/cmd/buf@v$(BUF_VERSION)
	@echo "Building embedded custom-gcl with NoKV code-contract analyzers..."
	golangci-lint custom
	@echo "✓ Tools installed"

install-tla-tools:
	@echo "Installing pinned TLA+ tools locally..."
	./scripts/tla/setup.sh

test-tla-smoke: tlc-eunomia tlc-mountlifecycle tlc-subtreeauthority tlc-root-replay-watch tlc-fsmeta-namespace

test-tla-nightly: test-tla-smoke tlc-eunomiamultidim tlc-contrast-models

define TLC_SPEC_TARGET
$(1):
	@echo "Running TLC on spec/$(2).tla..."
	./scripts/tla/tlc.sh spec/$(2).tla
endef

define TLC_COUNTEREXAMPLE_TARGET
$(1):
	@echo "Running TLC on spec/$(2).tla ($(3))..."
	@./scripts/tla/expect_counterexample.sh spec/$(2).tla
	@echo "✓ TLC found the expected counterexample for $(2)"
endef

$(eval $(call TLC_SPEC_TARGET,tlc-eunomia,Eunomia))
$(eval $(call TLC_SPEC_TARGET,tlc-eunomiamultidim,EunomiaMultiDim))
$(eval $(call TLC_SPEC_TARGET,tlc-mountlifecycle,MountLifecycle))
$(eval $(call TLC_SPEC_TARGET,tlc-subtreeauthority,SubtreeAuthority))
$(eval $(call TLC_SPEC_TARGET,tlc-root-replay-watch,RootReplayWatch))
$(eval $(call TLC_SPEC_TARGET,tlc-fsmeta-namespace,FSMetaNamespace))

$(eval $(call TLC_COUNTEREXAMPLE_TARGET,tlc-leaseonly-counterexample,LeaseOnly,expecting stale reply counterexample))
$(eval $(call TLC_COUNTEREXAMPLE_TARGET,tlc-leasestart-counterexample,LeaseStartOnly,expecting lease-start coverage counterexample))
$(eval $(call TLC_COUNTEREXAMPLE_TARGET,tlc-chubbyfenced-counterexample,ChubbyFencedLease,expecting coverage counterexample))
$(eval $(call TLC_COUNTEREXAMPLE_TARGET,tlc-tokenonly-counterexample,TokenOnly,expecting stale-delivery counterexample))
$(eval $(call TLC_COUNTEREXAMPLE_TARGET,tlc-subtreewithoutfrontiercoverage-counterexample,SubtreeWithoutFrontierCoverage,expecting inheritance counterexample))
$(eval $(call TLC_COUNTEREXAMPLE_TARGET,tlc-subtreewithoutseal-counterexample,SubtreeWithoutSeal,expecting primacy counterexample))

tlc-contrast-models: tlc-leaseonly-counterexample tlc-tokenonly-counterexample tlc-chubbyfenced-counterexample tlc-leasestart-counterexample tlc-subtreewithoutfrontiercoverage-counterexample tlc-subtreewithoutseal-counterexample

docker-up:
	@echo "Starting local fsmeta Docker demo..."
	@if docker compose pull --policy always --ignore-buildable; then \
		docker compose up -d; \
	else \
		echo "Published image unavailable; falling back to local build..."; \
		docker compose up -d --build; \
	fi

docker-dev-up:
	@echo "Building and starting local fsmeta Docker demo..."
	docker compose up -d --build

fsmeta-bench:
	@echo "Running local fsmeta benchmark matrix..."
	NOKV_FSMETA_BENCH_MODE=$${NOKV_FSMETA_BENCH_MODE:-local} ./scripts/run_fsmeta_benchmarks.sh

docker-down:
	@echo "Stopping Docker Compose..."
	docker compose down -v

clean:
	@echo "Cleaning build artifacts and test data..."
	rm -rf ./work_test ./artifacts ./build ./testdata
	rm -f coverage.out *.pprof benchmark.test
	rm -rf benchmark/data/fsmeta/profiles benchmark/data/fsmeta/ci
	@echo "✓ Clean complete"

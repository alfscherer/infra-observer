# infra-observer build and operations entry points.
# Everything here is plain make + go + sh: nothing depends on hosted CI.

SHELL := /bin/bash
GO      ?= go
BIN     := bin/infra-observer
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: help setup build test lint fmt check clean integration-test migrate

help: ## list targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

setup: ## download dependencies and verify the toolchain
	$(GO) version
	$(GO) mod download

build: ## build the single infra-observer binary into bin/
	CGO_ENABLED=0 $(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN) ./cmd/infra-observer

test: ## run unit tests
	$(GO) test -race -count=1 ./...

fmt: ## format all Go sources
	gofmt -w cmd internal

lint: ## gofmt + go vet (+ staticcheck when installed)
	@out="$$(gofmt -l cmd internal)"; if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi
	$(GO) vet ./...
	@if command -v staticcheck >/dev/null; then staticcheck ./...; else echo "staticcheck not installed; skipped"; fi

integration-test: ## run all tests including PostgreSQL-backed ones (needs docker)
	./hack/integration-test.sh

migrate: build ## apply database migrations (needs INFRA_OBSERVER_DATABASE_URL)
	$(BIN) migrate --config configs/config.yaml

check: lint test ## everything a change must pass before commit

clean:
	rm -rf bin dist

.PHONY: validate-config
validate-config: build ## validate configuration and inventory
	$(BIN) config validate --config configs/config.yaml

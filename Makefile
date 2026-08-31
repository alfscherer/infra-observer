# infra-observer build and operations entry points.
# Everything here is plain make + go + sh: nothing depends on hosted CI.

SHELL := /bin/bash
GO      ?= go
BIN     := bin/infra-observer
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.PHONY: help setup build test lint fmt check clean integration-test migrate scenario test-scripts validate-scripts run stop reset logs ps smoke

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

test-scripts: build ## run every JavaScript extension against its fixtures
	$(BIN) script test-all --config configs/config.yaml

validate-scripts: build ## load all scripts as a service would; fail if any is broken
	$(BIN) script validate --config configs/config.yaml

check: lint test test-scripts ## everything a change must pass before commit

clean:
	rm -rf bin dist

.PHONY: validate-config
validate-config: build ## validate configuration and inventory
	$(BIN) config validate --config configs/config.yaml

COMPOSE ?= docker compose

run: ## build images and start the whole stack (NATS, PostgreSQL, platform, simulated lab)
	$(COMPOSE) up -d --build
	@echo "api:       http://localhost:8080/api/devices"
	@echo "webhook:   http://localhost:8090/v1/received"
	@echo "metrics:   http://localhost:9091/metrics (processor)"

stop: ## stop the stack, keeping data
	$(COMPOSE) down

reset: ## stop the stack and delete all data (volumes)
	$(COMPOSE) down -v --remove-orphans

logs: ## follow logs of every service (SERVICE=processor to pick one)
	$(COMPOSE) logs -f --tail=100 $(SERVICE)

ps: ## service status
	$(COMPOSE) ps

# make scenario DEVICE=switch-01 EVENT=interface-flap [ARGS="--arg interface=Gi0/2 --duration 5m"]
scenario: build ## apply a simulator scenario: DEVICE=... EVENT=... [ARGS=...]
	INFRA_OBSERVER_NATS_URL=$${INFRA_OBSERVER_NATS_URL:-nats://localhost:4222} $(BIN) scenario --config configs/config.yaml --device "$(DEVICE)" --event "$(EVENT)" $(ARGS)

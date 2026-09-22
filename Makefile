# OmniFlow — the documented commands, in one place. `make help` lists them.
#
# Every target here is what CI runs, spelled the same way, so a local pass means something.
# Both Go modules are covered by each Go target; viz-gateway is a separate module and is easy to
# forget, which is the whole reason these targets exist.

SHELL := bash
.DEFAULT_GOAL := help

GOLANGCI_LINT_VERSION ?= v2.13.2
GOVULNCHECK_VERSION   ?= v1.6.0
GOBIN                 := $(shell go env GOPATH)/bin
# Static binaries for build; tests are NOT forced to CGO_ENABLED=0 because -race needs cgo.

MODULES := . services/viz-gateway

.PHONY: help build vet test test-integration fmt fmt-check lint lint-go lint-sh vuln check \
        frontend-install frontend-lint frontend-typecheck frontend-build frontend-test frontend \
        up up-observability down logs e2e failtests tools clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ---- Go ------------------------------------------------------------------------------------------

build: ## Build both Go modules
	@for m in $(MODULES); do echo "== build $$m"; (cd $$m && CGO_ENABLED=0 go build ./...) || exit 1; done

vet: ## go vet both modules
	@for m in $(MODULES); do echo "== vet $$m"; (cd $$m && go vet ./...) || exit 1; done

test: ## Unit tests with -race, both modules (no Docker)
	@for m in $(MODULES); do echo "== test $$m"; (cd $$m && go test -race -count=1 ./...) || exit 1; done

test-integration: ## Integration tests (testcontainers, needs Docker) — root module
	go test -tags=integration -timeout 15m -count=1 ./...

fmt: ## gofmt -w the tree
	gofmt -w ./services ./tools ./contracts ./internal

fmt-check: ## Fail if anything is not gofmt-clean (what CI does)
	@u="$$(gofmt -l ./services ./tools ./contracts ./internal)"; if [ -n "$$u" ]; then echo "not gofmt-clean:"; echo "$$u"; exit 1; fi

lint: lint-go lint-sh ## golangci-lint (both modules) + shellcheck

lint-go: tools
	@for m in $(MODULES); do echo "== golangci-lint $$m"; (cd $$m && "$(GOBIN)/golangci-lint" run ./...) || exit 1; done

lint-sh: ## shellcheck the proofs and init scripts
	shellcheck -x -S warning scripts/*.sh infrastructure/init/*.sh

vuln: tools ## govulncheck both modules (the gating CVE scan)
	@for m in $(MODULES); do echo "== govulncheck $$m"; (cd $$m && "$(GOBIN)/govulncheck" ./...) || exit 1; done

tools: ## Install the pinned linters into GOPATH/bin
	@command -v "$(GOBIN)/golangci-lint" >/dev/null 2>&1 || go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	@command -v "$(GOBIN)/govulncheck" >/dev/null 2>&1 || go install golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION)

# ---- Frontend ------------------------------------------------------------------------------------

frontend-install: ## npm ci
	cd frontend && npm ci --no-audit --no-fund

frontend-lint: ## eslint (a gate in CI)
	cd frontend && npm run lint

frontend-typecheck: ## tsc --noEmit
	cd frontend && npx tsc --noEmit

frontend-test: ## vitest
	cd frontend && npm test

frontend-build: ## next build
	cd frontend && NEXT_TELEMETRY_DISABLED=1 npm run build

frontend: frontend-lint frontend-typecheck frontend-test frontend-build ## Everything the frontend CI job runs

# ---- Stack ---------------------------------------------------------------------------------------

up: ## Boot the compose stack (detached, rebuild images)
	docker compose up -d --build

up-observability: ## Boot the stack plus collector, Tempo, Prometheus and Grafana (http://127.0.0.1:3001)
	OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318 docker compose --profile observability up -d --build

down: ## Tear the stack down, including volumes
	docker compose --profile observability down -v --remove-orphans

logs: ## Follow all container logs
	docker compose logs -f --no-color

e2e: ## The golden-path proof (boots and tears down its own stack)
	bash scripts/e2e.sh

failtests: ## Every failure-survival proof, serially
	@for s in scripts/failtest_*.sh; do echo "== $$s"; bash "$$s" || exit 1; done

# ---- Aggregate -----------------------------------------------------------------------------------

check: fmt-check build vet test lint frontend-lint frontend-typecheck ## CI's fast tier, locally

clean: ## Remove proof logs and build output
	rm -rf .proof-logs frontend/.next

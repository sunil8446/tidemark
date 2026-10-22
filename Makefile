COMPOSE := docker compose

.PHONY: help env up up-ha up-query up-all down nuke ps logs topics smoke

help: ## List targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-10s %s\n", $$1, $$2}'

env: ## Create .env from .env.example if missing
	@test -f .env || cp .env.example .env

up: env ## Lite stack: 1 broker, MinIO, Postgres, Prometheus, Grafana
	$(COMPOSE) up -d

up-ha: env ## Lite stack plus brokers 2 and 3
	$(COMPOSE) --profile ha up -d

up-query: env ## Lite stack plus Trino and ClickHouse
	$(COMPOSE) --profile query up -d

up-all: env ## Everything (needs ~8 GB for Docker)
	$(COMPOSE) --profile ha --profile query up -d

down: ## Stop all containers, keep data
	$(COMPOSE) --profile ha --profile query down

nuke: ## Stop all containers and DELETE all volumes
	$(COMPOSE) --profile ha --profile query down -v

ps: ## Show container status
	$(COMPOSE) --profile ha --profile query ps

logs: ## Tail logs (make logs S=trino)
	$(COMPOSE) --profile ha --profile query logs -f $(S)

topics: ## Create Kafka topics (RF follows broker count)
	./scripts/create-topics.sh

smoke: ## Check every running service answers
	./scripts/smoke.sh

# ---------------------------------------------------------------- Week 1: ingestion
.PHONY: build test test-integration ingest replay gen

gen: ## Regenerate Protobuf code
	PATH="$$PATH:$$(go env GOPATH)/bin" buf lint && PATH="$$PATH:$$(go env GOPATH)/bin" buf generate

build: ## Build ingest and replay binaries into ./bin
	go build -o bin/ingest ./ingest/cmd/ingest
	go build -o bin/replay ./replay/cmd/replay

test: ## Unit tests (race detector on)
	go test -race ./...

test-integration: ## Integration tests against a throwaway Redpanda container
	go test -tags integration -count=1 ./ingest/sink/

ingest: build ## Run the live ingestor against the local stack
	./bin/ingest --debug-endpoints

replay: build ## Replay recordings: make replay FILES="~/.tidemark-data/recordings/*.zst" SPEED=10
	./bin/replay --speed $(or $(SPEED),1) $(FILES)

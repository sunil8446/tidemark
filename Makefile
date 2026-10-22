COMPOSE := docker compose
# Build outputs live outside the synced repo folder.
BIN := $(HOME)/.tidemark-data/bin

.PHONY: help env up up-ha up-query up-stream up-orchestrate up-all down nuke ps logs topics smoke

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

up-stream: env ## Lite stack plus Trino, ClickHouse and Flink
	$(COMPOSE) --profile query --profile stream up -d

up-orchestrate: env ## Add Airflow (reconcile + dbt + Iceberg maintenance DAGs)
	$(COMPOSE) --profile query --profile stream --profile orchestrate up -d

up-all: env ## Everything (needs ~8 GB for Docker)
	$(COMPOSE) --profile ha --profile query --profile stream --profile orchestrate up -d

down: ## Stop all containers, keep data
	$(COMPOSE) --profile ha --profile query --profile stream --profile orchestrate down

nuke: ## Stop all containers and DELETE all volumes
	$(COMPOSE) --profile ha --profile query --profile stream --profile orchestrate down -v

ps: ## Show container status
	$(COMPOSE) --profile ha --profile query --profile stream --profile orchestrate ps

logs: ## Tail logs (make logs S=trino)
	$(COMPOSE) --profile ha --profile query --profile stream --profile orchestrate logs -f $(S)

topics: ## Create Kafka topics (RF follows broker count)
	./scripts/create-topics.sh

smoke: ## Check every running service answers
	./scripts/smoke.sh

# ---------------------------------------------------------------- Week 1: ingestion
.PHONY: build test test-integration ingest replay gen

gen: ## Regenerate Protobuf code
	PATH="$$PATH:$$(go env GOPATH)/bin" buf lint && PATH="$$PATH:$$(go env GOPATH)/bin" buf generate

build: ## Build ingest, replay and audit binaries into ~/.tidemark-data/bin
	go build -o $(BIN)/ingest ./ingest/cmd/ingest
	go build -o $(BIN)/replay ./replay/cmd/replay
	go build -o $(BIN)/audit ./ingest/cmd/audit

test: ## Unit tests (race detector on)
	go test -race ./...

test-integration: ## Integration tests against a throwaway Redpanda container
	go test -tags integration -count=1 ./ingest/sink/

ingest: build ## Run the live ingestor against the local stack
	$(BIN)/ingest --debug-endpoints

replay: build ## Replay recordings: make replay FILES="~/.tidemark-data/recordings/*.zst" SPEED=10
	$(BIN)/replay --speed $(or $(SPEED),1) $(FILES)

# ---------------------------------------------------------------- Week 2: stream processing
.PHONY: flink-build flink-test flink-submit flink-jobs

flink-build: ## Build the Flink job jar (and Flink lib/ extras) into ~/.tidemark-data/build
	cd flink && mvn -q -B package -DskipTests
	cd flink && mvn -q -B dependency:copy -Dartifact=org.postgresql:postgresql:42.7.7 -DoutputDirectory=$(HOME)/.tidemark-data/build/flink-lib

flink-test: ## Flink unit tests
	cd flink && mvn -q -B test

flink-submit: ## Submit the lakehouse job (JOB=dev.tidemark.flink.LakehouseJob ARGS="...")
	docker exec jobmanager flink run -d -c $(or $(JOB),dev.tidemark.flink.LakehouseJob) /opt/flink/usrlib/tidemark-flink.jar $(ARGS)

flink-jobs: ## List running Flink jobs
	docker exec jobmanager flink list

# ---------------------------------------------------------------- Week 2: lakehouse ops
.PHONY: dbt-build reconcile

DBT_ENV := DBT_TARGET_PATH=$(HOME)/.tidemark-data/build/dbt/target DBT_LOG_PATH=$(HOME)/.tidemark-data/build/dbt/logs

dbt-build: ## dbt build (models + tests) against local Trino
	cd dbt && $(DBT_ENV) $(HOME)/.local/bin/dbt build --profiles-dir .

reconcile: ## Check lakehouse completeness for the last 2h and repair gaps over REST
	go run ./ingest/cmd/reconcile --window 2h --settle 10m

# Airflow with the Tidemark tools baked in: the Go reconcile binary and dbt (in
# its own virtualenv so its pins cannot clash with Airflow's).
FROM golang:1.27 AS tools
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY gen ./gen
COPY schemas ./schemas
COPY ingest ./ingest
COPY replay ./replay
RUN CGO_ENABLED=0 go build -o /out/reconcile ./ingest/cmd/reconcile \
 && CGO_ENABLED=0 go build -o /out/audit ./ingest/cmd/audit

FROM apache/airflow:slim-3.3.2-python3.12
USER root
COPY --from=tools /out/ /opt/tidemark/bin/
USER airflow
RUN pip install --no-cache-dir psycopg2-binary==2.9.10 asyncpg==0.30.0 trino==0.335.0 \
 && python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir dbt-core==1.12.6 dbt-trino==1.10.6

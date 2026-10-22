# Architecture decision records

One page each: the decision, the alternatives, and why. Copy [0000-template.md](0000-template.md) to start a new one.

| ADR | Title | Status | Week |
|---|---|---|---|
| [0001](0001-local-object-store.md) | Use a pinned legacy MinIO image for local object storage | Accepted | 0 |
| [0002](0002-go-for-ingestion.md) | Use Go for ingestion | Accepted | 1 |
| [0003](0003-protobuf-over-avro.md) | Use Protobuf rather than Avro for event schemas | Accepted | 1 |
| [0004](0004-flink-over-spark-streaming.md) | Use Flink (1.20 LTS) rather than Spark Structured Streaming | Accepted | 2 |
| [0005](0005-iceberg-over-delta.md) | Use Apache Iceberg rather than Delta Lake | Accepted | 2 |

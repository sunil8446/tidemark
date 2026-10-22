# Schema evolution drill (Week 2)

**Goal:** add a field to the event schema and show every layer accepting it without downtime, data loss, or a table rewrite.

**Change:** `Trade.venue_sequence` (field 12, `uint64`): Coinbase's feed sequence number, useful later for ordering checks.

## Steps and evidence (2026-10-22)

| Layer | What happened | Evidence |
|---|---|---|
| Protobuf | Field 12 added to `Trade` | `buf breaking --against .git#branch=main` passes (additive) |
| Schema registry | BACKWARD compatibility accepts it | `go test -tags integration ./ingest/sink/` registers an additive change and rejects a type change on `trade_id` |
| Flink | Job stopped **with a savepoint** and the new jar restored from it | `savepoint-44ace9-…` → job `6e0dbf7c`, restored state and offsets |
| Iceberg | The job's `ensureColumns` added `venue_sequence bigint`: a metadata-only commit | `DESCRIBE` shows 14 columns; **all 34 data files that existed before are still referenced**; new snapshots are plain `append`s |
| Old data | Reads the new column as `NULL` | 41,427 / 41,427 existing rows have `venue_sequence IS NULL` |
| Correctness | Completeness across the upgrade **and** a TaskManager kill right after it | `reconcile --window 1h`: 40,492 / 40,492 ids, 0 duplicates |

The producer side (the ingestor filling `venue_sequence` for Coinbase) is already in the code. It is deployed once the 24-hour unattended run completes, so that run stays unattended. Until then, every row reads the new column as NULL, which is exactly what old producers mean under proto3 defaults.

## Why no rewrite was needed

Iceberg tracks columns by **field id**, not by name or position. Adding a column creates a new schema version in table metadata. Data files written under the old schema simply lack that field id, and readers fill it with NULL. Renames and type widening work the same way. Hive-style tables need a rewrite (or fragile positional mapping) for the same change.

## Found during the drill

The first restore failed repeatedly with `No suitable driver found for jdbc:postgresql://…`. JDBC's `DriverManager` remembers drivers per classloader. After a job restart, the driver registered by the previous job's classloader is not visible to the new one. The fix, now in `docker-compose.yml`, is to put the Postgres driver in Flink's `lib/` and load `org.postgresql` parent-first. A deliberate TaskManager kill afterwards recovered cleanly in about 75 s.

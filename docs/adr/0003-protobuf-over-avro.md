# ADR-0003: Use Protobuf rather than Avro for event schemas

- **Status:** Accepted
- **Date:** 2026-10-22
- **Week:** 1

## Context

Trades, book updates and gap events cross three languages (Go producer, Java Flink, Python agent tools) and must evolve over the project. Examples: adding a field in the Week 2 schema-evolution drill, and adding labels for injected anomalies in Week 3. The registry must reject breaking changes before they reach consumers.

## Decision

Define all events in one Protobuf file, `schemas/tidemark/v1/market.proto`, register it in the Redpanda schema registry under each topic's value subject with **BACKWARD** compatibility, and frame records with the Confluent wire header (magic byte, schema id, message index).

## Alternatives considered

| Option | Why not |
|---|---|
| Avro | Strong registry tooling, but the Go Avro libraries are weaker than generated Protobuf types. Avro needs the writer schema to decode, which makes ad-hoc debugging harder. |
| JSON Schema | No compact binary form, and exact decimals would still need string fields. Weakest compatibility checks. |
| Plain JSON, no registry | No enforcement. The first breaking change shows up as a Flink deserialisation error at 3 a.m. |

## Consequences

- **Good:** generated types in Go and Java. `buf lint` runs in CI. Field numbers make evolution rules explicit: never reuse a number, reserve removed ones, enum 0 is always UNSPECIFIED. The integration test proves the registry accepts an added field and rejects a type change on `trade_id`.
- **Bad:** proto3 has no "required", so validation lives in code. One file shared by three subjects means a change to any message is checked against all three (acceptable: they evolve together).
- **Prices and sizes are strings**, keeping the venue's exact decimal text. Floats would corrupt the Kraken checksum and introduce rounding in VWAP.
- **Revisit if:** a consumer needs schema-on-read from a tool with only Avro support.

## Evidence

`go test -tags integration ./ingest/sink/` round-trips 500 trades, a book update and a gap event through Redpanda, checks per-key ordering, and exercises the BACKWARD check (2026-10-22).

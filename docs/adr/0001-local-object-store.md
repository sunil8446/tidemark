# ADR-0001: Use a pinned legacy MinIO image for local object storage

- **Status:** Accepted
- **Date:** 2026-10-22
- **Week:** 0

## Context

The lakehouse (Iceberg data files, Flink checkpoints) needs S3-compatible storage locally. The plan named MinIO. During Week 0 setup, the official `minio/minio` and `minio/mc` images were no longer available on Docker Hub or Quay. In the cloud the stack uses real S3, so the local store only has to behave like S3 for Iceberg's S3FileIO, Trino's native S3 client and Flink's checkpoint storage.

## Decision

Use `bitnamilegacy/minio:2025.7.23`, pinned, for local development only. Every component reads the endpoint, bucket and credentials from configuration, so the store can be swapped without code changes.

## Alternatives considered

| Option | Why not |
|---|---|
| Build MinIO from source | Extra build step and image maintenance for no gain at this stage |
| SeaweedFS | Actively maintained, but less common in Iceberg tutorials and the S3 edge cases are less proven with Trino and Flink |
| RustFS | Young project, not worth the risk inside a 4-week plan |
| LocalStack S3 | Heavier, and the free tier is aimed at AWS API emulation, not a data lake |

## Consequences

- **Good:** zero code changes versus the original plan. Bucket creation is built into the image (`MINIO_DEFAULT_BUCKETS`), so no `mc` sidecar is needed.
- **Bad:** the image is frozen and gets no security updates. Acceptable because it only binds to localhost on a dev laptop and never holds real data.
- **Revisit if:** the image disappears, an S3 behaviour bug blocks Iceberg or Flink, or the stack needs a long-lived shared environment. SeaweedFS is the first fallback.

## Evidence

`docker manifest inspect` checks on 2026-10-22: `minio/minio` and `quay.io/minio/minio` returned not found or access denied; `bitnamilegacy/minio:2025.7.23` resolved.

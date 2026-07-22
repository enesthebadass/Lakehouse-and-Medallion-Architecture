# ADR-0001: CDC Pipeline Control Plane Ownership

- Status: Accepted for production-shaped PoC
- Date: 2026-07-21

## Context

Airflow metadata describes scheduler execution, not immutable data boundaries. The
operational source database is owned by the source application and must not contain
lakehouse orchestration state. Using either database for CDC watermarks couples data
correctness to an unrelated lifecycle, backup policy, and ownership boundary.

## Decision

Use a dedicated PostgreSQL control database owned by the data platform. Store batch,
partition watermark, attempt, task evidence, and state-transition history there. Store
the canonical immutable manifest under
`s3://lakehouse/bronze/_control/manifests/` and retain its SHA-256 in PostgreSQL.

The manifest interval is `(watermark_low, watermark_high]`. Manifest v2 introduced
exact bounded object selection. Manifest v3 adds the source workload-ledger
transactions, PostgreSQL LSN boundary, and event totals needed for point-in-time
reconciliation without querying a moving live-source count.

The current state machine is:

```text
CREATED -> RUNNING -> VALIDATED -> PUBLISHED
              |
              +----> FAILED -> RUNNING

CREATED or FAILED -> SUPERSEDED
```

Only one unfinished batch is allowed. A retry uses the existing `batch_id` and
manifest but creates a new attempt. Every Raw Vault task records the same manifest
checksum before publish. State transitions are insert-only evidence even though the
batch table also keeps its current state.

The latest `PUBLISHED` batch may be replayed with a new Airflow run and the same
`batch_id`. Its batch state remains `PUBLISHED` while the replay attempt runs, so a
failed replay cannot invalidate the last durable watermark. A running replay blocks
new manifest creation, and an older published batch cannot be replayed after a newer
batch exists.

## Consequences

- Airflow can be rebuilt without losing published watermarks.
- Source ownership and transaction load remain independent from the lakehouse.
- Trino exposes the PostgreSQL catalog for operational and audit queries.
- Each attempt owns an immutable audit URI/checksum and append-only rule results.
- Publish requires a passing audit, not merely successful scheduler task states.
- Production still requires HA PostgreSQL, TLS, secret management, backup/PITR,
  retention, monitoring, and an approved operator procedure for superseding a batch.

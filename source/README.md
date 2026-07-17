# Synthetic Core Banking Source

This directory bootstraps the local `core-banking-source` PostgreSQL service.
It exists to make the CDC demo behave like an operational source system instead
of generating files directly in Bronze.

## Scope

The schemas are deliberately synthetic:

- `mms`: customer, address, contact, and relationship records
- `krd`: loan application, loan, installment, and collateral records
- `prm`: currency, branch, product, status, and rate reference data

The names are inspired by module boundaries mentioned during architecture discovery.
No access to the bank's real schema definitions was used, and this model must not be
presented as a copy of the real Oracle implementation.

## Bootstrap Order

PostgreSQL executes the files in `init/` alphabetically when the data volume is empty:

1. `001_create_schemas.sql`
2. `002_create_source_tables.sql`
3. `003_seed_reference_data.sql`
4. `004_create_workload_control.sql`
5. `005_configure_cdc.sh`

The `simulator` schema created by the fourth file stores workload run and event
metadata. It is a local control schema and must be excluded from CDC capture.
The fifth file creates the dedicated replication login, explicit publication, and
full replica identity required by the local Debezium event contract. Its credentials
come from `CDC_DB_USER` and `CDC_DB_PASSWORD`; the script does not embed a password.

## Workload Simulator

The simulator writes only to PostgreSQL. It does not bypass CDC by writing directly
to Bronze. Its business timestamps, identifiers, and generated values are stable for
the same seed and base date.

Create the initial operational snapshot:

```bash
docker compose run --rm core-banking-workload snapshot \
  --run-id demo-snapshot-v1
```

Generate committed insert, update, and delete scenarios:

```bash
docker compose run --rm core-banking-workload changes \
  --run-id demo-changes-v1
```

The change run includes customer address/status changes, a contact deletion, loan
approval and disbursement, installment payment/delinquency, and an effective-dated
rate change. Reusing a completed `run-id` is a no-op. A new change batch therefore
requires a new run ID such as `demo-changes-v2`.

Inspect run and event outcomes:

```bash
docker compose exec core-banking-source sh -c \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT run_id, workload_type, status FROM simulator.workload_runs ORDER BY started_at;"'
```

A later implementation step will capture the `mms`, `krd`, and `prm` changes with
Debezium and Kafka Connect.

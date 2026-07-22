# CDC Data Vault Mapping

This contract maps the synthetic operational source to the CDC-aligned Raw Vault at
`silver/cdc_raw_vault`. It does not claim to model the bank's real Oracle schemas.

## Hash Standard

All Hub and Link hash keys use this deterministic contract:

1. Use SHA-256 and store the lowercase hexadecimal result produced by Spark `sha2`.
2. Add an uppercase entity namespace as the first component, such as `CUSTOMER`.
3. Cast each component to string, trim surrounding whitespace, and uppercase it.
4. Replace null or blank components with `^^`; required business keys are rejected
   before hashing, so the token primarily defines the cross-tool standard.
5. Join components in the documented order with `||`.

Examples:

```text
customer_hk = SHA256("CUSTOMER||C00004200000001")
loan_hk     = SHA256("LOAN||L00004200000001")

application_context_hk = SHA256(
  "APPLICATION_CONTEXT||<application_no>||<customer_no>||<product_code>||<branch_code>||<currency_code>"
)
```

Changing normalization, namespace, delimiter, component order, or algorithm is a
breaking data-contract change and requires controlled Raw Vault re-keying/rebuild.

## Source Mapping

| Source table | Business grain/key | Raw Vault mapping | Status |
|---|---|---|---|
| `mms.customers` | `customer_no` | `hub_customer`, `sat_customer_details` | Hub and Satellite implemented |
| `mms.customer_addresses` | customer + address occurrence | customer-address dependent Link and Satellite | Designed |
| `mms.customer_contacts` | customer + contact occurrence | customer-contact dependent Link and Satellite | Designed |
| `mms.customer_relations` | source customer + target customer + relation occurrence | customer-relation Link and Link Satellite | Designed |
| `krd.loan_applications` | `application_no` | `hub_loan_application`, `link_application_context`, `sat_loan_application_details` | Implemented |
| `krd.loans` | `loan_no` | `hub_loan`, `link_loan_context`, `sat_loan_details` | Implemented |
| `krd.installments` | loan + `installment_no` | loan-installment dependent Link and Satellite | Designed |
| `krd.collaterals` | collateral type + reference | collateral Hub, loan-collateral Link, Satellite | Designed |
| `prm.currencies` | `currency_code` | `hub_currency`, `sat_currency_details` | Implemented |
| `prm.branches` | `branch_code` | `hub_branch`, `sat_branch_details` | Implemented |
| `prm.products` | `product_code` | `hub_product`, `sat_product_details`; product-currency Link remains designed | Hub and Satellite implemented |
| `prm.status_codes` | domain + status code | status reference Hub and Satellite | Designed |
| `prm.rate_parameters` | product + rate type + effective date | product-rate dependent Link and Satellite | Designed |

The source identity columns such as `customer_id`, `application_id`, and `loan_id` are
used for source-side joins when necessary, but are not selected as durable business
keys. For example, `krd.loans.application_id` is resolved to `application_no` before
the Loan Context Link is hashed.

## Implemented Tables

The current job writes six Hubs:

```text
hub_customer
hub_loan_application
hub_loan
hub_product
hub_branch
hub_currency
```

It also writes two Links:

```text
link_application_context
  application -> customer -> product -> branch -> currency

link_loan_context
  loan -> application -> customer -> product -> branch -> currency
```

Six source-aligned Satellites retain descriptive changes:

```text
sat_customer_details
sat_loan_application_details
sat_loan_details
sat_product_details
sat_branch_details
sat_currency_details
```

`sat_source_record_status` records `ACTIVE` and `DELETED` source-state transitions
for all 13 captured tables. Invalid structural events and missing implemented Hub
business keys are retained under `silver/quarantine/cdc_raw_vault_events`. The
source/Bronze/Silver count report is stored under
`silver/audit/cdc_raw_vault_reconciliation`.

Three current-state Satellites make that history directly usable by Gold:

```text
sat_entity_record_status
sat_application_context_effectivity
sat_loan_context_effectivity
```

The first attaches `ACTIVE`/`DELETED` state to implemented Hub hash keys. The two
effectivity Satellites record each source event's active relationship context, so a
return from A to B to A remains visible even though the original A Link already exists.

Each row retains `load_datetime`, source event time, `record_source`, batch ID,
Debezium LSN, Kafka topic/partition/offset, Bronze object key, and source event ID.

## Incremental Semantics

The job deduplicates each staged batch by the Hub or Link hash key and keeps the first
arrival. Delta `MERGE` inserts only hash keys absent from the target. Existing Hub and
Link rows are never updated or deleted.

Satellite events are ordered by effective time, source LSN, Kafka partition, and
offset. A new row is retained only when its source-aligned payload hashdiff differs
from the preceding version. The source event ID is the idempotent merge identity, so
an A -> B -> A payload sequence preserves all three valid historical events while an
exact replay inserts nothing.

Deletes never remove Raw Vault rows. They create `DELETED` transitions in generic
`sat_source_record_status` and Hub-attached `sat_entity_record_status`; a later
reappearance creates another `ACTIVE` transition. Shared ephemeral dbt current models
exclude deleted entities and select the latest active relationship by source position.
The source contract treats a loan's `application_id` as immutable after loan creation;
changing that foreign key requires an explicit remapping rule rather than an ordinary
descriptive update.
The validation phase writes rejected records with a deterministic identity, reason
code, raw payload, Bronze reference, and batch ID before downstream loading.

The Airflow path seals a manifest v3 boundary before processing. Spark opens only the
exact immutable Bronze object list in `(watermark_low, watermark_high]`; a zero-object
batch exits before a Spark session is created. Target-side Delta `MERGE` preserves
idempotency, while the first Satellite event in a batch is compared with persisted
target state to prevent boundary regressions.

The source workload ledger stores each committed simulator transaction's PostgreSQL
transaction ID, WAL LSN boundary, and expected CDC event count in that same source
transaction. Reconciliation compares this frozen ledger set with only the manifest's
Bronze objects. It also verifies unique Kafka coordinates, accepted plus quarantine
conservation, and batch-to-target effects for every implemented Hub, Link, Satellite,
status, and quarantine table. Results are retained in Delta and as an immutable,
attempt-specific JSON audit artifact; publish fails closed when any rule fails.

## Run Locally

```bash
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --driver-memory 2g \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3 \
  /opt/spark/scripts/4_process_cdc_raw_vault.py
```

Use `--phase validate`, `core`, `satellites`, or `reconcile` to run one stage. The
Airflow DAG `cdc_raw_vault_incremental` exposes those stages as four dependent tasks.
Run `scripts/audit_cdc_raw_vault.py` to verify event uniqueness, multi-version
Satellite history, delete preservation, and the latest reconciliation batch.

The Airflow path also passes `--batch-id`, `--attempt-number`, `--airflow-run-id`,
`--manifest-uri`, and `--manifest-sha256`. All four phases verify the same immutable
manifest and, for v2/v3,
open only the exact object keys in `(watermark_low, watermark_high]`. The first bounded
Satellite row is compared with persisted target state; a non-advancing LSN/offset fails
closed. Direct Spark execution without a manifest is a diagnostic fallback.

`gold_dbt.gold_row_lineage` maps each current Gold business key to its contributing
Raw Vault object, source event ID, Bronze object, Kafka coordinate, source LSN, and
load batch. `load_batch_id` joins to the control catalog for manifest, attempt, audit,
and publication evidence.

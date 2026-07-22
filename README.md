# Lakehouse and Medallion Architecture Demo

[![CI](https://github.com/enesthebadass/Lakehouse-and-Medallion-Architecture/actions/workflows/ci.yml/badge.svg)](https://github.com/enesthebadass/Lakehouse-and-Medallion-Architecture/actions/workflows/ci.yml)

Reproducible local demo of a banking-oriented lakehouse pipeline using Docker,
Airflow, Spark, MinIO, and Delta Lake.

The project generates intentionally dirty banking data, stores it in a Bronze
object-storage layer, cleans and models it as a Data Vault 2.0 Raw Vault in
Silver, then publishes analytics-ready Kimball-style Gold tables.

The repository also contains a realistic local ingestion path: a separate synthetic
operational PostgreSQL database with `mms`, `krd`, and `prm` schemas feeds Debezium
and Kafka through logical replication. An idempotent consumer lands those events in
immutable raw Bronze objects. The original direct-to-Bronze generator remains available
as a demo fallback and manages only its own legacy prefixes.

## Architecture

The editable diagrams.net source is
[`LAKEHOUSE_ARCHITECTURE.drawio`](LAKEHOUSE_ARCHITECTURE.drawio). Its first page
shows the verified local PoC; the second separates the bank integration target and
external production gates; the third is a slide-ready simplified Source-to-BI data
flow.

```text
Operational CDC path:
Synthetic source -> PostgreSQL -> Debezium -> Kafka -> MinIO Bronze CDC
  -> immutable manifest v3, source transaction ledger, and PostgreSQL control plane
  -> incremental Silver CDC Raw Vault Hubs, Links, Satellites, and audit controls
  -> Hive Metastore -> Trino SQL -> dbt Gold mart and row lineage -> BI

Production source blueprint:
Oracle -> Debezium Oracle LogMiner adapter -> Kafka -> the governed CDC contract
(requires Oracle DBA, license, security, network, and source-owner approval)

Current regression/demo path:
Faker -> Bronze JSONL -> Silver Data Vault Delta -> Gold dimensional Delta
  -> Hive Metastore -> Trino SQL
```

Main components:

- **Airflow** orchestrates the pipeline.
- **Spark** processes Bronze, Silver, and Gold data.
- **MinIO** provides a local S3-compatible object store.
- **Delta Lake** stores Silver and Gold tables with transaction logs.
- **PostgreSQL** stores Airflow metadata.
- **Pipeline Control PostgreSQL** independently stores batch watermarks, attempts,
  source transactions, task evidence, immutable audit references, rule results, and
  state transitions; Trino exposes it as `pipeline_control`.
- **Core Banking Source PostgreSQL** is a separate synthetic operational source
  prepared for the CDC pilot. It is not a copy of the bank's Oracle schemas.
- **Debezium and Kafka Connect** capture PostgreSQL row changes from logical WAL.
- **Apache Kafka** retains ordered CDC events for downstream Bronze ingestion.
- **Bronze CDC Writer** stores replay-safe raw Kafka records in MinIO and commits
  offsets only after each object write is verified.
- **CDC Raw Vault Job** uses insert-only Delta merges to load durable business keys,
  relationships, descriptive history, Hub-attached delete status, Link effectivity,
  quarantine, and reconciliation without rebuilding existing history.
- **Apache Hive Metastore** keeps the technical table catalog in a dedicated
  PostgreSQL database and resolves the Spark-written S3A table locations.
- **Trino** exposes the Delta tables through the `lakehouse` SQL catalog for
  analysts, validation queries, and future Power BI connectivity.
- **dbt Core** builds documented and tested Gold SQL models from the CDC Raw Vault
  through Trino. Its customer and lending marts deliberately exclude raw PII from
  their BI surface. `gold_row_lineage` maps every current Gold business row to its
  contributing source event, Kafka coordinate, Bronze object, LSN, and load batch.
- **OpenMetadata** provides the optional searchable governance catalog, dbt metadata,
  glossary, domain, ownership, classification, and lineage surface. It does not
  replace Hive Metastore or enforce Trino query authorization.
- **Prometheus and the pipeline metrics exporter** collect CDC status, Kafka lag,
  Airflow/Spark execution, Trino query, reconciliation, and Gold freshness signals.
- **Grafana and Alertmanager** provide a provisioned operations dashboard and local
  alert evaluation. The local Alertmanager deliberately has no notification receiver.

## Repository Layout

```text
.
|-- dags/
|   |-- cdc_raw_vault_dag.py
|   `-- medallion_dag.py
|-- cdc/
|   |-- bronze/
|   |   |-- Dockerfile
|   |   |-- requirements.txt
|   |   |-- writer.py
|   |   `-- README.md
|   |-- connectors/
|   |   |-- core-banking-postgres.json
|   |   `-- core-banking-oracle-logminer.template.json
|   |-- oracle-readiness.yaml
|   |-- register_connector.py
|   `-- README.md
|-- scripts/
|   |-- 1_generate_bronze.py
|   |-- 2_process_silver.py
|   |-- 3_process_gold.py
|   |-- 4_process_cdc_raw_vault.py
|   `-- audit_cdc_raw_vault.py
|-- source/
|   |-- init/
|   |   |-- 001_create_schemas.sql
|   |   |-- 002_create_source_tables.sql
|   |   |-- 003_seed_reference_data.sql
|   |   |-- 004_create_workload_control.sql
|   |   `-- 005_configure_cdc.sh
|   `-- workload/
|       |-- Dockerfile
|       |-- requirements.txt
|       `-- workload.py
|-- hive/
|   `-- conf/core-site.xml
|-- trino/
|   |-- etc/
|   |-- register_tables.py
|   `-- README.md
|-- dbt/
|   |-- models/
|   |-- dbt_project.yml
|   |-- profiles.yml
|   |-- requirements.txt
|   `-- Dockerfile
|-- bi/
|   |-- sql/kpi_queries.sql
|   |-- export_powerbi_csv.sh
|   `-- README.md
|-- ci/
|   |-- trino/bootstrap.sql
|   `-- README.md
|-- governance/
|   |-- catalog/governance-blueprint.yaml
|   |-- ingestion/
|   `-- README.md
|-- security/
|   |-- ranger/
|   |-- trino/
|   |-- access-matrix.yaml
|   |-- verify_access_control.py
|   `-- README.md
|-- platform/
|   |-- kubernetes/examples/
|   |-- policies/kubernetes/
|   |-- deployment-responsibilities.yaml
|   |-- iam-group-mapping.yaml
|   `-- README.md
|-- observability/
|   |-- exporter/
|   |-- prometheus/
|   |-- alertmanager/
|   |-- grafana/
|   `-- metric-contract.yaml
|-- operations/
|   |-- end_to_end_smoke_test.sh
|   `-- reproducibility_test.sh
|-- tests/
|   |-- test_airflow_dags.py
|   |-- test_access_control_policy.py
|   |-- test_platform_blueprint.py
|   |-- test_governance_blueprint.py
|   |-- test_observability_blueprint.py
|   `-- test_final_package.py
|-- .github/
|   |-- workflows/ci.yml
|   `-- dependabot.yml
|-- .env.example
|-- docker-compose.yml
|-- docker-compose.access-control.yml
|-- docker-compose.openmetadata.yml
|-- docker-compose.observability.yml
|-- OPERATIONS_RUNBOOK.md
|-- Dockerfile.airflow
|-- Dockerfile.hive-metastore
|-- pyproject.toml
|-- requirements-dev.txt
|-- requirements.txt
`-- README.md
```

## Prerequisites

- Docker Desktop or Docker Engine with Docker Compose
- At least 8 GB available RAM for the containers
- Ports available on the host:
  - `8080` for Spark Master UI
  - `8081` for Airflow UI
  - `9000` for MinIO S3 API
  - `9001` for MinIO Console
  - `5433` for the synthetic core-banking PostgreSQL source
  - `29092` for Kafka access from the host
  - `8083` for the Kafka Connect REST API
  - `8082` for Trino UI and SQL
  - `9083` for the local Hive Metastore Thrift service
  - `8585` for the optional OpenMetadata UI and API
  - `8586` for the optional OpenMetadata admin health endpoint
  - `3000` for the optional Grafana UI
  - `9090` for the optional Prometheus UI
  - `9093` for the optional Alertmanager UI
  - `9108` for the optional pipeline metrics endpoint

## Quick Start

From the repository root:

```bash
docker compose up -d --build core-banking-source
docker compose --profile tools run --rm core-banking-workload snapshot --run-id quickstart-snapshot-v1
docker compose up -d --build
```

Starting the source and creating its deterministic baseline before the remaining
services ensures that Debezium's initial snapshot sees the complete operational
dataset. Reusing the same completed run ID is idempotent.

Check that the services are healthy:

```bash
docker compose ps
```

Open the UIs:

| Service | URL | Credentials |
|---|---|---|
| Airflow | <http://localhost:8081> | `admin` / `admin` |
| MinIO Console | <http://localhost:9001> | `minioadmin` / `minioadmin` |
| Spark Master | <http://localhost:8080> | none |
| Kafka Connect API | <http://localhost:8083/connectors> | none (local only) |
| Trino | <http://localhost:8082/ui/> | any username, no password (local only) |
| OpenMetadata (optional) | <http://localhost:8585> | `admin@open-metadata.org` / `admin` (local only) |

## Synthetic Operational Source

The `core-banking-source` service is isolated from the PostgreSQL database used by
Airflow. On its first start it creates:

- `mms`: synthetic customer-oriented source tables
- `krd`: synthetic lending source tables
- `prm`: synthetic reference and parameter tables

These names provide realistic local namespaces only. The table structures do not
claim to represent the bank's actual Oracle `MMS`, `KRD`, or `PRM` schemas.

Inspect the schemas after the container becomes healthy:

```bash
docker compose exec core-banking-source sh -c \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\dn"'
```

List the synthetic source tables:

```bash
docker compose exec core-banking-source sh -c \
  'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c \
  "SELECT schemaname, tablename FROM pg_tables WHERE schemaname IN ('\''mms'\'', '\''krd'\'', '\''prm'\'') ORDER BY 1, 2;"'
```

The SQL files under `source/init/` run only when `core-banking-db-volume` is empty.
To apply bootstrap DDL again during local development, remove the Compose volumes
with `docker compose down -v` and start the environment again. Do not use this reset
pattern for a production database.

Create a deterministic operational snapshot and then generate transactional changes:

```bash
docker compose --profile tools run --rm core-banking-workload snapshot --run-id demo-snapshot-v1
docker compose --profile tools run --rm core-banking-workload changes --run-id demo-changes-v1
```

Each change scenario commits separately and records its expected and actual outcome
under the `simulator` control schema. Running the same completed `run-id` again does
not duplicate data. See `source/README.md` for the scenario list and audit query.

## Local CDC

Kafka, Debezium Connect, and the PostgreSQL connector start with the main Compose
environment. The connector performs an initial snapshot and then streams committed
changes from the `lakehouse_cdc_slot` replication slot.

Check the connector and list CDC topics:

```bash
curl -fsS http://localhost:8083/connectors/core-banking-postgres-cdc/status

docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --list
```

Topics follow `bank.core.<schema>.<table>`. The local topic, key, partition, retention,
envelope, status, and offset contract is documented in `cdc/README.md`.

The `bronze-cdc-writer` service consumes only the allowlisted `mms`, `krd`, and `prm`
table topics. It preserves the Kafka key and full Debezium value, then writes one
deterministically named object per topic/partition/offset. Its object contract, failure
semantics, lag check, and replay procedure are documented in `cdc/bronze/README.md`.

Trigger the Airflow DAG `cdc_raw_vault_incremental` to validate Bronze, load Hubs and
Links, load Satellite/delete history, and reconcile source, Bronze, and Silver in four
visible tasks. The complete contract is documented in `cdc/DATA_VAULT_MAPPING.md`.
Current Gold models exclude deleted entities and select one active application/loan
context, including an `A -> B -> A` relationship return without duplicate fact grain.

## Oracle CDC Production Blueprint

The repository does not run an Oracle database and does not claim to validate Oracle
LogMiner. The selected production candidate is the Apache-licensed Debezium Oracle
connector with the native LogMiner adapter. XStream and GoldenGate are not selected.
The connector template deliberately contains unresolved placeholders and cannot be
deployed until the bank completes Oracle license, DBA, source-owner, security, and
network reviews.

See `ORACLE_CDC_READINESS.md` for the licensing boundary, PostgreSQL-to-Oracle event
mapping, snapshot-to-streaming procedure, DBA checklist, data-dictionary discovery,
and production acceptance gates. The machine-readable contract is stored in
`cdc/oracle-readiness.yaml`.

## Run the Pipeline

1. Open Airflow at <http://localhost:8081>.
2. Find the DAG named `lakehouse_medallion_pipeline`.
3. Trigger the DAG manually.
4. Wait for all tasks to finish successfully:
   - `generate_bronze_dirty_data`
   - `process_silver_data_vault`
   - `process_gold_star_schema`

The first Spark run can take longer because Spark downloads Delta Lake and
Hadoop S3 dependencies.

## Register Delta Tables in Trino

After the Spark pipelines have created the Delta paths, register every available
Silver, audit, quarantine, and Gold table in the technical catalog:

```bash
docker compose run --rm trino-init
```

The command is idempotent. Required CDC Raw Vault tables fail fast when missing;
legacy Raw Vault and Gold tables are skipped until `lakehouse_medallion_pipeline`
has produced them. Run the command again after that DAG to add the optional tables.

Query the catalog:

```bash
docker compose exec trino trino --execute \
  "SHOW TABLES FROM lakehouse.cdc_raw_vault"

docker compose exec trino trino --execute \
  "SELECT count(*) FROM lakehouse.gold.fact_transactions"

docker compose exec trino trino --execute \
  'SELECT version, operation FROM lakehouse.cdc_raw_vault."hub_customer$history"'
```

The final query reads Delta transaction history rather than scanning a plain
Parquet directory. See `trino/README.md` for catalog details and limitations.

## Build and Test the dbt Gold Mart

Register the technical schemas, then build the current customer dimension and run
its source and model quality checks:

```bash
docker compose run --rm trino-init
docker compose run --rm dbt debug
docker compose run --rm dbt compile
docker compose run --rm dbt run
docker compose run --rm dbt test
```

The build creates seven customer and lending models under `lakehouse.gold_dbt` from
the latest CDC Satellite records: four conformed dimensions, two facts, and a
currency-safe portfolio aggregate. It exposes customer classification, status,
branch, and age band while excluding name, national ID, tax ID, and date of birth.
The local project uses `on_table_exists: drop` because rename-based table replacement
is not safe for this Trino Delta catalog. See `dbt/README.md` for the local profile
and `bi/README.md` for the Power BI Report Server consumption path.

## Continuous Integration

Every push and pull request runs blocking Python quality, Bronze writer unit, Docker
Compose configuration, Airflow DAG import, and isolated dbt integration checks.
Dependency, secret, configuration, and runtime image scans currently collect a
non-blocking security baseline for remediation and policy tuning.

Run the fast local checks with a Python 3.11 virtual environment:

```bash
python -m pip install -r requirements-dev.txt -r cdc/bronze/requirements.txt
ruff check .
python -m compileall -q cdc dags observability operations scripts security source trino tests
PYTHONPATH=cdc/bronze python -m unittest discover -s cdc/bronze -p 'test_*.py'
python tests/test_observability_blueprint.py
python tests/test_final_package.py
python tests/test_phase0_baseline.py
bash -n operations/end_to_end_smoke_test.sh
bash -n operations/reproducibility_test.sh
docker compose config --quiet
```

The workflow and its deterministic Trino Raw Vault fixture are documented in
`ci/README.md`.

## Reproducibility Acceptance

`operations/reproducibility_test.sh` runs the complete acceptance path in an
isolated `lakehouse-repro-*` Compose project with fresh volumes. It preserves the
normal demo volumes and writes local evidence under the Git-ignored
`tests/results/` directory. This is a reviewable local PoC acceptance test, not a
production-readiness certification.

## Correctness Baseline

Before changing batch boundaries, incremental reads, or publish behavior, collect a
stable source, Kafka, storage, Raw Vault, and Gold baseline:

```bash
python operations/collect_phase0_baseline.py \
  --output tests/results/phase0-baseline.json
```

Run the collector again with `--compare-to` after a no-op retry. It fails when a
mandatory live check or stable business fingerprint changes. The machine-readable
invariants and technical field semantics are documented in
`quality/correctness-invariants.yaml` and `quality/README.md`.

## Optional Observability Baseline

Start the local Prometheus, Grafana, Alertmanager, and pipeline exporter overlay:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.observability.yml \
  --profile observability \
  up -d --build
```

Open **Lakehouse > Lakehouse Pipeline Operations** at <http://localhost:3000>.
The default local credentials are `admin` / `admin-local-only`; override them through
`.env`. Prometheus is available at <http://localhost:9090> and the raw local metric
endpoint at <http://localhost:9108/metrics>.

The baseline evaluates connector status, Kafka consumer lag, latest Airflow and
SparkSubmit task results, Trino query counters, reconciliation outcomes, and Gold
freshness/grain. It is a single-node PoC, not an HA monitoring or production paging
system. Operational response, provisional RPO/RTO assumptions, and the end-to-end
test procedure are in `OPERATIONS_RUNBOOK.md`.

## Optional Access-Control Proof of Concept

The base stack remains unauthenticated for isolated local development. A separate
Compose overlay enables Trino password authentication and deny-by-default file-based
access control without changing the normal demo path:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.access-control.yml \
  up -d --force-recreate trino

.venv/bin/python security/verify_access_control.py
```

The integration check proves authentication failure, Gold-only analyst access,
masked identifiers for the auditor, and a branch-level row filter. File-based access
control is the executable local behavior PoC, not the production authorization target.
The production target remains Apache Ranger 2.5+ with enterprise identity, TLS, a
dedicated audit store, and SIEM forwarding. See `security/README.md` and
`SECURITY_AND_ACCESS_CONTROL.md`.

## Target Deployment and OPA Policies

The production runtime candidates remain Kubernetes, OpenShift, or the institution's
approved container platform. The repository defines component ownership, namespace
segmentation, IAM mappings, secure reference manifests, and tested OPA admission
policies without claiming that an enterprise cluster has been deployed.

Run the policy tests locally:

```bash
docker run --rm \
  --volume "$PWD/platform/policies:/policies:ro" \
  openpolicyagent/opa:1.17.0-static \
  test /policies --verbose --fail-on-empty

.venv/bin/python tests/test_platform_blueprint.py
```

The policies reject unapproved registries, mutable images, root/privileged workloads,
missing resources, plaintext secret values, default service accounts, missing seccomp
controls, and hostPath volumes. See `platform/README.md` and
`DEPLOYMENT_AND_POLICY_BLUEPRINT.md` for the rollout and ownership model.

## Optional Governance Catalog

OpenMetadata runs in a separate Compose profile so the normal CDC demo does not pay
its CPU and memory cost. Start it only after the base stack and dbt Gold models are
ready:

```bash
docker compose \
  -f docker-compose.yml \
  -f docker-compose.openmetadata.yml \
  --profile governance \
  up -d \
  openmetadata-postgresql \
  openmetadata-elasticsearch \
  openmetadata-migrate \
  openmetadata-server
```

The external ingestion workflows catalog the synthetic PostgreSQL source, Trino
Raw Vault and Gold schemas, and dbt artifacts. The local profile uses OpenMetadata's
basic login and requires an ingestion-bot JWT; its default credentials and keys must
not be exposed or treated as a production deployment.
See `governance/README.md` for the ingestion sequence and
`GOVERNANCE_AND_METADATA.md` for responsibility boundaries and the Metaworks decision.

## Expected Data Outputs

Open MinIO Console at <http://localhost:9001>, then browse the `lakehouse`
bucket.

Expected paths:

```text
bronze/
  cdc/
    source=core_banking/
      schema=<schema>/
        table=<table>/
          event_date=<date>/
  customers/
  accounts/
  merchants/
  transactions/

silver/raw_vault/
  hub_customer/
  hub_account/
  hub_merchant/
  hub_transaction/
  link_customer_account/
  link_transaction_context/
  sat_customer_profile/
  sat_account_details/
  sat_merchant_details/
  sat_transaction_details/

silver/cdc_raw_vault/
  hub_customer/
  hub_loan_application/
  hub_loan/
  hub_product/
  hub_branch/
  hub_currency/
  link_application_context/
  link_loan_context/
  sat_customer_details/
  sat_loan_application_details/
  sat_loan_details/
  sat_product_details/
  sat_branch_details/
  sat_currency_details/
  sat_source_record_status/
  sat_entity_record_status/
  sat_application_context_effectivity/
  sat_loan_context_effectivity/

silver/quarantine/
  cdc_raw_vault_events/

silver/audit/
  cdc_raw_vault_reconciliation/

bronze/_control/
  manifests/
  audit/

gold/
  dbt/
    agg_customer_loan_portfolio-<generated-id>/
    dim_branch_current-<generated-id>/
    dim_currency_current-<generated-id>/
    dim_customer_current-<generated-id>/
    dim_product_current-<generated-id>/
    fct_loan_applications_current-<generated-id>/
    fct_loans_current-<generated-id>/
    gold_row_lineage-<generated-id>/
  dim_customer/
  dim_account/
  dim_merchant/
  fact_transactions/
```

Legacy Bronze data is written as JSON Lines files; CDC Bronze data is written as one
raw JSON object per Kafka record. Silver and Gold data are Delta Lake tables, so each
table contains Parquet data files and a `_delta_log` directory.
MinIO may not preview these files directly in the browser; that is expected.

## Clean Reproducible Reset

Use the isolated acceptance script for a tested clean run:

```bash
./operations/reproducibility_test.sh
```

It accepts only a generated `lakehouse-repro-*` Compose project, rejects existing
containers or volumes with that name, runs the source-to-dashboard checks on fresh
volumes, writes evidence under `tests/results/`, and removes only the isolated
project after success. The normal demo stack must be stopped first if it occupies
the same host ports; stopping it does not delete its volumes.

`docker compose down -v` is destructive: it permanently removes the local
Airflow, source, Kafka, MinIO, metastore, and governance state. Use it only when
that data loss is intentional, then follow the source-first Quick Start sequence.

If you only want to clear the lakehouse bucket while keeping containers running:

```bash
docker exec lakehouse-medallion-demo-minio-1 mc alias set local http://localhost:9000 minioadmin minioadmin
docker exec lakehouse-medallion-demo-minio-1 mc rm --recursive --force local/lakehouse
docker exec lakehouse-medallion-demo-minio-1 mc mb local/lakehouse
docker exec lakehouse-medallion-demo-minio-1 sh -c "printf '' | mc pipe local/lakehouse/bronze/.keep"
docker exec lakehouse-medallion-demo-minio-1 sh -c "printf '' | mc pipe local/lakehouse/silver/.keep"
docker exec lakehouse-medallion-demo-minio-1 sh -c "printf '' | mc pipe local/lakehouse/gold/.keep"
```

After clearing the bucket, trigger the Airflow DAG again for the legacy demo path. To
rebuild CDC Bronze as well, follow the offset reset procedure in `cdc/bronze/README.md`;
otherwise Kafka considers the deleted events already consumed.

## Manual Execution

The recommended path is Airflow, but the scripts can also be executed manually.

Generate Bronze data from the Airflow container:

```bash
docker exec lakehouse-medallion-demo-airflow-webserver-1 python /opt/airflow/scripts/1_generate_bronze.py
```

Run Silver processing from the Spark master container:

```bash
docker exec lakehouse-medallion-demo-spark-master-1 /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/scripts/2_process_silver.py
```

Run Gold processing from the Spark master container:

```bash
docker exec lakehouse-medallion-demo-spark-master-1 /opt/spark/bin/spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/scripts/3_process_gold.py
```

Trigger the manifest-controlled incremental CDC Raw Vault DAG:

```bash
docker compose exec airflow-webserver airflow dags trigger \
  --run-id cdc-example-001 cdc_raw_vault_incremental
```

Inspect its immutable boundary and attempts through Trino:

```bash
docker compose exec trino trino --execute \
  "SELECT * FROM pipeline_control.public.v_pipeline_batch_history ORDER BY created_at DESC"
```

Manifest version 3 stores the exact `(low, high]` Bronze object keys and byte sizes,
plus frozen source workload transactions, PostgreSQL LSN boundary, and event totals.
The orchestrated Spark phases open only those paths; a zero-object batch exits before
creating a Spark session. Selected object/byte evidence is available in
`pipeline_control.public.pipeline_task_evidence`. Attempt-level audit evidence and
individual rule results are available in `pipeline_attempt_audit` and
`pipeline_audit_rule_result`; publish requires a passing audit.

Directly running the Spark application remains an unbounded diagnostic fallback and
does not create a control-plane manifest:

```bash
docker exec lakehouse-medallion-demo-spark-master-1 /opt/spark/bin/spark-submit \
  --driver-memory 2g \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3 \
  /opt/spark/scripts/4_process_cdc_raw_vault.py
```

Append `--phase validate`, `core`, `satellites`, or `reconcile` to run one stage.
See `pipeline_control/README.md` for replay and control-plane queries.
Deployments upgrading existing data must run the one-time
`--phase current-state-backfill` migration over complete Bronze history before dbt
current models are published. Normal bounded Airflow batches then maintain the three
current-state Satellites incrementally.
Verify the resulting history and controls with:

```bash
docker exec lakehouse-medallion-demo-spark-master-1 /opt/spark/bin/spark-submit \
  --driver-memory 2g \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/scripts/audit_cdc_raw_vault.py
```

## Data Model

### Bronze

Bronze stores raw, dirty JSONL data:

- customers
- accounts
- merchants
- transactions

The generator intentionally creates invalid IDs, bad dates, malformed emails,
and non-numeric amount-like values to demonstrate data quality handling.

### Silver

Silver stores a Data Vault 2.0-style Raw Vault:

- Hubs represent business keys.
- Links represent relationships.
- Satellites represent descriptive attributes.
- Audit columns such as `load_datetime` and `record_source` are included.

### Gold

Gold stores analytics-ready dimensional tables:

- `dim_customer`
- `dim_account`
- `dim_merchant`
- `fact_transactions`
- eight customer/lending and lineage models in `gold_dbt`, built and tested by dbt
  from the CDC Raw Vault

These tables are exposed through Trino for SQL clients and BI connectivity.

## Power BI Notes

Power BI Desktop Report Server does not directly read Delta tables from MinIO.
Trino now provides the local SQL serving layer at `localhost:8082`, but the Power BI
Report Server ODBC/gateway compatibility path is intentionally left for the BI phase
and has not yet been certified. The available patterns are:

- Prefer an approved Trino ODBC connection to the `lakehouse.gold` schema.
- Load Gold tables into SQL Server when Report Server connector policy requires it.
- Use controlled CSV or Parquet export only as a local fallback.

Recommended relationships:

- `dim_customer.customer_id` -> `fact_transactions.customer_id`
- `dim_account.account_id` -> `fact_transactions.account_id`
- `dim_merchant.merchant_id` -> `fact_transactions.merchant_id`

Recommended dashboard visuals:

- KPI cards for transaction count, total amount, approval rate, and decline rate
- Line chart for transaction amount by month
- Bar chart for transaction amount by merchant category
- Donut chart for transaction status distribution
- Matrix by customer segment and merchant risk band

## Troubleshooting

If `spark-master` is unhealthy, check logs:

```bash
docker logs lakehouse-medallion-demo-spark-master-1
```

If Airflow cannot import the DAG:

```bash
docker exec lakehouse-medallion-demo-airflow-webserver-1 airflow dags list-import-errors
```

If the DAG is not visible, restart the scheduler and webserver:

```bash
docker compose restart airflow-scheduler airflow-webserver
```

If Spark package downloads fail, rerun the failed Airflow task after confirming
internet access from Docker.

## License and Storage Note

This demo uses MinIO as a local S3-compatible object store. MinIO is open source
under the GNU AGPLv3 license. For enterprise or banking use, review licensing,
support, security, and compliance requirements before production adoption.

Open-source alternatives worth evaluating for production are Ceph Object Gateway
and Apache Ozone.

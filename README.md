# Lakehouse and Medallion Architecture Demo

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

```text
Operational CDC path:
Synthetic source -> PostgreSQL -> Debezium -> Kafka -> MinIO Bronze CDC
  -> incremental Silver CDC Raw Vault Hubs, Links, Satellites, and audit controls
  -> Hive Metastore -> Trino SQL -> dbt Gold mart -> BI

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
- **Core Banking Source PostgreSQL** is a separate synthetic operational source
  prepared for the CDC pilot. It is not a copy of the bank's Oracle schemas.
- **Debezium and Kafka Connect** capture PostgreSQL row changes from logical WAL.
- **Apache Kafka** retains ordered CDC events for downstream Bronze ingestion.
- **Bronze CDC Writer** stores replay-safe raw Kafka records in MinIO and commits
  offsets only after each object write is verified.
- **CDC Raw Vault Job** uses insert-only Delta merges to load durable business keys,
  relationships, descriptive history, delete status, quarantine, and reconciliation
  without rebuilding existing history.
- **Apache Hive Metastore** keeps the technical table catalog in a dedicated
  PostgreSQL database and resolves the Spark-written S3A table locations.
- **Trino** exposes the Delta tables through the `lakehouse` SQL catalog for
  analysts, validation queries, and future Power BI connectivity.
- **dbt Core** builds documented and tested Gold SQL models from the CDC Raw Vault
  through Trino. Its customer and lending marts deliberately exclude raw PII from
  their BI surface.

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
|   |   `-- core-banking-postgres.json
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
|-- .env.example
|-- docker-compose.yml
|-- Dockerfile.airflow
|-- Dockerfile.hive-metastore
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

## Quick Start

From the repository root:

```bash
docker compose up -d --build
```

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
docker compose run --rm core-banking-workload snapshot --run-id demo-snapshot-v1
docker compose run --rm core-banking-workload changes --run-id demo-changes-v1
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

silver/quarantine/
  cdc_raw_vault_events/

silver/audit/
  cdc_raw_vault_reconciliation/

gold/
  dbt/
    agg_customer_loan_portfolio-<generated-id>/
    dim_branch_current-<generated-id>/
    dim_currency_current-<generated-id>/
    dim_customer_current-<generated-id>/
    dim_product_current-<generated-id>/
    fct_loan_applications_current-<generated-id>/
    fct_loans_current-<generated-id>/
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

To remove all containers and volumes, including Airflow metadata, synthetic source
data, and MinIO data:

```bash
docker compose down -v
docker compose up -d --build
```

Use this when you want a fully clean demo run.

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

Run the incremental CDC Raw Vault job:

```bash
docker exec lakehouse-medallion-demo-spark-master-1 /opt/spark/bin/spark-submit \
  --driver-memory 2g \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4,org.postgresql:postgresql:42.7.3 \
  /opt/spark/scripts/4_process_cdc_raw_vault.py
```

Append `--phase validate`, `core`, `satellites`, or `reconcile` to run one stage.
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
- seven customer/lending models in `gold_dbt`, built and tested by dbt from the CDC Raw Vault

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

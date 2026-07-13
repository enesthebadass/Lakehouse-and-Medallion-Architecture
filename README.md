# Lakehouse and Medallion Architecture Demo

Reproducible local demo of a banking-oriented lakehouse pipeline using Docker,
Airflow, Spark, MinIO, and Delta Lake.

The project generates intentionally dirty banking data, stores it in a Bronze
object-storage layer, cleans and models it as a Data Vault 2.0 Raw Vault in
Silver, then publishes analytics-ready Kimball-style Gold tables.

## Architecture

```text
Synthetic banking data
        |
        v
Bronze - raw JSONL files in MinIO
        |
        v
Silver - cleaned Data Vault 2.0 Delta tables
        |
        v
Gold - dimensional Delta tables for reporting
```

Main components:

- **Airflow** orchestrates the pipeline.
- **Spark** processes Bronze, Silver, and Gold data.
- **MinIO** provides a local S3-compatible object store.
- **Delta Lake** stores Silver and Gold tables with transaction logs.
- **PostgreSQL** stores Airflow metadata.

## Repository Layout

```text
.
|-- dags/
|   `-- medallion_dag.py
|-- scripts/
|   |-- 1_generate_bronze.py
|   |-- 2_process_silver.py
|   `-- 3_process_gold.py
|-- docker-compose.yml
|-- Dockerfile.airflow
|-- requirements.txt
`-- README.md
```

## Prerequisites

- Docker Desktop or Docker Engine with Docker Compose
- At least 6 GB available RAM for the containers
- Ports available on the host:
  - `8080` for Spark Master UI
  - `8081` for Airflow UI
  - `9000` for MinIO S3 API
  - `9001` for MinIO Console

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

## Expected Data Outputs

Open MinIO Console at <http://localhost:9001>, then browse the `lakehouse`
bucket.

Expected paths:

```text
bronze/
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

gold/
  dim_customer/
  dim_account/
  dim_merchant/
  fact_transactions/
```

Bronze data is written as JSON Lines files. Silver and Gold data are Delta Lake
tables, so each table contains Parquet data files and a `_delta_log` directory.
MinIO may not preview these files directly in the browser; that is expected.

## Clean Reproducible Reset

To remove all containers and volumes, including Airflow metadata and MinIO data:

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

After clearing the bucket, trigger the Airflow DAG again.

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

These tables are intended for BI tools such as Power BI after exporting or
loading them through a query layer.

## Power BI Notes

Power BI Desktop Report Server does not directly read Delta tables from MinIO in
this setup. For reporting, use one of these production-style patterns:

- Export Gold tables to CSV or Parquet and load them into Power BI.
- Load Gold tables into SQL Server and connect Power BI to SQL Server.
- Use a query engine such as Trino, Spark Thrift Server, or another SQL layer in
  front of the lakehouse.

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

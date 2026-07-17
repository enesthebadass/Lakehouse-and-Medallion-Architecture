# Trino SQL Serving

This directory configures a local Trino coordinator with the Delta Lake connector,
an Apache Hive Metastore, and native S3 access to MinIO. Trino reads the same Delta
transaction logs and Parquet files written by Spark; registration adds only catalog
metadata and does not copy lakehouse data.

## Components

- `hive-metastore-postgres`: dedicated PostgreSQL metadata database
- `hive-metastore`: local Thrift metastore on port `9083`
- `trino`: SQL coordinator and web UI on host port `8082`
- `trino-init`: one-shot, idempotent registration tool under the `tools` profile
- `lakehouse` catalog: Delta Lake connector backed by HMS and MinIO

The local HMS uses a dedicated PostgreSQL database persisted in a Docker volume.
The custom image pins and verifies the PostgreSQL JDBC and Hadoop S3A dependencies.
This is still a single-instance local pilot. Production needs managed credentials,
backups and restore tests, authentication, TLS, high availability, monitoring, and
an approved object-storage endpoint.

## Register External Delta Tables

Run the Spark pipelines first, then register all available tables:

```bash
docker compose run --rm trino-init
```

The CDC Raw Vault, quarantine, and audit tables are required. Legacy Raw Vault and
Gold tables are optional because they exist only after the legacy medallion DAG has
run. Re-running registration is safe; existing tables are detected and left intact.

## Query

```bash
docker compose exec trino trino --catalog lakehouse --schema cdc_raw_vault \
  --execute "SHOW TABLES"

docker compose exec trino trino --execute \
  "SELECT count(*) FROM lakehouse.cdc_raw_vault.hub_customer"

docker compose exec trino trino --execute \
  "SELECT count(*) FROM lakehouse.gold.fact_transactions"

docker compose exec trino trino --execute \
  'SELECT version, operation FROM lakehouse.cdc_raw_vault."hub_customer$history"'
```

Open the Trino UI at <http://localhost:8082/ui/>. The local environment intentionally
uses no authentication and `ALLOW_ALL` catalog access. It must not be exposed outside
the development machine.

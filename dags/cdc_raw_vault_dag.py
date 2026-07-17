"""Airflow DAG for incremental CDC Bronze to Data Vault Raw Vault loading."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


SPARK_PACKAGES = ",".join(
    [
        "io.delta:delta-spark_2.12:3.2.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "org.postgresql:postgresql:42.7.3",
    ]
)

SPARK_CONF = {
    "spark.jars.ivy": "/tmp/.ivy2",
    "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
    "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": (
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    ),
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.shuffle.partitions": "8",
    "spark.sql.constraintPropagation.enabled": "false",
}

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="cdc_raw_vault_incremental",
    description="Incrementally merge CDC Bronze business keys and relationships into Raw Vault.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["lakehouse", "cdc", "data-vault", "incremental"],
) as dag:

    def raw_vault_task(task_id: str, phase: str) -> SparkSubmitOperator:
        return SparkSubmitOperator(
            task_id=task_id,
            application="/opt/airflow/scripts/4_process_cdc_raw_vault.py",
            application_args=["--phase", phase],
            conn_id="spark_default",
            packages=SPARK_PACKAGES,
            conf=SPARK_CONF,
            driver_memory="2g",
            verbose=False,
            env_vars={
                "S3_ENDPOINT": "http://minio:9000",
                "S3_BUCKET": "lakehouse",
                "AWS_ACCESS_KEY_ID": "minioadmin",
                "AWS_SECRET_ACCESS_KEY": "minioadmin",
                "SPARK_MASTER_URL": "spark://spark-master:7077",
                "RAW_VAULT_BATCH_ID": "{{ run_id }}",
                "SOURCE_DB_JDBC_URL": (
                    "jdbc:postgresql://core-banking-source:5432/core_banking"
                ),
                "SOURCE_DB_USER": "core_banking",
                "SOURCE_DB_PASSWORD": "core_banking_local",
            },
        )

    validate_bronze = raw_vault_task("validate_bronze_and_quarantine", "validate")
    load_hubs_links = raw_vault_task("load_incremental_hubs_links", "core")
    load_satellites = raw_vault_task("load_satellite_and_delete_history", "satellites")
    reconcile = raw_vault_task("reconcile_source_bronze_silver", "reconcile")

    validate_bronze >> load_hubs_links >> load_satellites >> reconcile

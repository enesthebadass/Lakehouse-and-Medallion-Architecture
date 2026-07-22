"""Airflow DAG for incremental CDC Bronze to Data Vault Raw Vault loading."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from pipeline_control.control import (
    create_or_get_batch,
    fail_airflow_run,
    publish_batch,
    record_task_success,
    start_attempt,
)

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

MANIFEST_XCOM_TASK = "create_batch_manifest"
BATCH_ID_TEMPLATE = (
    "{{ ti.xcom_pull(task_ids='create_batch_manifest')['batch_id'] }}"
)
MANIFEST_URI_TEMPLATE = (
    "{{ ti.xcom_pull(task_ids='create_batch_manifest')['manifest_uri'] }}"
)
MANIFEST_SHA_TEMPLATE = (
    "{{ ti.xcom_pull(task_ids='create_batch_manifest')['manifest_sha256'] }}"
)
ATTEMPT_NUMBER_TEMPLATE = (
    "{{ ti.xcom_pull(task_ids='start_batch_attempt')['attempt_number'] }}"
)


def record_dag_failure(context) -> None:
    task_instance = context.get("task_instance")
    task_id = task_instance.task_id if task_instance else "unknown"
    exception = context.get("exception")
    fail_airflow_run(
        context["run_id"],
        f"DAG failed at task {task_id}: {exception or 'no exception detail'}",
    )


with DAG(
    dag_id="cdc_raw_vault_incremental",
    description="Incrementally merge CDC Bronze business keys and relationships into Raw Vault.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    on_failure_callback=record_dag_failure,
    tags=["lakehouse", "cdc", "data-vault", "incremental"],
) as dag:

    def raw_vault_task(task_id: str, phase: str) -> SparkSubmitOperator:
        return SparkSubmitOperator(
            task_id=task_id,
            application="/opt/airflow/scripts/4_process_cdc_raw_vault.py",
            application_args=[
                "--phase",
                phase,
                "--attempt-number",
                ATTEMPT_NUMBER_TEMPLATE,
                "--airflow-run-id",
                "{{ run_id }}",
                "--batch-id",
                BATCH_ID_TEMPLATE,
                "--manifest-uri",
                MANIFEST_URI_TEMPLATE,
                "--manifest-sha256",
                MANIFEST_SHA_TEMPLATE,
            ],
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
                "RAW_VAULT_BATCH_ID": BATCH_ID_TEMPLATE,
                "PYTHONPATH": "/opt/airflow",
                "SOURCE_DB_JDBC_URL": (
                    "jdbc:postgresql://core-banking-source:5432/core_banking"
                ),
                "SOURCE_DB_USER": "core_banking",
                "SOURCE_DB_PASSWORD": "core_banking_local",
            },
        )

    create_manifest = PythonOperator(
        task_id=MANIFEST_XCOM_TASK,
        python_callable=create_or_get_batch,
        op_kwargs={
            "batch_id": "{{ dag_run.conf.get('batch_id') or run_id }}",
            "airflow_run_id": "{{ run_id }}",
        },
    )
    begin_attempt = PythonOperator(
        task_id="start_batch_attempt",
        python_callable=start_attempt,
        op_kwargs={
            "batch_id": BATCH_ID_TEMPLATE,
            "airflow_run_id": "{{ run_id }}",
        },
    )

    validate_bronze = raw_vault_task("validate_bronze_and_quarantine", "validate")
    load_hubs_links = raw_vault_task("load_incremental_hubs_links", "core")
    load_satellites = raw_vault_task("load_satellite_and_delete_history", "satellites")
    reconcile = raw_vault_task("reconcile_source_bronze_silver", "reconcile")

    def evidence_task(task_id: str) -> PythonOperator:
        return PythonOperator(
            task_id=f"record_{task_id}_evidence",
            python_callable=record_task_success,
            op_kwargs={
                "batch_id": BATCH_ID_TEMPLATE,
                "airflow_run_id": "{{ run_id }}",
                "task_id": task_id,
                "manifest_uri": MANIFEST_URI_TEMPLATE,
                "manifest_sha256": MANIFEST_SHA_TEMPLATE,
            },
        )

    validate_evidence = evidence_task("validate_bronze_and_quarantine")
    core_evidence = evidence_task("load_incremental_hubs_links")
    satellite_evidence = evidence_task("load_satellite_and_delete_history")
    reconciliation_evidence = evidence_task("reconcile_source_bronze_silver")
    publish = PythonOperator(
        task_id="publish_raw_vault_batch",
        python_callable=publish_batch,
        op_kwargs={
            "batch_id": BATCH_ID_TEMPLATE,
            "airflow_run_id": "{{ run_id }}",
            "manifest_sha256": MANIFEST_SHA_TEMPLATE,
        },
    )

    (
        create_manifest
        >> begin_attempt
        >> validate_bronze
        >> validate_evidence
        >> load_hubs_links
        >> core_evidence
        >> load_satellites
        >> satellite_evidence
        >> reconcile
        >> reconciliation_evidence
        >> publish
    )

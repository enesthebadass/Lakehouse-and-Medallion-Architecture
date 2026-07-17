"""Airflow DAG for the lakehouse medallion demo."""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

SPARK_PACKAGES = ",".join(
    [
        "io.delta:delta-spark_2.12:3.2.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
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
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    "spark.sql.session.timeZone": "UTC",
    "spark.sql.shuffle.partitions": "8",
}

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="lakehouse_medallion_pipeline",
    description="Bronze to Silver to Gold medallion pipeline for banking demo data.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["lakehouse", "medallion", "delta", "banking"],
) as dag:
    generate_bronze = BashOperator(
        task_id="generate_bronze_dirty_data",
        bash_command="python /opt/airflow/scripts/1_generate_bronze.py",
    )

    process_silver = SparkSubmitOperator(
        task_id="process_silver_data_vault",
        application="/opt/airflow/scripts/2_process_silver.py",
        conn_id="spark_default",
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        verbose=False,
        env_vars={
            "S3_ENDPOINT": "http://minio:9000",
            "S3_BUCKET": "lakehouse",
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "SPARK_MASTER_URL": "spark://spark-master:7077",
        },
    )

    process_gold = SparkSubmitOperator(
        task_id="process_gold_star_schema",
        application="/opt/airflow/scripts/3_process_gold.py",
        conn_id="spark_default",
        packages=SPARK_PACKAGES,
        conf=SPARK_CONF,
        verbose=False,
        env_vars={
            "S3_ENDPOINT": "http://minio:9000",
            "S3_BUCKET": "lakehouse",
            "AWS_ACCESS_KEY_ID": "minioadmin",
            "AWS_SECRET_ACCESS_KEY": "minioadmin",
            "SPARK_MASTER_URL": "spark://spark-master:7077",
        },
    )

    generate_bronze >> process_silver >> process_gold

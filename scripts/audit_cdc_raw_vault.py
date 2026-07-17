"""Fail fast when the local CDC Raw Vault history contract is violated."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "lakehouse")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
RAW_VAULT_PATH = f"s3a://{S3_BUCKET}/silver/cdc_raw_vault"
AUDIT_PATH = f"s3a://{S3_BUCKET}/silver/audit"

SATELLITE_KEYS = {
    "sat_customer_details": "customer_hk",
    "sat_loan_application_details": "loan_application_hk",
    "sat_loan_details": "loan_hk",
    "sat_product_details": "product_hk",
    "sat_branch_details": "branch_hk",
    "sat_currency_details": "currency_hk",
}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("audit-cdc-raw-vault")
        .master(SPARK_MASTER_URL)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.constraintPropagation.enabled", "false")
        .getOrCreate()
    )


def read_delta(spark: SparkSession, base_path: str, table_name: str):
    return spark.read.format("delta").load(f"{base_path}/{table_name}")


def assert_event_identity_is_unique(table_name: str, table) -> int:
    total = table.count()
    distinct_events = table.select("source_event_id").distinct().count()
    if total != distinct_events:
        raise RuntimeError(
            f"{table_name} contains duplicate source_event_id values: "
            f"rows={total}, distinct_events={distinct_events}"
        )
    return total


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        history_versions: dict[str, int] = {}
        for table_name, parent_key in SATELLITE_KEYS.items():
            table = read_delta(spark, RAW_VAULT_PATH, table_name)
            row_count = assert_event_identity_is_unique(table_name, table)
            max_versions = (
                table.groupBy(parent_key)
                .count()
                .agg(F.max("count").alias("max_versions"))
                .first()["max_versions"]
            )
            history_versions[table_name] = int(max_versions or 0)
            print(
                f"{table_name}: rows={row_count:,}, "
                f"max_versions_per_parent={max_versions}"
            )

        for table_name in (
            "sat_customer_details",
            "sat_loan_application_details",
            "sat_loan_details",
        ):
            if history_versions[table_name] < 2:
                raise RuntimeError(f"No multi-version history found in {table_name}")

        tracking = read_delta(spark, RAW_VAULT_PATH, "sat_source_record_status")
        tracking_rows = assert_event_identity_is_unique(
            "sat_source_record_status", tracking
        )
        deleted_rows = tracking.filter(F.col("is_deleted")).count()
        if deleted_rows == 0:
            raise RuntimeError(
                "No CDC delete history found in sat_source_record_status"
            )
        print(
            f"sat_source_record_status: rows={tracking_rows:,}, "
            f"deleted_transitions={deleted_rows:,}"
        )

        reconciliation = read_delta(spark, AUDIT_PATH, "cdc_raw_vault_reconciliation")
        latest_batch = (
            reconciliation.orderBy(F.col("checked_at").desc())
            .select("load_batch_id")
            .first()["load_batch_id"]
        )
        latest_checks = reconciliation.filter(F.col("load_batch_id") == latest_batch)
        check_count = latest_checks.count()
        failed_count = latest_checks.filter(~F.col("passed")).count()
        if check_count == 0 or failed_count:
            raise RuntimeError(
                f"Latest reconciliation failed: checks={check_count}, "
                f"failed={failed_count}, batch={latest_batch}"
            )
        print(f"reconciliation: batch={latest_batch}, checks={check_count}, failed=0")
        print("CDC Raw Vault audit passed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

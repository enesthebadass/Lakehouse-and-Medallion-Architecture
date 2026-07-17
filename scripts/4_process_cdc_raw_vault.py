"""Validate and incrementally load CDC Bronze events into a Data Vault 2.0 Raw Vault."""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from delta.tables import DeltaTable
from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F


S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "lakehouse")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
BRONZE_CDC_PATH = os.getenv(
    "BRONZE_CDC_PATH", f"s3a://{S3_BUCKET}/bronze/cdc/source=core_banking"
)
CDC_RAW_VAULT_BASE_PATH = os.getenv(
    "CDC_RAW_VAULT_BASE_PATH", f"s3a://{S3_BUCKET}/silver/cdc_raw_vault"
)
SILVER_QUARANTINE_BASE_PATH = os.getenv(
    "SILVER_QUARANTINE_BASE_PATH", f"s3a://{S3_BUCKET}/silver/quarantine"
)
SILVER_AUDIT_BASE_PATH = os.getenv(
    "SILVER_AUDIT_BASE_PATH", f"s3a://{S3_BUCKET}/silver/audit"
)
SOURCE_DB_JDBC_URL = os.getenv(
    "SOURCE_DB_JDBC_URL", "jdbc:postgresql://core-banking-source:5432/core_banking"
)
SOURCE_DB_USER = os.getenv("SOURCE_DB_USER", "core_banking")
SOURCE_DB_PASSWORD = os.getenv("SOURCE_DB_PASSWORD", "core_banking_local")
LOAD_BATCH_ID = os.getenv(
    "RAW_VAULT_BATCH_ID",
    datetime.now(timezone.utc).strftime("cdc-rv-%Y%m%dT%H%M%S%fZ"),
)

HASH_ALGORITHM = "SHA-256"
HASH_DELIMITER = "||"
HASH_NULL_TOKEN = "^^"


@dataclass(frozen=True)
class HubMapping:
    table_name: str
    source_schema: str
    source_table: str
    namespace: str
    hash_key: str
    business_key: str
    source_column: str


@dataclass(frozen=True)
class SatelliteMapping:
    table_name: str
    source_schema: str
    source_table: str
    parent_namespace: str
    parent_hash_key: str
    business_key_column: str
    payload_columns: tuple[str, ...]


HUB_MAPPINGS = (
    HubMapping(
        "hub_customer",
        "mms",
        "customers",
        "CUSTOMER",
        "customer_hk",
        "customer_bk",
        "customer_no",
    ),
    HubMapping(
        "hub_loan_application",
        "krd",
        "loan_applications",
        "LOAN_APPLICATION",
        "loan_application_hk",
        "loan_application_bk",
        "application_no",
    ),
    HubMapping("hub_loan", "krd", "loans", "LOAN", "loan_hk", "loan_bk", "loan_no"),
    HubMapping(
        "hub_product",
        "prm",
        "products",
        "PRODUCT",
        "product_hk",
        "product_bk",
        "product_code",
    ),
    HubMapping(
        "hub_branch",
        "prm",
        "branches",
        "BRANCH",
        "branch_hk",
        "branch_bk",
        "branch_code",
    ),
    HubMapping(
        "hub_currency",
        "prm",
        "currencies",
        "CURRENCY",
        "currency_hk",
        "currency_bk",
        "currency_code",
    ),
)

SATELLITE_MAPPINGS = (
    SatelliteMapping(
        "sat_customer_details",
        "mms",
        "customers",
        "CUSTOMER",
        "customer_hk",
        "customer_no",
        (
            "customer_type",
            "first_name",
            "last_name",
            "legal_name",
            "national_id",
            "tax_id",
            "date_of_birth",
            "segment_code",
            "status_code",
            "home_branch_code",
            "created_at",
            "updated_at",
        ),
    ),
    SatelliteMapping(
        "sat_loan_application_details",
        "krd",
        "loan_applications",
        "LOAN_APPLICATION",
        "loan_application_hk",
        "application_no",
        (
            "requested_amount",
            "term_months",
            "status_code",
            "applied_at",
            "decision_at",
            "created_at",
            "updated_at",
        ),
    ),
    SatelliteMapping(
        "sat_loan_details",
        "krd",
        "loans",
        "LOAN",
        "loan_hk",
        "loan_no",
        (
            "principal_amount",
            "annual_interest_rate",
            "term_months",
            "status_code",
            "disbursed_at",
            "maturity_date",
            "created_at",
            "updated_at",
        ),
    ),
    SatelliteMapping(
        "sat_product_details",
        "prm",
        "products",
        "PRODUCT",
        "product_hk",
        "product_code",
        (
            "product_name",
            "product_type",
            "default_currency_code",
            "is_active",
            "created_at",
            "updated_at",
        ),
    ),
    SatelliteMapping(
        "sat_branch_details",
        "prm",
        "branches",
        "BRANCH",
        "branch_hk",
        "branch_code",
        ("branch_name", "city", "is_active", "created_at", "updated_at"),
    ),
    SatelliteMapping(
        "sat_currency_details",
        "prm",
        "currencies",
        "CURRENCY",
        "currency_hk",
        "currency_code",
        ("currency_name", "minor_unit", "is_active", "created_at", "updated_at"),
    ),
)

SOURCE_TABLES = (
    "mms.customers",
    "mms.customer_addresses",
    "mms.customer_contacts",
    "mms.customer_relations",
    "krd.loan_applications",
    "krd.loans",
    "krd.installments",
    "krd.collaterals",
    "prm.currencies",
    "prm.branches",
    "prm.products",
    "prm.status_codes",
    "prm.rate_parameters",
)

ALLOWED_OPERATIONS = ("r", "c", "u", "d", "tombstone")


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("lakehouse-cdc-incremental-raw-vault")
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
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.constraintPropagation.enabled", "false")
        .getOrCreate()
    )


def is_delta_table(spark: SparkSession, path: str) -> bool:
    return DeltaTable.isDeltaTable(spark, path)


def canonical_value(column: Column) -> Column:
    value = F.trim(column.cast("string"))
    return F.when(value.isNull() | (value == ""), F.lit(HASH_NULL_TOKEN)).otherwise(
        F.upper(value)
    )


def valid_business_key(column: Column) -> Column:
    value = F.trim(column.cast("string"))
    return value.isNotNull() & (value != "")


def hash_key(namespace: str, *columns: Column) -> Column:
    components = [F.lit(namespace), *(canonical_value(column) for column in columns)]
    return F.sha2(F.concat_ws(HASH_DELIMITER, *components), 256)


def read_cdc_events(spark: SparkSession) -> DataFrame:
    return (
        spark.read.option("recursiveFileLookup", "true")
        .option("pathGlobFilter", "*.json")
        .json(BRONZE_CDC_PATH)
    )


def source_records(
    events: DataFrame, source_schema: str, source_table: str
) -> DataFrame:
    return events.filter(
        (F.col("_metadata.source_schema") == source_schema)
        & (F.col("_metadata.source_table") == source_table)
        & F.col("_metadata.operation").isin("r", "c", "u")
        & F.col("value.after").isNotNull()
    )


def audit_expressions() -> list[Column]:
    return [
        F.to_timestamp(F.col("_metadata.ingested_at")).alias("load_datetime"),
        F.to_timestamp(F.col("_metadata.event_timestamp")).alias("effective_from"),
        F.concat_ws(
            ".",
            F.col("_metadata.source_system"),
            F.col("_metadata.source_schema"),
            F.col("_metadata.source_table"),
        ).alias("record_source"),
        F.col("_metadata.event_id").alias("source_event_id"),
        F.format_string(
            "%s:%d:%d",
            F.col("_metadata.kafka_topic"),
            F.col("_metadata.kafka_partition"),
            F.col("_metadata.kafka_offset"),
        ).alias("source_position"),
        F.col("_metadata.kafka_topic").alias("kafka_topic"),
        F.col("_metadata.kafka_partition").cast("int").alias("kafka_partition"),
        F.col("_metadata.kafka_offset").cast("long").alias("kafka_offset"),
        F.col("value.source.lsn").cast("long").alias("source_lsn"),
        F.col("_metadata.object_key").alias("bronze_object_key"),
        F.lit(LOAD_BATCH_ID).alias("load_batch_id"),
    ]


def keep_first_arrival(df: DataFrame, hash_key_column: str) -> DataFrame:
    order = Window.partitionBy(hash_key_column).orderBy(
        F.col("load_datetime").asc_nulls_last(),
        F.col("effective_from").asc_nulls_last(),
        F.col("kafka_topic"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
    )
    return (
        df.withColumn("_arrival_rank", F.row_number().over(order))
        .filter(F.col("_arrival_rank") == 1)
        .drop("_arrival_rank")
    )


def build_hub(events: DataFrame, mapping: HubMapping) -> DataFrame:
    business_key = F.col(f"value.after.{mapping.source_column}")
    candidates = source_records(
        events, mapping.source_schema, mapping.source_table
    ).filter(valid_business_key(business_key))
    hub = candidates.select(
        hash_key(mapping.namespace, business_key).alias(mapping.hash_key),
        canonical_value(business_key).alias(mapping.business_key),
        *audit_expressions(),
    )
    return keep_first_arrival(hub, mapping.hash_key)


def build_link_application_context(events: DataFrame) -> DataFrame:
    records = source_records(events, "krd", "loan_applications")
    required = (
        "application_no",
        "customer_no",
        "product_code",
        "branch_code",
        "currency_code",
    )
    for column_name in required:
        records = records.filter(
            valid_business_key(F.col(f"value.after.{column_name}"))
        )

    application = F.col("value.after.application_no")
    customer = F.col("value.after.customer_no")
    product = F.col("value.after.product_code")
    branch = F.col("value.after.branch_code")
    currency = F.col("value.after.currency_code")
    link = records.select(
        hash_key(
            "APPLICATION_CONTEXT", application, customer, product, branch, currency
        ).alias("application_context_hk"),
        hash_key("LOAN_APPLICATION", application).alias("loan_application_hk"),
        hash_key("CUSTOMER", customer).alias("customer_hk"),
        hash_key("PRODUCT", product).alias("product_hk"),
        hash_key("BRANCH", branch).alias("branch_hk"),
        hash_key("CURRENCY", currency).alias("currency_hk"),
        *audit_expressions(),
    )
    return keep_first_arrival(link, "application_context_hk")


def build_link_loan_context(events: DataFrame) -> DataFrame:
    records = source_records(events, "krd", "loans")
    required = (
        "loan_no",
        "application_id",
        "customer_no",
        "product_code",
        "branch_code",
        "currency_code",
    )
    for column_name in required:
        records = records.filter(
            valid_business_key(F.col(f"value.after.{column_name}"))
        )

    loan = F.col("value.after.loan_no")
    customer = F.col("value.after.customer_no")
    product = F.col("value.after.product_code")
    branch = F.col("value.after.branch_code")
    currency = F.col("value.after.currency_code")

    # loans carries the application technical ID but not application_no. Resolve the
    # durable application business key from application CDC history before hashing.
    applications = latest_source_state(
        events, "krd", "loan_applications", "application_id"
    ).select(
        F.col("value.after.application_id").alias("application_id"),
        F.col("value.after.application_no").alias("application_no"),
    )
    records = records.join(
        applications,
        F.col("value.after.application_id") == F.col("application_id"),
        "inner",
    ).filter(valid_business_key(F.col("application_no")))

    application = F.col("application_no")
    link = records.select(
        hash_key(
            "LOAN_CONTEXT", loan, application, customer, product, branch, currency
        ).alias("loan_context_hk"),
        hash_key("LOAN", loan).alias("loan_hk"),
        hash_key("LOAN_APPLICATION", application).alias("loan_application_hk"),
        hash_key("CUSTOMER", customer).alias("customer_hk"),
        hash_key("PRODUCT", product).alias("product_hk"),
        hash_key("BRANCH", branch).alias("branch_hk"),
        hash_key("CURRENCY", currency).alias("currency_hk"),
        *audit_expressions(),
    )
    return keep_first_arrival(link, "loan_context_hk")


def latest_source_state(
    events: DataFrame, source_schema: str, source_table: str, source_primary_key: str
) -> DataFrame:
    records = source_records(events, source_schema, source_table)
    order = Window.partitionBy(F.col(f"value.after.{source_primary_key}")).orderBy(
        F.col("_metadata.event_timestamp").desc_nulls_last(),
        F.col("_metadata.kafka_offset").desc(),
    )
    return (
        records.withColumn("_state_rank", F.row_number().over(order))
        .filter(F.col("_state_rank") == 1)
        .drop("_state_rank")
    )


def build_satellite(events: DataFrame, mapping: SatelliteMapping) -> DataFrame:
    business_key = F.col(f"value.after.{mapping.business_key_column}")
    records = source_records(
        events, mapping.source_schema, mapping.source_table
    ).filter(valid_business_key(business_key))
    payload = [F.col(f"value.after.{name}") for name in mapping.payload_columns]
    staged = records.select(
        hash_key(mapping.parent_namespace, business_key).alias(mapping.parent_hash_key),
        hash_key(f"{mapping.table_name.upper()}_HASHDIFF", *payload).alias("hashdiff"),
        F.col("_metadata.operation").alias("record_operation"),
        *(
            column.cast("string").alias(name)
            for column, name in zip(payload, mapping.payload_columns)
        ),
        *audit_expressions(),
    )
    order = Window.partitionBy(mapping.parent_hash_key).orderBy(
        F.col("effective_from").asc_nulls_last(),
        F.col("source_lsn").asc_nulls_last(),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
    )
    return (
        staged.withColumn("_previous_hashdiff", F.lag("hashdiff").over(order))
        .filter(
            F.col("_previous_hashdiff").isNull()
            | (F.col("hashdiff") != F.col("_previous_hashdiff"))
        )
        .drop("_previous_hashdiff")
    )


def build_source_record_tracking(events: DataFrame) -> DataFrame:
    records = events.filter(
        F.col("_metadata.operation").isin("r", "c", "u", "d") & F.col("key").isNotNull()
    )
    source_key = F.to_json(F.col("key"))
    staged = records.select(
        hash_key(
            "SOURCE_RECORD",
            F.col("_metadata.source_schema"),
            F.col("_metadata.source_table"),
            source_key,
        ).alias("source_record_hk"),
        source_key.alias("source_record_key"),
        F.col("_metadata.source_schema").alias("source_schema"),
        F.col("_metadata.source_table").alias("source_table"),
        F.when(F.col("_metadata.operation") == "d", F.lit("DELETED"))
        .otherwise(F.lit("ACTIVE"))
        .alias("record_status"),
        (F.col("_metadata.operation") == "d").alias("is_deleted"),
        F.col("_metadata.operation").alias("record_operation"),
        *audit_expressions(),
    )
    order = Window.partitionBy("source_record_hk").orderBy(
        F.col("effective_from").asc_nulls_last(),
        F.col("source_lsn").asc_nulls_last(),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
    )
    return (
        staged.withColumn("_previous_status", F.lag("record_status").over(order))
        .filter(
            F.col("_previous_status").isNull()
            | (F.col("record_status") != F.col("_previous_status"))
        )
        .drop("_previous_status")
    )


def quarantine_projection(df: DataFrame, reason: Column) -> DataFrame:
    raw_payload = F.to_json(F.struct(F.col("key"), F.col("value")))
    event_identity = F.coalesce(
        F.col("_metadata.event_id"),
        F.col("_metadata.payload_sha256"),
        F.col("_metadata.object_key"),
        F.sha2(raw_payload, 256),
    )
    return df.select(
        F.sha2(F.concat_ws(HASH_DELIMITER, event_identity, reason), 256).alias(
            "quarantine_hk"
        ),
        reason.alias("reason_code"),
        F.col("_metadata.event_id").alias("source_event_id"),
        F.col("_metadata.source_schema").alias("source_schema"),
        F.col("_metadata.source_table").alias("source_table"),
        F.col("_metadata.operation").alias("record_operation"),
        F.col("_metadata.object_key").alias("bronze_object_key"),
        raw_payload.alias("raw_payload"),
        F.current_timestamp().alias("detected_at"),
        F.lit(LOAD_BATCH_ID).alias("load_batch_id"),
    )


def build_quarantine(events: DataFrame) -> DataFrame:
    contract_reason = (
        F.when(F.col("_metadata.event_id").isNull(), F.lit("MISSING_EVENT_ID"))
        .when(F.col("_metadata.source_schema").isNull(), F.lit("MISSING_SOURCE_SCHEMA"))
        .when(F.col("_metadata.source_table").isNull(), F.lit("MISSING_SOURCE_TABLE"))
        .when(F.col("_metadata.operation").isNull(), F.lit("MISSING_OPERATION"))
        .when(
            ~F.col("_metadata.operation").isin(*ALLOWED_OPERATIONS),
            F.lit("UNSUPPORTED_OPERATION"),
        )
        .when(
            F.col("_metadata.operation").isin("r", "c", "u")
            & F.col("value.after").isNull(),
            F.lit("MISSING_AFTER_IMAGE"),
        )
    )
    invalid_contract = events.withColumn("_reason", contract_reason).filter(
        F.col("_reason").isNotNull()
    )
    quarantine = quarantine_projection(invalid_contract, F.col("_reason"))

    for mapping in HUB_MAPPINGS:
        business_key = F.col(f"value.after.{mapping.source_column}")
        invalid_key = source_records(
            events, mapping.source_schema, mapping.source_table
        ).filter(~valid_business_key(business_key))
        mapped = quarantine_projection(
            invalid_key,
            F.lit(f"MISSING_BUSINESS_KEY_{mapping.namespace}"),
        )
        quarantine = quarantine.unionByName(mapped)

    return quarantine.dropDuplicates(["quarantine_hk"])


def current_bronze_counts(events: DataFrame) -> dict[str, int]:
    records = (
        events.filter(
            F.col("_metadata.operation").isin("r", "c", "u", "d")
            & F.col("key").isNotNull()
        )
        .withColumn(
            "object_name",
            F.concat_ws(
                ".",
                F.col("_metadata.source_schema"),
                F.col("_metadata.source_table"),
            ),
        )
        .withColumn("_source_key", F.to_json(F.col("key")))
    )
    order = Window.partitionBy("object_name", "_source_key").orderBy(
        F.col("_metadata.event_timestamp").desc_nulls_last(),
        F.col("value.source.lsn").desc_nulls_last(),
        F.col("_metadata.kafka_offset").desc(),
    )
    latest = (
        records.withColumn("_state_rank", F.row_number().over(order))
        .filter((F.col("_state_rank") == 1) & (F.col("_metadata.operation") != "d"))
        .groupBy("object_name")
        .count()
    )
    return {row["object_name"]: row["count"] for row in latest.collect()}


def read_source_counts(spark: SparkSession) -> dict[str, int]:
    count_query = " UNION ALL ".join(
        f"SELECT '{table}' AS object_name, COUNT(*)::BIGINT AS source_count FROM {table}"
        for table in SOURCE_TABLES
    )
    counts = (
        spark.read.format("jdbc")
        .option("url", SOURCE_DB_JDBC_URL)
        .option("query", count_query)
        .option("user", SOURCE_DB_USER)
        .option("password", SOURCE_DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
    )
    return {row["object_name"]: row["source_count"] for row in counts.collect()}


def quoted_columns(columns: Iterable[str]) -> str:
    return ", ".join(f"`{column}`" for column in columns)


def source_columns(columns: Iterable[str]) -> str:
    return ", ".join(f"source.`{column}`" for column in columns)


def merge_insert_only(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    hash_key_column: str,
    base_path: str = CDC_RAW_VAULT_BASE_PATH,
) -> tuple[int, int]:
    path = f"{base_path}/{table_name}"
    candidate_count = df.count()
    if not is_delta_table(spark, path):
        df.write.format("delta").mode("errorifexists").save(path)
        return candidate_count, candidate_count

    temporary_view = f"staged_{table_name}"
    df.createOrReplaceTempView(temporary_view)
    columns = df.columns
    before_count = spark.read.format("delta").load(path).count()
    spark.sql(
        f"""
        MERGE INTO delta.`{path}` AS target
        USING {temporary_view} AS source
          ON target.`{hash_key_column}` = source.`{hash_key_column}`
        WHEN NOT MATCHED THEN INSERT ({quoted_columns(columns)})
          VALUES ({source_columns(columns)})
        """
    )
    after_count = spark.read.format("delta").load(path).count()
    return candidate_count, after_count - before_count


def assert_unique_key(df: DataFrame, key_column: str, table_name: str) -> None:
    duplicate_exists = (
        df.groupBy(key_column).count().filter(F.col("count") > 1).limit(1).count() > 0
    )
    if duplicate_exists:
        raise RuntimeError(f"Duplicate {key_column} detected in {table_name}")


def load_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    hash_key_column: str,
    base_path: str = CDC_RAW_VAULT_BASE_PATH,
) -> None:
    assert_unique_key(df, hash_key_column, table_name)
    candidates, inserted = merge_insert_only(
        spark, df, table_name, hash_key_column, base_path
    )
    total = spark.read.format("delta").load(f"{base_path}/{table_name}").count()
    print(
        f"{table_name}: candidates={candidates:,}, inserted={inserted:,}, total={total:,}, "
        f"batch={LOAD_BATCH_ID}"
    )


def run_validate(spark: SparkSession, events: DataFrame) -> None:
    event_count = events.count()
    if event_count == 0:
        raise RuntimeError(f"No Bronze CDC records found at {BRONZE_CDC_PATH}")
    quarantine = build_quarantine(events)
    load_table(
        spark,
        quarantine,
        "cdc_raw_vault_events",
        "quarantine_hk",
        SILVER_QUARANTINE_BASE_PATH,
    )
    print(f"Validated {event_count:,} Bronze CDC records.")


def run_core(spark: SparkSession, events: DataFrame) -> None:
    for mapping in HUB_MAPPINGS:
        load_table(
            spark,
            build_hub(events, mapping),
            mapping.table_name,
            mapping.hash_key,
        )

    load_table(
        spark,
        build_link_application_context(events),
        "link_application_context",
        "application_context_hk",
    )
    load_table(
        spark,
        build_link_loan_context(events),
        "link_loan_context",
        "loan_context_hk",
    )


def run_satellites(spark: SparkSession, events: DataFrame) -> None:
    for mapping in SATELLITE_MAPPINGS:
        staged = build_satellite(events, mapping).localCheckpoint(eager=True)
        load_table(
            spark,
            staged,
            mapping.table_name,
            "source_event_id",
        )
        staged.unpersist()
    tracking = build_source_record_tracking(events).localCheckpoint(eager=True)
    load_table(
        spark,
        tracking,
        "sat_source_record_status",
        "source_event_id",
    )
    tracking.unpersist()


def delta_count(spark: SparkSession, base_path: str, table_name: str) -> int:
    path = f"{base_path}/{table_name}"
    if not is_delta_table(spark, path):
        return 0
    return spark.read.format("delta").load(path).count()


def reconciliation_row(
    check_scope: str, object_name: str, expected_count: int, actual_count: int
) -> dict[str, object]:
    return {
        "check_scope": check_scope,
        "object_name": object_name,
        "expected_count": int(expected_count),
        "actual_count": int(actual_count),
        "difference": int(actual_count - expected_count),
        "passed": actual_count == expected_count,
    }


def reconciliation_report(
    spark: SparkSession, rows: list[dict[str, object]]
) -> DataFrame:
    statements = []
    for row in rows:
        passed = "TRUE" if row["passed"] else "FALSE"
        statements.append(
            "SELECT "
            f"'{row['check_scope']}' AS check_scope, "
            f"'{row['object_name']}' AS object_name, "
            f"CAST({row['expected_count']} AS BIGINT) AS expected_count, "
            f"CAST({row['actual_count']} AS BIGINT) AS actual_count, "
            f"CAST({row['difference']} AS BIGINT) AS difference, "
            f"{passed} AS passed"
        )
    return spark.sql(" UNION ALL ".join(statements))


def run_reconciliation(spark: SparkSession, events: DataFrame) -> None:
    rows: list[dict[str, object]] = []
    source_counts = read_source_counts(spark)
    bronze_counts = current_bronze_counts(events)
    for table_name in SOURCE_TABLES:
        rows.append(
            reconciliation_row(
                "SOURCE_TO_BRONZE_CURRENT_STATE",
                table_name,
                source_counts.get(table_name, 0),
                bronze_counts.get(table_name, 0),
            )
        )

    for mapping in HUB_MAPPINGS:
        rows.append(
            reconciliation_row(
                "BRONZE_TO_HUB_HISTORICAL_KEYS",
                mapping.table_name,
                build_hub(events, mapping).count(),
                delta_count(spark, CDC_RAW_VAULT_BASE_PATH, mapping.table_name),
            )
        )

    link_mappings = (
        (
            "link_application_context",
            build_link_application_context(events),
        ),
        ("link_loan_context", build_link_loan_context(events)),
    )
    for table_name, staged in link_mappings:
        rows.append(
            reconciliation_row(
                "BRONZE_TO_LINK_HISTORICAL_KEYS",
                table_name,
                staged.count(),
                delta_count(spark, CDC_RAW_VAULT_BASE_PATH, table_name),
            )
        )

    for mapping in SATELLITE_MAPPINGS:
        rows.append(
            reconciliation_row(
                "BRONZE_TO_SATELLITE_HISTORY",
                mapping.table_name,
                build_satellite(events, mapping).count(),
                delta_count(spark, CDC_RAW_VAULT_BASE_PATH, mapping.table_name),
            )
        )

    tracking = build_source_record_tracking(events)
    rows.append(
        reconciliation_row(
            "BRONZE_TO_RECORD_TRACKING_HISTORY",
            "sat_source_record_status",
            tracking.count(),
            delta_count(spark, CDC_RAW_VAULT_BASE_PATH, "sat_source_record_status"),
        )
    )
    quarantine = build_quarantine(events)
    rows.append(
        reconciliation_row(
            "BRONZE_TO_QUARANTINE",
            "cdc_raw_vault_events",
            quarantine.count(),
            delta_count(spark, SILVER_QUARANTINE_BASE_PATH, "cdc_raw_vault_events"),
        )
    )

    # Build the small report in Catalyst SQL. Airflow and Spark worker images can use
    # different Python minors; avoiding a PythonRDD keeps this distributed job JVM-only.
    report = (
        reconciliation_report(spark, rows)
        .withColumn("load_batch_id", F.lit(LOAD_BATCH_ID))
        .withColumn("checked_at", F.current_timestamp())
        .withColumn(
            "reconciliation_hk",
            hash_key(
                "RECONCILIATION",
                F.col("load_batch_id"),
                F.col("check_scope"),
                F.col("object_name"),
            ),
        )
        .select(
            "reconciliation_hk",
            "load_batch_id",
            "checked_at",
            "check_scope",
            "object_name",
            "expected_count",
            "actual_count",
            "difference",
            "passed",
        )
    )
    load_table(
        spark,
        report,
        "cdc_raw_vault_reconciliation",
        "reconciliation_hk",
        SILVER_AUDIT_BASE_PATH,
    )
    failed = [row for row in rows if not row["passed"]]
    if failed:
        raise RuntimeError(f"Raw Vault reconciliation failed: {failed}")
    print(f"All {len(rows)} reconciliation checks passed.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--phase",
        choices=("validate", "core", "satellites", "reconcile", "all"),
        default="all",
        help="Run one orchestration phase or the complete local pipeline.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        events = read_cdc_events(spark).persist(StorageLevel.DISK_ONLY)
        event_count = events.count()
        print(
            f"Read {event_count:,} Bronze CDC records from {BRONZE_CDC_PATH}; "
            f"hash_standard={HASH_ALGORITHM}/UPPER-TRIM/{HASH_DELIMITER}/{HASH_NULL_TOKEN}"
        )
        if args.phase in ("validate", "all"):
            run_validate(spark, events)
        if args.phase in ("core", "all"):
            run_core(spark, events)
        if args.phase in ("satellites", "all"):
            run_satellites(spark, events)
        if args.phase in ("reconcile", "all"):
            run_reconciliation(spark, events)
        events.unpersist()
    finally:
        spark.stop()

    print(f"CDC Raw Vault phase {args.phase!r} completed.")


if __name__ == "__main__":
    main()

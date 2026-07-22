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

from pipeline_control.audit import (
    audit_evidence_uri,
    build_audit_payload,
    write_audit_evidence,
)
from pipeline_control.manifest import (
    BOUNDED_READER_MODE,
    LEGACY_READER_MODE,
    parse_s3_uri,
    read_manifest,
    s3_client,
)

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


def envelope_field(image: str, field_name: str) -> Column:
    """Read a Debezium image field without depending on inferred struct members."""
    return F.get_json_object(F.to_json(F.col("value")), f"$.{image}.{field_name}")


def hash_key(namespace: str, *columns: Column) -> Column:
    components = [F.lit(namespace), *(canonical_value(column) for column in columns)]
    return F.sha2(F.concat_ws(HASH_DELIMITER, *components), 256)


def manifest_snapshot_condition(manifest: dict[str, object]) -> Column:
    condition = None
    for bound in manifest["partitions"]:
        partition_condition = (
            (F.col("_metadata.kafka_topic") == F.lit(bound["topic"]))
            & (F.col("_metadata.kafka_partition") == F.lit(bound["partition"]))
            & (F.col("_metadata.kafka_offset") <= F.lit(bound["watermark_high"]))
        )
        condition = (
            partition_condition
            if condition is None
            else condition | partition_condition
        )
    if condition is None:
        raise RuntimeError("Manifest has no partition bounds")
    return condition


def bounded_object_paths(manifest: dict[str, object], bucket: str) -> list[str]:
    if manifest.get("reader_mode") != BOUNDED_READER_MODE:
        raise RuntimeError("Exact object paths require a bounded manifest")
    return [f"s3a://{bucket}/{item['object_key']}" for item in manifest["objects"]]


def read_cdc_events(
    spark: SparkSession,
    manifest: dict[str, object] | None = None,
    manifest_bucket: str = S3_BUCKET,
) -> DataFrame:
    if manifest is not None and manifest.get("reader_mode") == BOUNDED_READER_MODE:
        paths = bounded_object_paths(manifest, manifest_bucket)
        if not paths:
            raise RuntimeError("A no-op bounded manifest must be skipped before Spark read")
        return spark.read.json(paths)

    events = spark.read.option("recursiveFileLookup", "true").option(
        "pathGlobFilter", "*.json"
    ).json(BRONZE_CDC_PATH)
    if manifest is not None:
        events = events.filter(manifest_snapshot_condition(manifest))
    return events


def source_records(
    events: DataFrame, source_schema: str, source_table: str
) -> DataFrame:
    return events.filter(
        (F.col("_metadata.source_schema") == source_schema)
        & (F.col("_metadata.source_table") == source_table)
        & F.col("_metadata.operation").isin("r", "c", "u")
        & F.col("value.after").isNotNull()
    )


def source_change_records(
    events: DataFrame, source_schema: str, source_table: str
) -> DataFrame:
    return events.filter(
        (F.col("_metadata.source_schema") == source_schema)
        & (F.col("_metadata.source_table") == source_table)
        & F.col("_metadata.operation").isin("r", "c", "u", "d")
    )


def change_field(field_name: str) -> Column:
    """Return the current image, or the before image carried by a delete."""
    return F.coalesce(
        envelope_field("after", field_name),
        envelope_field("before", field_name),
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


def audit_expressions_column_names() -> tuple[str, ...]:
    return (
        "load_datetime",
        "effective_from",
        "record_source",
        "source_event_id",
        "source_position",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "source_lsn",
        "bronze_object_key",
        "load_batch_id",
    )


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
    business_key = envelope_field("after", mapping.source_column)
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
        records = records.filter(valid_business_key(envelope_field("after", column_name)))

    application = envelope_field("after", "application_no")
    customer = envelope_field("after", "customer_no")
    product = envelope_field("after", "product_code")
    branch = envelope_field("after", "branch_code")
    currency = envelope_field("after", "currency_code")
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


def persisted_loan_application_lookup(spark: SparkSession) -> DataFrame:
    link_path = f"{CDC_RAW_VAULT_BASE_PATH}/link_loan_context"
    application_path = f"{CDC_RAW_VAULT_BASE_PATH}/hub_loan_application"
    if not is_delta_table(spark, link_path) or not is_delta_table(spark, application_path):
        return spark.sql(
            "SELECT CAST(NULL AS STRING) AS _persisted_loan_hk, "
            "CAST(NULL AS STRING) AS _persisted_application_no WHERE FALSE"
        )

    order = Window.partitionBy("loan_hk").orderBy(
        F.col("source_lsn").desc_nulls_last(),
        F.col("effective_from").desc_nulls_last(),
        F.col("kafka_partition").desc(),
        F.col("kafka_offset").desc(),
    )
    links = (
        spark.read.format("delta")
        .load(link_path)
        .withColumn("_context_rank", F.row_number().over(order))
        .filter(F.col("_context_rank") == 1)
    )
    applications = spark.read.format("delta").load(application_path)
    return links.join(applications, on="loan_application_hk", how="inner").select(
        F.col("loan_hk").alias("_persisted_loan_hk"),
        F.col("loan_application_bk").alias("_persisted_application_no"),
    )


def resolved_loan_context_records(
    spark: SparkSession, events: DataFrame, *, include_deletes: bool
) -> DataFrame:
    records = (
        source_change_records(events, "krd", "loans")
        if include_deletes
        else source_records(events, "krd", "loans")
    )
    field = change_field if include_deletes else lambda name: envelope_field("after", name)
    required = (
        "loan_no",
        "application_id",
        "customer_no",
        "product_code",
        "branch_code",
        "currency_code",
    )
    for column_name in required:
        records = records.filter(valid_business_key(field(column_name)))

    staged = records.select(
        field("loan_no").alias("_loan_no"),
        field("application_id").alias("_application_id"),
        field("customer_no").alias("_customer_no"),
        field("product_code").alias("_product_code"),
        field("branch_code").alias("_branch_code"),
        field("currency_code").alias("_currency_code"),
        F.col("_metadata.operation").alias("record_operation"),
        *audit_expressions(),
    ).withColumn("_loan_hk", hash_key("LOAN", F.col("_loan_no")))

    application_events = source_change_records(events, "krd", "loan_applications")
    application_order = Window.partitionBy(change_field("application_id")).orderBy(
        F.col("value.source.lsn").cast("long").desc_nulls_last(),
        F.col("_metadata.kafka_partition").desc(),
        F.col("_metadata.kafka_offset").desc(),
    )
    applications = (
        application_events.filter(valid_business_key(change_field("application_id")))
        .filter(valid_business_key(change_field("application_no")))
        .withColumn("_application_rank", F.row_number().over(application_order))
        .filter(F.col("_application_rank") == 1)
        .select(
            change_field("application_id").alias("_batch_application_id"),
            change_field("application_no").alias("_batch_application_no"),
        )
    )
    resolved = (
        staged.join(
            applications,
            staged._application_id == applications._batch_application_id,
            "left",
        )
        .join(
            persisted_loan_application_lookup(spark),
            staged._loan_hk == F.col("_persisted_loan_hk"),
            "left",
        )
        .withColumn(
            "_application_no",
            F.coalesce(F.col("_batch_application_no"), F.col("_persisted_application_no")),
        )
        .filter(valid_business_key(F.col("_application_no")))
    )
    return resolved


def build_link_loan_context(spark: SparkSession, events: DataFrame) -> DataFrame:
    records = resolved_loan_context_records(spark, events, include_deletes=False)
    link = records.select(
        hash_key(
            "LOAN_CONTEXT",
            F.col("_loan_no"),
            F.col("_application_no"),
            F.col("_customer_no"),
            F.col("_product_code"),
            F.col("_branch_code"),
            F.col("_currency_code"),
        ).alias("loan_context_hk"),
        F.col("_loan_hk").alias("loan_hk"),
        hash_key("LOAN_APPLICATION", F.col("_application_no")).alias(
            "loan_application_hk"
        ),
        hash_key("CUSTOMER", F.col("_customer_no")).alias("customer_hk"),
        hash_key("PRODUCT", F.col("_product_code")).alias("product_hk"),
        hash_key("BRANCH", F.col("_branch_code")).alias("branch_hk"),
        hash_key("CURRENCY", F.col("_currency_code")).alias("currency_hk"),
        *(
            F.col(name)
            for name in audit_expressions_column_names()
        ),
    )
    return keep_first_arrival(link, "loan_context_hk")


def latest_source_state(
    events: DataFrame, source_schema: str, source_table: str, source_primary_key: str
) -> DataFrame:
    records = source_records(events, source_schema, source_table)
    order = Window.partitionBy(envelope_field("after", source_primary_key)).orderBy(
        F.col("_metadata.event_timestamp").desc_nulls_last(),
        F.col("_metadata.kafka_offset").desc(),
    )
    return (
        records.withColumn("_state_rank", F.row_number().over(order))
        .filter(F.col("_state_rank") == 1)
        .drop("_state_rank")
    )


def build_satellite(events: DataFrame, mapping: SatelliteMapping) -> DataFrame:
    business_key = envelope_field("after", mapping.business_key_column)
    records = source_records(
        events, mapping.source_schema, mapping.source_table
    ).filter(valid_business_key(business_key))
    payload = [envelope_field("after", name) for name in mapping.payload_columns]
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
        F.col("source_lsn").asc_nulls_last(),
        F.col("effective_from").asc_nulls_last(),
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
        F.col("source_lsn").asc_nulls_last(),
        F.col("effective_from").asc_nulls_last(),
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


def keep_state_changes(
    staged: DataFrame, entity_key: str, state_column: str
) -> DataFrame:
    order = Window.partitionBy(entity_key).orderBy(
        F.col("source_lsn").asc_nulls_last(),
        F.col("effective_from").asc_nulls_last(),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
    )
    return (
        staged.withColumn("_previous_state", F.lag(state_column).over(order))
        .filter(
            F.col("_previous_state").isNull()
            | ~F.col(state_column).eqNullSafe(F.col("_previous_state"))
        )
        .drop("_previous_state")
    )


def build_entity_record_status(events: DataFrame) -> DataFrame:
    combined: DataFrame | None = None
    for mapping in HUB_MAPPINGS:
        business_key = change_field(mapping.source_column)
        records = source_change_records(
            events, mapping.source_schema, mapping.source_table
        ).filter(valid_business_key(business_key))
        entity = records.select(
            F.lit(mapping.namespace).alias("entity_type"),
            hash_key(mapping.namespace, business_key).alias("entity_hk"),
            canonical_value(business_key).alias("entity_business_key"),
            F.when(F.col("_metadata.operation") == "d", F.lit("DELETED"))
            .otherwise(F.lit("ACTIVE"))
            .alias("record_status"),
            (F.col("_metadata.operation") == "d").alias("is_deleted"),
            F.col("_metadata.operation").alias("record_operation"),
            *audit_expressions(),
        )
        combined = entity if combined is None else combined.unionByName(entity)
    if combined is None:
        raise RuntimeError("Entity status mappings must not be empty")
    return keep_state_changes(combined, "entity_hk", "record_status")


def build_application_context_effectivity(events: DataFrame) -> DataFrame:
    records = source_change_records(events, "krd", "loan_applications")
    required = (
        "application_no",
        "customer_no",
        "product_code",
        "branch_code",
        "currency_code",
    )
    for column_name in required:
        records = records.filter(valid_business_key(change_field(column_name)))

    application = change_field("application_no")
    customer = change_field("customer_no")
    product = change_field("product_code")
    branch = change_field("branch_code")
    currency = change_field("currency_code")
    status = F.when(F.col("_metadata.operation") == "d", F.lit("DELETED")).otherwise(
        F.lit("ACTIVE")
    )
    context_hk = hash_key(
        "APPLICATION_CONTEXT", application, customer, product, branch, currency
    )
    staged = records.select(
        hash_key("LOAN_APPLICATION", application).alias("loan_application_hk"),
        context_hk.alias("application_context_hk"),
        hash_key("CUSTOMER", customer).alias("customer_hk"),
        hash_key("PRODUCT", product).alias("product_hk"),
        hash_key("BRANCH", branch).alias("branch_hk"),
        hash_key("CURRENCY", currency).alias("currency_hk"),
        status.alias("record_status"),
        (F.col("_metadata.operation") == "d").alias("is_deleted"),
        hash_key("APPLICATION_EFFECTIVITY", context_hk, status).alias(
            "effectivity_hashdiff"
        ),
        F.col("_metadata.operation").alias("record_operation"),
        *audit_expressions(),
    )
    return keep_state_changes(
        staged, "loan_application_hk", "effectivity_hashdiff"
    )


def build_loan_context_effectivity(
    spark: SparkSession, events: DataFrame
) -> DataFrame:
    records = resolved_loan_context_records(spark, events, include_deletes=True)
    status = F.when(F.col("record_operation") == "d", F.lit("DELETED")).otherwise(
        F.lit("ACTIVE")
    )
    context_hk = hash_key(
        "LOAN_CONTEXT",
        F.col("_loan_no"),
        F.col("_application_no"),
        F.col("_customer_no"),
        F.col("_product_code"),
        F.col("_branch_code"),
        F.col("_currency_code"),
    )
    staged = records.select(
        F.col("_loan_hk").alias("loan_hk"),
        context_hk.alias("loan_context_hk"),
        hash_key("LOAN_APPLICATION", F.col("_application_no")).alias(
            "loan_application_hk"
        ),
        hash_key("CUSTOMER", F.col("_customer_no")).alias("customer_hk"),
        hash_key("PRODUCT", F.col("_product_code")).alias("product_hk"),
        hash_key("BRANCH", F.col("_branch_code")).alias("branch_hk"),
        hash_key("CURRENCY", F.col("_currency_code")).alias("currency_hk"),
        status.alias("record_status"),
        (F.col("record_operation") == "d").alias("is_deleted"),
        hash_key("LOAN_EFFECTIVITY", context_hk, status).alias(
            "effectivity_hashdiff"
        ),
        F.col("record_operation"),
        *(F.col(name) for name in audit_expressions_column_names()),
    )
    return keep_state_changes(staged, "loan_hk", "effectivity_hashdiff")


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
        business_key = envelope_field("after", mapping.source_column)
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
    if candidate_count == 0:
        return 0, 0
    if not is_delta_table(spark, path):
        df.write.format("delta").mode("errorifexists").save(path)
        return candidate_count, candidate_count

    temporary_view = f"staged_{table_name}"
    df.createOrReplaceTempView(temporary_view)
    columns = df.columns
    spark.sql(
        f"""
        MERGE INTO delta.`{path}` AS target
        USING {temporary_view} AS source
          ON target.`{hash_key_column}` = source.`{hash_key_column}`
        WHEN NOT MATCHED THEN INSERT ({quoted_columns(columns)})
          VALUES ({source_columns(columns)})
        """
    )
    metrics = DeltaTable.forPath(spark, path).history(1).select("operationMetrics").first()
    operation_metrics = metrics["operationMetrics"] if metrics else {}
    inserted = int((operation_metrics or {}).get("numTargetRowsInserted", 0))
    return candidate_count, inserted


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
    print(
        f"{table_name}: candidates={candidates:,}, inserted={inserted:,}, "
        f"batch={LOAD_BATCH_ID}"
    )


def filter_against_persisted_state(
    spark: SparkSession,
    staged: DataFrame,
    *,
    table_name: str,
    entity_key: str,
    state_column: str,
) -> DataFrame:
    """Compare a bounded batch's first state with the preceding persisted state."""
    path = f"{CDC_RAW_VAULT_BASE_PATH}/{table_name}"
    if not is_delta_table(spark, path):
        return staged

    target = spark.read.format("delta").load(path).filter(
        F.col("load_batch_id") != F.lit(LOAD_BATCH_ID)
    )
    latest_order = Window.partitionBy(entity_key).orderBy(
        F.col("source_lsn").desc_nulls_last(),
        F.col("effective_from").desc_nulls_last(),
        F.col("kafka_partition").desc(),
        F.col("kafka_offset").desc(),
    )
    previous = (
        target.withColumn("_persisted_rank", F.row_number().over(latest_order))
        .filter(F.col("_persisted_rank") == 1)
        .select(
            entity_key,
            F.col(state_column).alias("_persisted_state"),
            F.col("source_lsn").alias("_persisted_lsn"),
            F.col("kafka_topic").alias("_persisted_topic"),
            F.col("kafka_partition").alias("_persisted_partition"),
            F.col("kafka_offset").alias("_persisted_offset"),
        )
    )
    batch_order = Window.partitionBy(entity_key).orderBy(
        F.col("source_lsn").asc_nulls_last(),
        F.col("effective_from").asc_nulls_last(),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
    )
    compared = staged.withColumn("_batch_rank", F.row_number().over(batch_order)).join(
        previous, on=entity_key, how="left"
    )
    same_stream = (
        (F.col("kafka_topic") == F.col("_persisted_topic"))
        & (F.col("kafka_partition") == F.col("_persisted_partition"))
    )
    source_position_not_after = (
        F.col("_persisted_state").isNotNull()
        & (F.col("_batch_rank") == 1)
        & (
            (F.col("source_lsn").isNotNull() & F.col("_persisted_lsn").isNotNull())
            & (
                (F.col("source_lsn") < F.col("_persisted_lsn"))
                | (
                    (F.col("source_lsn") == F.col("_persisted_lsn"))
                    & same_stream
                    & (F.col("kafka_offset") <= F.col("_persisted_offset"))
                )
            )
            | (
                (F.col("source_lsn").isNull() | F.col("_persisted_lsn").isNull())
                & same_stream
                & (F.col("kafka_offset") <= F.col("_persisted_offset"))
            )
        )
    )
    if compared.filter(source_position_not_after).limit(1).count():
        raise RuntimeError(
            f"Out-of-order source position crossed the persisted boundary for {table_name}"
        )

    changed = compared.filter(
        (F.col("_batch_rank") > 1)
        | F.col("_persisted_state").isNull()
        | ~F.col(state_column).eqNullSafe(F.col("_persisted_state"))
    )
    return changed.drop(
        "_batch_rank",
        "_persisted_state",
        "_persisted_lsn",
        "_persisted_topic",
        "_persisted_partition",
        "_persisted_offset",
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
        build_link_loan_context(spark, events),
        "link_loan_context",
        "loan_context_hk",
    )


def bounded_satellite(
    spark: SparkSession, events: DataFrame, mapping: SatelliteMapping
) -> DataFrame:
    return filter_against_persisted_state(
        spark,
        build_satellite(events, mapping),
        table_name=mapping.table_name,
        entity_key=mapping.parent_hash_key,
        state_column="hashdiff",
    )


def bounded_record_tracking(spark: SparkSession, events: DataFrame) -> DataFrame:
    return filter_against_persisted_state(
        spark,
        build_source_record_tracking(events),
        table_name="sat_source_record_status",
        entity_key="source_record_hk",
        state_column="record_status",
    )


def bounded_entity_record_status(spark: SparkSession, events: DataFrame) -> DataFrame:
    return filter_against_persisted_state(
        spark,
        build_entity_record_status(events),
        table_name="sat_entity_record_status",
        entity_key="entity_hk",
        state_column="record_status",
    )


def bounded_application_context_effectivity(
    spark: SparkSession, events: DataFrame
) -> DataFrame:
    return filter_against_persisted_state(
        spark,
        build_application_context_effectivity(events),
        table_name="sat_application_context_effectivity",
        entity_key="loan_application_hk",
        state_column="effectivity_hashdiff",
    )


def bounded_loan_context_effectivity(
    spark: SparkSession, events: DataFrame
) -> DataFrame:
    return filter_against_persisted_state(
        spark,
        build_loan_context_effectivity(spark, events),
        table_name="sat_loan_context_effectivity",
        entity_key="loan_hk",
        state_column="effectivity_hashdiff",
    )


def current_state_satellites(
    spark: SparkSession, events: DataFrame, *, bounded: bool
) -> tuple[tuple[str, DataFrame], ...]:
    if bounded:
        return (
            ("sat_entity_record_status", bounded_entity_record_status(spark, events)),
            (
                "sat_application_context_effectivity",
                bounded_application_context_effectivity(spark, events),
            ),
            (
                "sat_loan_context_effectivity",
                bounded_loan_context_effectivity(spark, events),
            ),
        )
    return (
        ("sat_entity_record_status", build_entity_record_status(events)),
        (
            "sat_application_context_effectivity",
            build_application_context_effectivity(events),
        ),
        (
            "sat_loan_context_effectivity",
            build_loan_context_effectivity(spark, events),
        ),
    )


def run_current_state_satellites(
    spark: SparkSession, events: DataFrame, *, bounded: bool
) -> None:
    for table_name, source in current_state_satellites(spark, events, bounded=bounded):
        staged = source.localCheckpoint(eager=True)
        load_table(spark, staged, table_name, "source_event_id")
        staged.unpersist()


def run_satellites(
    spark: SparkSession, events: DataFrame, *, bounded: bool = False
) -> None:
    for mapping in SATELLITE_MAPPINGS:
        staged = (
            bounded_satellite(spark, events, mapping)
            if bounded
            else build_satellite(events, mapping)
        ).localCheckpoint(eager=True)
        load_table(
            spark,
            staged,
            mapping.table_name,
            "source_event_id",
        )
        staged.unpersist()
    tracking = (
        bounded_record_tracking(spark, events)
        if bounded
        else build_source_record_tracking(events)
    ).localCheckpoint(eager=True)
    load_table(
        spark,
        tracking,
        "sat_source_record_status",
        "source_event_id",
    )
    tracking.unpersist()
    run_current_state_satellites(spark, events, bounded=bounded)


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


def missing_target_key_count(
    spark: SparkSession,
    staged: DataFrame,
    *,
    table_name: str,
    key_column: str,
    base_path: str = CDC_RAW_VAULT_BASE_PATH,
) -> int:
    expected = staged.select(key_column).dropDuplicates([key_column])
    path = f"{base_path}/{table_name}"
    if not is_delta_table(spark, path):
        return expected.count()
    actual = spark.read.format("delta").load(path).select(key_column)
    return expected.join(actual, on=key_column, how="left_anti").count()


def bounded_reconciliation_rows(
    spark: SparkSession, events: DataFrame
) -> list[dict[str, object]]:
    checks = []
    for mapping in HUB_MAPPINGS:
        checks.append(
            (
                mapping.table_name,
                build_hub(events, mapping),
                mapping.hash_key,
                CDC_RAW_VAULT_BASE_PATH,
            )
        )
    checks.extend(
        [
            (
                "link_application_context",
                build_link_application_context(events),
                "application_context_hk",
                CDC_RAW_VAULT_BASE_PATH,
            ),
            (
                "link_loan_context",
                build_link_loan_context(spark, events),
                "loan_context_hk",
                CDC_RAW_VAULT_BASE_PATH,
            ),
        ]
    )
    for mapping in SATELLITE_MAPPINGS:
        checks.append(
            (
                mapping.table_name,
                bounded_satellite(spark, events, mapping),
                "source_event_id",
                CDC_RAW_VAULT_BASE_PATH,
            )
        )
    checks.extend(
        [
            (
                "sat_source_record_status",
                bounded_record_tracking(spark, events),
                "source_event_id",
                CDC_RAW_VAULT_BASE_PATH,
            ),
            *(
                (table_name, staged, "source_event_id", CDC_RAW_VAULT_BASE_PATH)
                for table_name, staged in current_state_satellites(
                    spark, events, bounded=True
                )
            ),
            (
                "cdc_raw_vault_events",
                build_quarantine(events),
                "quarantine_hk",
                SILVER_QUARANTINE_BASE_PATH,
            ),
        ]
    )

    rows = []
    for table_name, staged, key_column, base_path in checks:
        missing = missing_target_key_count(
            spark,
            staged,
            table_name=table_name,
            key_column=key_column,
            base_path=base_path,
        )
        rows.append(
            reconciliation_row(
                "BOUNDED_BATCH_TO_TARGET_MISSING_KEYS",
                table_name,
                0,
                missing,
            )
        )
    return rows


def point_in_time_control_rows(
    events: DataFrame,
    manifest: dict[str, object],
) -> list[dict[str, object]]:
    event_count = events.count()
    coordinate_count = events.select(
        "_metadata.kafka_topic",
        "_metadata.kafka_partition",
        "_metadata.kafka_offset",
    ).distinct().count()
    quarantined_keys = build_quarantine(events).select("bronze_object_key").distinct()
    quarantine_count = quarantined_keys.count()
    accepted_count = (
        events.select(F.col("_metadata.object_key").alias("bronze_object_key"))
        .join(quarantined_keys, on="bronze_object_key", how="left_anti")
        .count()
    )
    manifest_count = int(manifest["interval_object_count"])
    rows = [
        reconciliation_row(
            "MANIFEST_TO_BRONZE_EVENT_COUNT",
            "bronze_batch",
            manifest_count,
            event_count,
        ),
        reconciliation_row(
            "MANIFEST_TO_UNIQUE_KAFKA_COORDINATES",
            "bronze_batch",
            manifest_count,
            coordinate_count,
        ),
        reconciliation_row(
            "BRONZE_CLASSIFICATION_CONSERVATION",
            "accepted_plus_quarantine",
            event_count,
            accepted_count + quarantine_count,
        ),
    ]
    source_control = manifest.get("source_control")
    if isinstance(source_control, dict):
        transactions = source_control["transactions"]
        rows.extend(
            [
                reconciliation_row(
                    "SOURCE_LEDGER_TO_BRONZE_EVENTS",
                    "postgres_workload_ledger",
                    int(source_control["expected_event_count"]),
                    int(source_control["observed_event_count"]),
                ),
                reconciliation_row(
                    "SOURCE_TRANSACTION_COMPLETENESS",
                    "postgres_workload_ledger",
                    len(transactions),
                    sum(item["status"] == "PASS" for item in transactions),
                ),
                reconciliation_row(
                    "UNTRACKED_SOURCE_EVENTS",
                    "bronze_batch",
                    0,
                    int(source_control["untracked_event_count"]),
                ),
            ]
        )
    return rows


def point_in_time_control_rows_for_noop(
    manifest: dict[str, object],
) -> list[dict[str, object]]:
    manifest_count = int(manifest["interval_object_count"])
    rows = [
        reconciliation_row(
            "MANIFEST_TO_BRONZE_EVENT_COUNT", "bronze_batch", manifest_count, 0
        ),
        reconciliation_row(
            "MANIFEST_TO_UNIQUE_KAFKA_COORDINATES", "bronze_batch", manifest_count, 0
        ),
        reconciliation_row(
            "BRONZE_CLASSIFICATION_CONSERVATION", "accepted_plus_quarantine", 0, 0
        ),
    ]
    source_control = manifest.get("source_control")
    if isinstance(source_control, dict):
        transactions = source_control["transactions"]
        rows.extend(
            [
                reconciliation_row(
                    "SOURCE_LEDGER_TO_BRONZE_EVENTS",
                    "postgres_workload_ledger",
                    int(source_control["expected_event_count"]),
                    int(source_control["observed_event_count"]),
                ),
                reconciliation_row(
                    "SOURCE_TRANSACTION_COMPLETENESS",
                    "postgres_workload_ledger",
                    len(transactions),
                    sum(item["status"] == "PASS" for item in transactions),
                ),
                reconciliation_row(
                    "UNTRACKED_SOURCE_EVENTS",
                    "bronze_batch",
                    0,
                    int(source_control["untracked_event_count"]),
                ),
            ]
        )
    return rows


def audit_rules(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        {
            "rule_id": str(row["check_scope"]),
            "object_name": str(row["object_name"]),
            "scope": "point_in_time_batch",
            "expected_value": int(row["expected_count"]),
            "observed_value": int(row["actual_count"]),
            "difference": int(row["difference"]),
            "status": "PASS" if row["passed"] else "FAIL",
            "details": {},
        }
        for row in rows
    ]


def persist_audit_evidence(
    *,
    manifest: dict[str, object],
    manifest_sha256: str,
    attempt_number: int,
    airflow_run_id: str,
    rows: list[dict[str, object]],
) -> tuple[str, str]:
    uri = audit_evidence_uri(S3_BUCKET, LOAD_BATCH_ID, attempt_number)
    payload = build_audit_payload(
        batch_id=LOAD_BATCH_ID,
        attempt_number=attempt_number,
        airflow_run_id=airflow_run_id,
        manifest_sha256=manifest_sha256,
        manifest=manifest,
        rules=audit_rules(rows),
    )
    checksum = write_audit_evidence(s3_client(), uri, payload)
    print(
        f"Point-in-time audit evidence persisted: uri={uri}; sha256={checksum}; "
        f"rules={len(rows)}; failed={sum(not row['passed'] for row in rows)}"
    )
    return uri, checksum


def run_reconciliation(
    spark: SparkSession,
    events: DataFrame,
    *,
    manifest_snapshot: bool = False,
    bounded_manifest: bool = False,
    manifest: dict[str, object] | None = None,
    manifest_sha256: str | None = None,
    attempt_number: int | None = None,
    airflow_run_id: str | None = None,
) -> None:
    rows: list[dict[str, object]] = []
    if bounded_manifest:
        print(
            "Running bounded batch-to-target reconciliation; point-in-time source "
            "controls are a separate acceptance gate."
        )
        if manifest is None:
            raise RuntimeError("Bounded reconciliation requires its manifest payload")
        rows = point_in_time_control_rows(events, manifest)
        rows.extend(bounded_reconciliation_rows(spark, events))
    elif manifest_snapshot:
        print(
            "Skipping live source-count comparison for the frozen manifest snapshot; "
            "point-in-time source controls are a separate acceptance gate."
        )
    else:
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

    if not bounded_manifest:
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
            ("link_loan_context", build_link_loan_context(spark, events)),
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
        for table_name, staged in current_state_satellites(
            spark, events, bounded=False
        ):
            rows.append(
                reconciliation_row(
                    "BRONZE_TO_CURRENT_STATE_HISTORY",
                    table_name,
                    staged.count(),
                    delta_count(spark, CDC_RAW_VAULT_BASE_PATH, table_name),
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
    if bounded_manifest:
        if manifest_sha256 is None or attempt_number is None or airflow_run_id is None:
            raise RuntimeError("Bounded reconciliation requires immutable attempt context")
        persist_audit_evidence(
            manifest=manifest,
            manifest_sha256=manifest_sha256,
            attempt_number=attempt_number,
            airflow_run_id=airflow_run_id,
            rows=rows,
        )
        if failed:
            print(f"Point-in-time audit contains failed rules: {failed}")
        else:
            print(f"All {len(rows)} point-in-time audit rules passed.")
        return
    if failed:
        raise RuntimeError(f"Raw Vault reconciliation failed: {failed}")
    print(f"All {len(rows)} reconciliation checks passed.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--phase",
        choices=(
            "validate",
            "core",
            "satellites",
            "current-state-backfill",
            "reconcile",
            "all",
        ),
        default="all",
        help="Run one orchestration phase or the complete local pipeline.",
    )
    parser.add_argument("--batch-id", help="Expected immutable control-plane batch ID")
    parser.add_argument("--attempt-number", type=int, help="Control-plane attempt number")
    parser.add_argument("--airflow-run-id", help="Airflow run ID owning this attempt")
    parser.add_argument("--manifest-uri", help="S3 URI of the immutable batch manifest")
    parser.add_argument(
        "--manifest-sha256",
        help="Expected SHA-256 of the canonical manifest payload",
    )
    return parser.parse_args()


def main() -> None:
    global LOAD_BATCH_ID

    args = parse_args()
    if bool(args.manifest_uri) != bool(args.manifest_sha256):
        raise RuntimeError("--manifest-uri and --manifest-sha256 must be provided together")
    manifest = None
    manifest_bucket = S3_BUCKET
    if args.manifest_uri:
        expected_batch_id = args.batch_id or LOAD_BATCH_ID
        manifest_bucket, _ = parse_s3_uri(args.manifest_uri)
        if manifest_bucket != S3_BUCKET:
            raise RuntimeError(
                f"Manifest bucket {manifest_bucket!r} differs from configured {S3_BUCKET!r}"
            )
        manifest = read_manifest(
            s3_client(),
            args.manifest_uri,
            expected_sha256=args.manifest_sha256,
            expected_batch_id=expected_batch_id,
        )
        if manifest.get("reader_mode") not in {
            LEGACY_READER_MODE,
            BOUNDED_READER_MODE,
        }:
            raise RuntimeError(f"Unsupported manifest reader mode: {manifest.get('reader_mode')}")
        LOAD_BATCH_ID = str(manifest["batch_id"])
    elif args.batch_id:
        LOAD_BATCH_ID = args.batch_id

    if (
        manifest is not None
        and manifest.get("reader_mode") == BOUNDED_READER_MODE
        and not manifest["objects"]
    ):
        if args.phase in ("reconcile", "all"):
            if args.attempt_number is None or not args.airflow_run_id:
                raise RuntimeError("No-op reconciliation requires immutable attempt context")
            rows = point_in_time_control_rows_for_noop(manifest)
            persist_audit_evidence(
                manifest=manifest,
                manifest_sha256=str(args.manifest_sha256),
                attempt_number=args.attempt_number,
                airflow_run_id=args.airflow_run_id,
                rows=rows,
            )
        print(
            f"Skipping no-op Raw Vault phase {args.phase!r}; batch={LOAD_BATCH_ID}; "
            f"manifest={args.manifest_uri}; selected_input_objects=0; selected_input_bytes=0"
        )
        return

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    try:
        events = read_cdc_events(
            spark, manifest, manifest_bucket=manifest_bucket
        ).persist(StorageLevel.DISK_ONLY)
        event_count = events.count()
        manifest_detail = ""
        if manifest is not None:
            interval_objects = sum(
                int(bound["interval_object_count"])
                for bound in manifest["partitions"]
            )
            interval_bytes = int(manifest.get("interval_input_bytes", 0))
            manifest_detail = (
                f"; manifest={args.manifest_uri}; manifest_sha256={args.manifest_sha256}; "
                f"selected_input_objects={interval_objects:,}; "
                f"selected_input_bytes={interval_bytes:,}; reader_mode={manifest['reader_mode']}"
            )
        print(
            f"Read {event_count:,} Bronze CDC records from {BRONZE_CDC_PATH}; "
            f"hash_standard={HASH_ALGORITHM}/UPPER-TRIM/{HASH_DELIMITER}/{HASH_NULL_TOKEN}"
            f"{manifest_detail}"
        )
        if args.phase in ("validate", "all"):
            run_validate(spark, events)
        if args.phase in ("core", "all"):
            run_core(spark, events)
        if args.phase in ("satellites", "all"):
            run_satellites(
                spark,
                events,
                bounded=manifest is not None
                and manifest.get("reader_mode") == BOUNDED_READER_MODE,
            )
        if args.phase == "current-state-backfill":
            if manifest is not None:
                raise RuntimeError(
                    "Current-state backfill must read complete Bronze history"
                )
            run_current_state_satellites(spark, events, bounded=False)
        if args.phase in ("reconcile", "all"):
            run_reconciliation(
                spark,
                events,
                manifest_snapshot=manifest is not None,
                bounded_manifest=manifest is not None
                and manifest.get("reader_mode") == BOUNDED_READER_MODE,
                manifest=manifest,
                manifest_sha256=args.manifest_sha256,
                attempt_number=args.attempt_number,
                airflow_run_id=args.airflow_run_id,
            )
        events.unpersist()
    finally:
        spark.stop()

    print(f"CDC Raw Vault phase {args.phase!r} completed.")


if __name__ == "__main__":
    main()

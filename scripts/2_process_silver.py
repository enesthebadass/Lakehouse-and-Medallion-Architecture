"""Clean bronze data and build a Data Vault 2.0 Raw Vault in Silver."""

from __future__ import annotations

import os
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "lakehouse")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

BRONZE_BASE_PATH = f"s3a://{S3_BUCKET}/bronze"
SILVER_BASE_PATH = f"s3a://{S3_BUCKET}/silver"
RAW_VAULT_BASE_PATH = f"{SILVER_BASE_PATH}/raw_vault"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("lakehouse-medallion-process-silver-data-vault")
        .master(SPARK_MASTER_URL)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def delete_path(spark: SparkSession, path: str) -> None:
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs_path = spark._jvm.org.apache.hadoop.fs.Path(path)
    fs = fs_path.getFileSystem(hadoop_conf)
    if fs.exists(fs_path):
        fs.delete(fs_path, True)


def read_bronze_table(spark: SparkSession, table_name: str) -> DataFrame:
    return spark.read.option("multiLine", "false").json(f"{BRONZE_BASE_PATH}/{table_name}/*.jsonl")


def write_delta(df: DataFrame, table_name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{RAW_VAULT_BASE_PATH}/{table_name}")
    )


def parse_timestamp(column_name: str) -> F.Column:
    return F.to_timestamp(F.col(column_name))


def parse_date(column_name: str) -> F.Column:
    return F.to_date(F.col(column_name))


def cast_double(column_name: str) -> F.Column:
    return F.regexp_replace(F.col(column_name).cast("string"), ",", ".").cast("double")


def cast_int(column_name: str) -> F.Column:
    return F.col(column_name).cast("int")


def require_columns(df: DataFrame, columns: Iterable[str]) -> DataFrame:
    return df.dropna(subset=list(columns))


def hash_columns(*columns: str) -> F.Column:
    normalized = [F.coalesce(F.col(column).cast("string"), F.lit("^^")) for column in columns]
    return F.sha2(F.concat_ws("||", *normalized), 256)


def add_audit_columns(df: DataFrame, record_source: str) -> DataFrame:
    return df.withColumn("load_datetime", F.current_timestamp()).withColumn("record_source", F.lit(record_source))


def clean_customers(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("first_name").cast("string").alias("first_name"),
            F.col("last_name").cast("string").alias("last_name"),
            F.when(F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$"), F.col("email")).alias("email"),
            F.col("phone_number").cast("string").alias("phone_number"),
            parse_date("date_of_birth").alias("date_of_birth"),
            F.col("city").cast("string").alias("city"),
            F.col("country").cast("string").alias("country"),
            F.col("customer_segment").cast("string").alias("customer_segment"),
            parse_timestamp("created_at").alias("created_at"),
        )
        .dropDuplicates(["customer_id"])
        .transform(lambda clean_df: require_columns(clean_df, ["customer_id"]))
    )


def clean_accounts(df: DataFrame, customers: DataFrame) -> DataFrame:
    clean_df = (
        df.select(
            F.col("account_id").cast("string").alias("account_id"),
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("account_type").cast("string").alias("account_type"),
            F.col("account_status").cast("string").alias("account_status"),
            F.col("card_network").cast("string").alias("card_network"),
            F.col("masked_card_number").cast("string").alias("masked_card_number"),
            cast_double("credit_limit").alias("credit_limit"),
            parse_timestamp("opened_at").alias("opened_at"),
        )
        .dropDuplicates(["account_id"])
        .transform(lambda current_df: require_columns(current_df, ["account_id", "customer_id"]))
    )
    return clean_df.join(customers.select("customer_id"), on="customer_id", how="left_semi")


def clean_merchants(df: DataFrame) -> DataFrame:
    return (
        df.select(
            F.col("merchant_id").cast("string").alias("merchant_id"),
            F.col("merchant_name").cast("string").alias("merchant_name"),
            F.col("merchant_category").cast("string").alias("merchant_category"),
            F.col("city").cast("string").alias("city"),
            F.col("country").cast("string").alias("country"),
            parse_timestamp("onboarded_at").alias("onboarded_at"),
            cast_int("risk_score").alias("risk_score"),
        )
        .dropDuplicates(["merchant_id"])
        .transform(lambda clean_df: require_columns(clean_df, ["merchant_id"]))
    )


def clean_transactions(
    df: DataFrame,
    customers: DataFrame,
    accounts: DataFrame,
    merchants: DataFrame,
) -> DataFrame:
    clean_df = (
        df.select(
            F.col("transaction_id").cast("string").alias("transaction_id"),
            F.col("account_id").cast("string").alias("account_id"),
            F.col("customer_id").cast("string").alias("customer_id"),
            F.col("merchant_id").cast("string").alias("merchant_id"),
            cast_double("amount").alias("amount"),
            F.col("currency").cast("string").alias("currency"),
            F.col("transaction_status").cast("string").alias("transaction_status"),
            F.col("transaction_channel").cast("string").alias("transaction_channel"),
            parse_timestamp("transaction_timestamp").alias("transaction_timestamp"),
        )
        .dropDuplicates(["transaction_id"])
        .transform(
            lambda current_df: require_columns(
                current_df,
                [
                    "transaction_id",
                    "account_id",
                    "customer_id",
                    "merchant_id",
                    "amount",
                    "transaction_timestamp",
                ],
            )
        )
    )

    valid_accounts = accounts.select("account_id", "customer_id")
    return (
        clean_df.join(valid_accounts, on=["account_id", "customer_id"], how="left_semi")
        .join(customers.select("customer_id"), on="customer_id", how="left_semi")
        .join(merchants.select("merchant_id"), on="merchant_id", how="left_semi")
    )


def build_hub_customer(customers: DataFrame) -> DataFrame:
    return add_audit_columns(
        customers.select(hash_columns("customer_id").alias("customer_hk"), "customer_id"),
        "bronze.customers",
    )


def build_hub_account(accounts: DataFrame) -> DataFrame:
    return add_audit_columns(
        accounts.select(hash_columns("account_id").alias("account_hk"), "account_id"),
        "bronze.accounts",
    )


def build_hub_merchant(merchants: DataFrame) -> DataFrame:
    return add_audit_columns(
        merchants.select(hash_columns("merchant_id").alias("merchant_hk"), "merchant_id"),
        "bronze.merchants",
    )


def build_hub_transaction(transactions: DataFrame) -> DataFrame:
    return add_audit_columns(
        transactions.select(hash_columns("transaction_id").alias("transaction_hk"), "transaction_id"),
        "bronze.transactions",
    )


def build_link_customer_account(accounts: DataFrame) -> DataFrame:
    link = accounts.select(
        hash_columns("customer_id", "account_id").alias("customer_account_hk"),
        hash_columns("customer_id").alias("customer_hk"),
        hash_columns("account_id").alias("account_hk"),
    ).dropDuplicates(["customer_account_hk"])
    return add_audit_columns(link, "bronze.accounts")


def build_link_transaction_context(transactions: DataFrame) -> DataFrame:
    link = transactions.select(
        hash_columns("transaction_id", "customer_id", "account_id", "merchant_id").alias("transaction_context_hk"),
        hash_columns("transaction_id").alias("transaction_hk"),
        hash_columns("customer_id").alias("customer_hk"),
        hash_columns("account_id").alias("account_hk"),
        hash_columns("merchant_id").alias("merchant_hk"),
    ).dropDuplicates(["transaction_context_hk"])
    return add_audit_columns(link, "bronze.transactions")


def build_sat_customer_profile(customers: DataFrame) -> DataFrame:
    sat = customers.select(
        hash_columns("customer_id").alias("customer_hk"),
        "first_name",
        "last_name",
        "email",
        "phone_number",
        "date_of_birth",
        "city",
        "country",
        "customer_segment",
        "created_at",
        hash_columns(
            "first_name",
            "last_name",
            "email",
            "phone_number",
            "date_of_birth",
            "city",
            "country",
            "customer_segment",
            "created_at",
        ).alias("hashdiff"),
    )
    return add_audit_columns(sat, "bronze.customers")


def build_sat_account_details(accounts: DataFrame) -> DataFrame:
    sat = accounts.select(
        hash_columns("account_id").alias("account_hk"),
        "account_type",
        "account_status",
        "card_network",
        "masked_card_number",
        "credit_limit",
        "opened_at",
        hash_columns(
            "account_type",
            "account_status",
            "card_network",
            "masked_card_number",
            "credit_limit",
            "opened_at",
        ).alias("hashdiff"),
    )
    return add_audit_columns(sat, "bronze.accounts")


def build_sat_merchant_details(merchants: DataFrame) -> DataFrame:
    sat = merchants.select(
        hash_columns("merchant_id").alias("merchant_hk"),
        "merchant_name",
        "merchant_category",
        "city",
        "country",
        "onboarded_at",
        "risk_score",
        hash_columns(
            "merchant_name",
            "merchant_category",
            "city",
            "country",
            "onboarded_at",
            "risk_score",
        ).alias("hashdiff"),
    )
    return add_audit_columns(sat, "bronze.merchants")


def build_sat_transaction_details(transactions: DataFrame) -> DataFrame:
    sat = transactions.select(
        hash_columns("transaction_id").alias("transaction_hk"),
        "amount",
        "currency",
        "transaction_status",
        "transaction_channel",
        "transaction_timestamp",
        hash_columns(
            "amount",
            "currency",
            "transaction_status",
            "transaction_channel",
            "transaction_timestamp",
        ).alias("hashdiff"),
    )
    return add_audit_columns(sat, "bronze.transactions")


def log_counts(table_name: str, bronze_df: DataFrame, clean_df: DataFrame) -> None:
    bronze_count = bronze_df.count()
    clean_count = clean_df.count()
    rejected_count = bronze_count - clean_count
    print(
        f"{table_name}: bronze={bronze_count:,}, clean_for_vault={clean_count:,}, "
        f"rejected={rejected_count:,}"
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # The legacy demo owns only this path. CDC Raw Vault tables live under
    # silver/cdc_raw_vault and must survive a fallback demo run.
    delete_path(spark, RAW_VAULT_BASE_PATH)

    bronze_customers = read_bronze_table(spark, "customers")
    bronze_accounts = read_bronze_table(spark, "accounts")
    bronze_merchants = read_bronze_table(spark, "merchants")
    bronze_transactions = read_bronze_table(spark, "transactions")

    clean_customers_df = clean_customers(bronze_customers)
    clean_accounts_df = clean_accounts(bronze_accounts, clean_customers_df)
    clean_merchants_df = clean_merchants(bronze_merchants)
    clean_transactions_df = clean_transactions(
        bronze_transactions,
        clean_customers_df,
        clean_accounts_df,
        clean_merchants_df,
    )

    log_counts("customers", bronze_customers, clean_customers_df)
    log_counts("accounts", bronze_accounts, clean_accounts_df)
    log_counts("merchants", bronze_merchants, clean_merchants_df)
    log_counts("transactions", bronze_transactions, clean_transactions_df)

    vault_tables = {
        "hub_customer": build_hub_customer(clean_customers_df),
        "hub_account": build_hub_account(clean_accounts_df),
        "hub_merchant": build_hub_merchant(clean_merchants_df),
        "hub_transaction": build_hub_transaction(clean_transactions_df),
        "link_customer_account": build_link_customer_account(clean_accounts_df),
        "link_transaction_context": build_link_transaction_context(clean_transactions_df),
        "sat_customer_profile": build_sat_customer_profile(clean_customers_df),
        "sat_account_details": build_sat_account_details(clean_accounts_df),
        "sat_merchant_details": build_sat_merchant_details(clean_merchants_df),
        "sat_transaction_details": build_sat_transaction_details(clean_transactions_df),
    }

    for table_name, df in vault_tables.items():
        row_count = df.count()
        write_delta(df, table_name)
        print(f"Wrote {row_count:,} rows to {RAW_VAULT_BASE_PATH}/{table_name}")

    spark.stop()
    print("Silver Data Vault processing completed.")


if __name__ == "__main__":
    main()

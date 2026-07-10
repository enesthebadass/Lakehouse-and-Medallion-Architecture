"""Clean bronze JSON data and write Delta tables to the silver layer."""

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


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("lakehouse-medallion-process-silver")
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


def read_bronze_table(spark: SparkSession, table_name: str) -> DataFrame:
    return spark.read.option("multiLine", "false").json(f"{BRONZE_BASE_PATH}/{table_name}/*.jsonl")


def write_delta(df: DataFrame, table_name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{SILVER_BASE_PATH}/{table_name}")
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


def log_counts(table_name: str, bronze_df: DataFrame, silver_df: DataFrame) -> None:
    bronze_count = bronze_df.count()
    silver_count = silver_df.count()
    rejected_count = bronze_count - silver_count
    print(
        f"{table_name}: bronze={bronze_count:,}, silver={silver_count:,}, "
        f"rejected={rejected_count:,}"
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    bronze_customers = read_bronze_table(spark, "customers")
    bronze_accounts = read_bronze_table(spark, "accounts")
    bronze_merchants = read_bronze_table(spark, "merchants")
    bronze_transactions = read_bronze_table(spark, "transactions")

    silver_customers = clean_customers(bronze_customers)
    silver_accounts = clean_accounts(bronze_accounts, silver_customers)
    silver_merchants = clean_merchants(bronze_merchants)
    silver_transactions = clean_transactions(
        bronze_transactions,
        silver_customers,
        silver_accounts,
        silver_merchants,
    )

    outputs = {
        "customers": (bronze_customers, silver_customers),
        "accounts": (bronze_accounts, silver_accounts),
        "merchants": (bronze_merchants, silver_merchants),
        "transactions": (bronze_transactions, silver_transactions),
    }

    for table_name, (bronze_df, silver_df) in outputs.items():
        log_counts(table_name, bronze_df, silver_df)
        write_delta(silver_df, table_name)
        print(f"Wrote Delta table to {SILVER_BASE_PATH}/{table_name}")

    spark.stop()
    print("Silver processing completed.")


if __name__ == "__main__":
    main()

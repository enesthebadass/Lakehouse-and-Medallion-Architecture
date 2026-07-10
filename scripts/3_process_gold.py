"""Build Kimball-style gold Delta tables from cleaned silver Delta tables."""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "lakehouse")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

SILVER_BASE_PATH = f"s3a://{S3_BUCKET}/silver"
GOLD_BASE_PATH = f"s3a://{S3_BUCKET}/gold"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("lakehouse-medallion-process-gold")
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


def read_silver_table(spark: SparkSession, table_name: str) -> DataFrame:
    return spark.read.format("delta").load(f"{SILVER_BASE_PATH}/{table_name}")


def write_gold_table(df: DataFrame, table_name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{GOLD_BASE_PATH}/{table_name}")
    )


def build_dim_customer(customers: DataFrame) -> DataFrame:
    return customers.select(
        "customer_id",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name")).alias("customer_full_name"),
        "email",
        "phone_number",
        "date_of_birth",
        F.floor(F.months_between(F.current_date(), F.col("date_of_birth")) / 12).cast("int").alias("age"),
        "city",
        "country",
        "customer_segment",
        "created_at",
    )


def build_dim_account(accounts: DataFrame) -> DataFrame:
    return accounts.select(
        "account_id",
        "customer_id",
        "account_type",
        "account_status",
        "card_network",
        "masked_card_number",
        "credit_limit",
        "opened_at",
    )


def build_dim_merchant(merchants: DataFrame) -> DataFrame:
    return merchants.select(
        "merchant_id",
        "merchant_name",
        "merchant_category",
        "city",
        "country",
        "risk_score",
        F.when(F.col("risk_score") >= 80, F.lit("high"))
        .when(F.col("risk_score") >= 50, F.lit("medium"))
        .when(F.col("risk_score").isNotNull(), F.lit("low"))
        .otherwise(F.lit("unknown"))
        .alias("risk_band"),
        "onboarded_at",
    )


def build_fact_transactions(
    transactions: DataFrame,
    dim_customer: DataFrame,
    dim_account: DataFrame,
    dim_merchant: DataFrame,
) -> DataFrame:
    fact = transactions.select(
        "transaction_id",
        "customer_id",
        "account_id",
        "merchant_id",
        "amount",
        "currency",
        "transaction_status",
        "transaction_channel",
        "transaction_timestamp",
        F.to_date("transaction_timestamp").alias("transaction_date"),
        F.hour("transaction_timestamp").alias("transaction_hour"),
        F.date_format("transaction_timestamp", "yyyy-MM").alias("transaction_month"),
    )

    return (
        fact.join(dim_customer.select("customer_id"), on="customer_id", how="left_semi")
        .join(dim_account.select("account_id"), on="account_id", how="left_semi")
        .join(dim_merchant.select("merchant_id"), on="merchant_id", how="left_semi")
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    silver_customers = read_silver_table(spark, "customers")
    silver_accounts = read_silver_table(spark, "accounts")
    silver_merchants = read_silver_table(spark, "merchants")
    silver_transactions = read_silver_table(spark, "transactions")

    dim_customer = build_dim_customer(silver_customers)
    dim_account = build_dim_account(silver_accounts)
    dim_merchant = build_dim_merchant(silver_merchants)
    fact_transactions = build_fact_transactions(
        silver_transactions,
        dim_customer,
        dim_account,
        dim_merchant,
    )

    gold_tables = {
        "dim_customer": dim_customer,
        "dim_account": dim_account,
        "dim_merchant": dim_merchant,
        "fact_transactions": fact_transactions,
    }

    for table_name, df in gold_tables.items():
        row_count = df.count()
        write_gold_table(df, table_name)
        print(f"Wrote {row_count:,} rows to {GOLD_BASE_PATH}/{table_name}")

    spark.stop()
    print("Gold processing completed.")


if __name__ == "__main__":
    main()

"""Build Kimball-style gold Delta tables from the Silver Data Vault."""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "lakehouse")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")

RAW_VAULT_BASE_PATH = f"s3a://{S3_BUCKET}/silver/raw_vault"
GOLD_BASE_PATH = f"s3a://{S3_BUCKET}/gold"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("lakehouse-medallion-process-gold-from-vault")
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


def read_vault_table(spark: SparkSession, table_name: str) -> DataFrame:
    return spark.read.format("delta").load(f"{RAW_VAULT_BASE_PATH}/{table_name}")


def write_gold_table(df: DataFrame, table_name: str) -> None:
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(f"{GOLD_BASE_PATH}/{table_name}")
    )


def latest_satellite(df: DataFrame, hash_key_column: str) -> DataFrame:
    window = Window.partitionBy(hash_key_column).orderBy(F.col("load_datetime").desc())
    return df.withColumn("row_number", F.row_number().over(window)).where(F.col("row_number") == 1).drop("row_number")


def build_dim_customer(hub_customer: DataFrame, sat_customer_profile: DataFrame) -> DataFrame:
    sat = latest_satellite(sat_customer_profile, "customer_hk")
    return (
        hub_customer.join(sat, on="customer_hk", how="inner")
        .select(
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
    )


def build_dim_account(
    hub_account: DataFrame,
    hub_customer: DataFrame,
    link_customer_account: DataFrame,
    sat_account_details: DataFrame,
) -> DataFrame:
    sat = latest_satellite(sat_account_details, "account_hk")
    account_customer = (
        link_customer_account.join(hub_account.select("account_hk", "account_id"), on="account_hk", how="inner")
        .join(hub_customer.select("customer_hk", "customer_id"), on="customer_hk", how="inner")
        .select("account_hk", "account_id", "customer_id")
    )
    return account_customer.join(sat, on="account_hk", how="inner").select(
        "account_id",
        "customer_id",
        "account_type",
        "account_status",
        "card_network",
        "masked_card_number",
        "credit_limit",
        "opened_at",
    )


def build_dim_merchant(hub_merchant: DataFrame, sat_merchant_details: DataFrame) -> DataFrame:
    sat = latest_satellite(sat_merchant_details, "merchant_hk")
    return (
        hub_merchant.join(sat, on="merchant_hk", how="inner")
        .select(
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
    )


def build_fact_transactions(
    hub_transaction: DataFrame,
    hub_customer: DataFrame,
    hub_account: DataFrame,
    hub_merchant: DataFrame,
    link_transaction_context: DataFrame,
    sat_transaction_details: DataFrame,
    dim_customer: DataFrame,
    dim_account: DataFrame,
    dim_merchant: DataFrame,
) -> DataFrame:
    sat = latest_satellite(sat_transaction_details, "transaction_hk")
    transaction_context = (
        link_transaction_context.join(
            hub_transaction.select("transaction_hk", "transaction_id"), on="transaction_hk", how="inner"
        )
        .join(hub_customer.select("customer_hk", "customer_id"), on="customer_hk", how="inner")
        .join(hub_account.select("account_hk", "account_id"), on="account_hk", how="inner")
        .join(hub_merchant.select("merchant_hk", "merchant_id"), on="merchant_hk", how="inner")
        .select("transaction_hk", "transaction_id", "customer_id", "account_id", "merchant_id")
    )

    fact = transaction_context.join(sat, on="transaction_hk", how="inner").select(
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

    delete_path(spark, GOLD_BASE_PATH)

    hub_customer = read_vault_table(spark, "hub_customer")
    hub_account = read_vault_table(spark, "hub_account")
    hub_merchant = read_vault_table(spark, "hub_merchant")
    hub_transaction = read_vault_table(spark, "hub_transaction")
    link_customer_account = read_vault_table(spark, "link_customer_account")
    link_transaction_context = read_vault_table(spark, "link_transaction_context")
    sat_customer_profile = read_vault_table(spark, "sat_customer_profile")
    sat_account_details = read_vault_table(spark, "sat_account_details")
    sat_merchant_details = read_vault_table(spark, "sat_merchant_details")
    sat_transaction_details = read_vault_table(spark, "sat_transaction_details")

    dim_customer = build_dim_customer(hub_customer, sat_customer_profile)
    dim_account = build_dim_account(hub_account, hub_customer, link_customer_account, sat_account_details)
    dim_merchant = build_dim_merchant(hub_merchant, sat_merchant_details)
    fact_transactions = build_fact_transactions(
        hub_transaction,
        hub_customer,
        hub_account,
        hub_merchant,
        link_transaction_context,
        sat_transaction_details,
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
    print("Gold processing from Data Vault completed.")


if __name__ == "__main__":
    main()

"""Generate dirty bronze-layer JSON data for the lakehouse demo.

The script is intentionally idempotent: it deletes the existing bronze table
objects and writes fresh JSON Lines files on each run.
"""

from __future__ import annotations

import json
import os
import random
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Iterable

import boto3
from botocore.config import Config
from faker import Faker


S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localhost:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "lakehouse")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

CUSTOMER_COUNT = int(os.getenv("CUSTOMER_COUNT", "10000"))
ACCOUNT_COUNT = int(os.getenv("ACCOUNT_COUNT", "10000"))
MERCHANT_COUNT = int(os.getenv("MERCHANT_COUNT", "10000"))
TRANSACTION_COUNT = int(os.getenv("TRANSACTION_COUNT", "10000"))
DIRTY_RATIO = float(os.getenv("DIRTY_RATIO", "0.25"))
RANDOM_SEED = int(os.getenv("RANDOM_SEED", "42"))

TABLE_KEYS = {
    "customers": "bronze/customers/customers.jsonl",
    "accounts": "bronze/accounts/accounts.jsonl",
    "merchants": "bronze/merchants/merchants.jsonl",
    "transactions": "bronze/transactions/transactions.jsonl",
}

ACCOUNT_TYPES = ["checking", "savings", "credit_card"]
ACCOUNT_STATUSES = ["active", "blocked", "closed"]
CARD_NETWORKS = ["Visa", "Mastercard", "Troy", "American Express"]
MERCHANT_CATEGORIES = [
    "grocery",
    "fuel",
    "electronics",
    "travel",
    "restaurant",
    "healthcare",
    "ecommerce",
    "utilities",
]
TRANSACTION_STATUSES = ["approved", "declined", "reversed"]
TRANSACTION_CHANNELS = ["pos", "ecommerce", "atm", "mobile"]
BAD_AMOUNT_VALUES = ["Yuz TL", "Bilinmiyor", "N/A", "one hundred", ""]
BAD_DATE_VALUES = ["31-31-2024", "not-a-date", "2024/99/99", "yesterday-ish", ""]


fake = Faker("tr_TR")
Faker.seed(RANDOM_SEED)
random.seed(RANDOM_SEED)


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def decimal_to_float(value: Decimal) -> float:
    return float(round(value, 2))


def iso_date(value: datetime) -> str:
    return value.date().isoformat()


def iso_timestamp(value: datetime) -> str:
    return value.replace(tzinfo=timezone.utc).isoformat()


def is_dirty(index: int, total_count: int) -> bool:
    dirty_count = int(total_count * DIRTY_RATIO)
    return index <= dirty_count


def delete_prefix(client, prefix: str) -> None:
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
        if objects:
            client.delete_objects(Bucket=S3_BUCKET, Delete={"Objects": objects})


def write_jsonl(client, key: str, rows: Iterable[dict[str, Any]]) -> None:
    body = "\n".join(json.dumps(row, ensure_ascii=False, default=str) for row in rows)
    client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=(body + "\n").encode("utf-8"),
        ContentType="application/x-ndjson",
    )


def generate_customers() -> list[dict[str, Any]]:
    rows = []
    for index in range(1, CUSTOMER_COUNT + 1):
        customer_id = f"CUST{index:06d}"
        row = {
            "customer_id": customer_id,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone_number": fake.phone_number(),
            "date_of_birth": iso_date(fake.date_time_between(start_date="-75y", end_date="-18y")),
            "city": fake.city(),
            "country": "TR",
            "customer_segment": random.choice(["retail", "sme", "private", "mass_affluent"]),
            "created_at": iso_timestamp(fake.date_time_between(start_date="-8y", end_date="-1d")),
        }

        if is_dirty(index, CUSTOMER_COUNT):
            dirty_field = random.choice(["customer_id", "email", "date_of_birth", "created_at"])
            if dirty_field == "customer_id":
                row["customer_id"] = None
            elif dirty_field == "email":
                row["email"] = "invalid-email"
            elif dirty_field == "date_of_birth":
                row["date_of_birth"] = random.choice(BAD_DATE_VALUES)
            else:
                row["created_at"] = random.choice(BAD_DATE_VALUES)

        rows.append(row)
    return rows


def generate_accounts(valid_customer_ids: list[str]) -> list[dict[str, Any]]:
    rows = []
    for index in range(1, ACCOUNT_COUNT + 1):
        account_type = random.choice(ACCOUNT_TYPES)
        row = {
            "account_id": f"ACC{index:06d}",
            "customer_id": random.choice(valid_customer_ids),
            "account_type": account_type,
            "account_status": random.choice(ACCOUNT_STATUSES),
            "card_network": random.choice(CARD_NETWORKS) if account_type == "credit_card" else None,
            "masked_card_number": fake.credit_card_number(card_type=None)[-4:] if account_type == "credit_card" else None,
            "credit_limit": decimal_to_float(fake.pydecimal(left_digits=5, right_digits=2, positive=True)),
            "opened_at": iso_timestamp(fake.date_time_between(start_date="-7y", end_date="-1d")),
        }

        if is_dirty(index, ACCOUNT_COUNT):
            dirty_field = random.choice(["account_id", "customer_id", "credit_limit", "opened_at"])
            if dirty_field == "account_id":
                row["account_id"] = None
            elif dirty_field == "customer_id":
                row["customer_id"] = None
            elif dirty_field == "credit_limit":
                row["credit_limit"] = random.choice(BAD_AMOUNT_VALUES)
            else:
                row["opened_at"] = random.choice(BAD_DATE_VALUES)

        rows.append(row)
    return rows


def generate_merchants() -> list[dict[str, Any]]:
    rows = []
    for index in range(1, MERCHANT_COUNT + 1):
        row = {
            "merchant_id": f"MER{index:06d}",
            "merchant_name": fake.company(),
            "merchant_category": random.choice(MERCHANT_CATEGORIES),
            "city": fake.city(),
            "country": "TR",
            "onboarded_at": iso_timestamp(fake.date_time_between(start_date="-6y", end_date="-1d")),
            "risk_score": random.randint(1, 100),
        }

        if is_dirty(index, MERCHANT_COUNT):
            dirty_field = random.choice(["merchant_id", "onboarded_at", "risk_score"])
            if dirty_field == "merchant_id":
                row["merchant_id"] = None
            elif dirty_field == "onboarded_at":
                row["onboarded_at"] = random.choice(BAD_DATE_VALUES)
            else:
                row["risk_score"] = random.choice(["high", "unknown", None])

        rows.append(row)
    return rows


def generate_transactions(
    valid_account_ids: list[str],
    account_to_customer: dict[str, str],
    valid_merchant_ids: list[str],
) -> list[dict[str, Any]]:
    rows = []
    for index in range(1, TRANSACTION_COUNT + 1):
        account_id = random.choice(valid_account_ids)
        row = {
            "transaction_id": f"TXN{index:08d}",
            "account_id": account_id,
            "customer_id": account_to_customer[account_id],
            "merchant_id": random.choice(valid_merchant_ids),
            "amount": decimal_to_float(fake.pydecimal(left_digits=4, right_digits=2, positive=True)),
            "currency": "TRY",
            "transaction_status": random.choice(TRANSACTION_STATUSES),
            "transaction_channel": random.choice(TRANSACTION_CHANNELS),
            "transaction_timestamp": iso_timestamp(fake.date_time_between(start_date="-18M", end_date="now")),
        }

        if is_dirty(index, TRANSACTION_COUNT):
            dirty_field = random.choice(
                ["transaction_id", "account_id", "customer_id", "merchant_id", "amount", "transaction_timestamp"]
            )
            if dirty_field == "transaction_id":
                row["transaction_id"] = None
            elif dirty_field == "account_id":
                row["account_id"] = None
            elif dirty_field == "customer_id":
                row["customer_id"] = None
            elif dirty_field == "merchant_id":
                row["merchant_id"] = None
            elif dirty_field == "amount":
                row["amount"] = random.choice(BAD_AMOUNT_VALUES)
            else:
                row["transaction_timestamp"] = random.choice(BAD_DATE_VALUES)

        rows.append(row)
    return rows


def valid_ids(rows: list[dict[str, Any]], id_column: str) -> list[str]:
    return [row[id_column] for row in rows if row.get(id_column)]


def main() -> None:
    client = s3_client()
    client.head_bucket(Bucket=S3_BUCKET)
    delete_prefix(client, "bronze/")

    customers = generate_customers()
    customer_ids = valid_ids(customers, "customer_id")

    accounts = generate_accounts(customer_ids)
    valid_accounts = [row for row in accounts if row.get("account_id") and row.get("customer_id")]
    account_ids = [row["account_id"] for row in valid_accounts]
    account_to_customer = {row["account_id"]: row["customer_id"] for row in valid_accounts}

    merchants = generate_merchants()
    merchant_ids = valid_ids(merchants, "merchant_id")

    transactions = generate_transactions(account_ids, account_to_customer, merchant_ids)

    tables = {
        "customers": customers,
        "accounts": accounts,
        "merchants": merchants,
        "transactions": transactions,
    }
    for table_name, rows in tables.items():
        write_jsonl(client, TABLE_KEYS[table_name], rows)
        print(f"Wrote {len(rows):,} rows to s3://{S3_BUCKET}/{TABLE_KEYS[table_name]}")

    print("Bronze data generation completed.")


if __name__ == "__main__":
    main()

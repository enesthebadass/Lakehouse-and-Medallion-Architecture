"""Idempotently register Spark-created Delta tables in the Trino metastore."""

from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from base64 import b64encode
from dataclasses import dataclass
from typing import Any

TRINO_URL = os.getenv("TRINO_URL", "http://trino:8080").rstrip("/")
TRINO_USER = os.getenv("TRINO_USER", "lakehouse-admin")
TRINO_PASSWORD = os.getenv("TRINO_PASSWORD", "")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "lakehouse")
TRINO_STARTUP_TIMEOUT_SECONDS = int(os.getenv("TRINO_STARTUP_TIMEOUT_SECONDS", "120"))


@dataclass(frozen=True)
class TableRegistration:
    schema_name: str
    table_name: str
    table_location: str
    required: bool = True


def registrations() -> tuple[TableRegistration, ...]:
    raw_vault_tables = (
        "hub_customer",
        "hub_account",
        "hub_merchant",
        "hub_transaction",
        "link_customer_account",
        "link_transaction_context",
        "sat_customer_profile",
        "sat_account_details",
        "sat_merchant_details",
        "sat_transaction_details",
    )
    cdc_raw_vault_tables = (
        "hub_customer",
        "hub_loan_application",
        "hub_loan",
        "hub_product",
        "hub_branch",
        "hub_currency",
        "link_application_context",
        "link_loan_context",
        "sat_customer_details",
        "sat_loan_application_details",
        "sat_loan_details",
        "sat_product_details",
        "sat_branch_details",
        "sat_currency_details",
        "sat_source_record_status",
    )
    gold_tables = (
        "dim_customer",
        "dim_account",
        "dim_merchant",
        "fact_transactions",
    )

    result = [
        TableRegistration(
            "cdc_raw_vault",
            table_name,
            f"s3a://lakehouse/silver/cdc_raw_vault/{table_name}",
        )
        for table_name in cdc_raw_vault_tables
    ]
    result.append(
        TableRegistration(
            "quarantine",
            "cdc_raw_vault_events",
            "s3a://lakehouse/silver/quarantine/cdc_raw_vault_events",
        )
    )
    result.append(
        TableRegistration(
            "audit",
            "cdc_raw_vault_reconciliation",
            "s3a://lakehouse/silver/audit/cdc_raw_vault_reconciliation",
        )
    )
    result.extend(
        TableRegistration(
            "raw_vault",
            table_name,
            f"s3a://lakehouse/silver/raw_vault/{table_name}",
            required=False,
        )
        for table_name in raw_vault_tables
    )
    result.extend(
        TableRegistration(
            "gold",
            table_name,
            f"s3a://lakehouse/gold/{table_name}",
            required=False,
        )
        for table_name in gold_tables
    )
    return tuple(result)


SCHEMAS = (
    "cdc_raw_vault",
    "quarantine",
    "audit",
    "raw_vault",
    "gold",
    "gold_dbt",
)

SCHEMA_LOCATIONS = {"gold_dbt": "s3a://lakehouse/gold/dbt"}


class TrinoQueryError(RuntimeError):
    pass


def trino_headers() -> dict[str, str]:
    headers = {"X-Trino-User": TRINO_USER}
    if TRINO_PASSWORD:
        credentials = b64encode(f"{TRINO_USER}:{TRINO_PASSWORD}".encode()).decode()
        headers["Authorization"] = f"Basic {credentials}"
        headers["X-Forwarded-Proto"] = "https"
    return headers


def request_json(request: urllib.request.Request) -> dict[str, Any]:
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise TrinoQueryError(f"Trino HTTP {exc.code}: {detail}") from exc


def execute(sql: str) -> list[list[Any]]:
    request = urllib.request.Request(
        f"{TRINO_URL}/v1/statement",
        data=sql.encode("utf-8"),
        headers={
            "Content-Type": "text/plain; charset=utf-8",
            **trino_headers(),
        },
        method="POST",
    )
    page = request_json(request)
    rows: list[list[Any]] = []
    while True:
        if page.get("error"):
            error = page["error"]
            raise TrinoQueryError(
                f"{error.get('errorName', 'TRINO_ERROR')}: {error.get('message')}"
            )
        rows.extend(page.get("data", []))
        next_uri = page.get("nextUri")
        if not next_uri:
            return rows
        if TRINO_URL.startswith("http://") and next_uri.startswith("https://"):
            next_uri = "http://" + next_uri.removeprefix("https://")
        page = request_json(
            urllib.request.Request(
                next_uri,
                headers=trino_headers(),
                method="GET",
            )
        )


def wait_for_trino() -> None:
    deadline = time.monotonic() + TRINO_STARTUP_TIMEOUT_SECONDS
    while True:
        try:
            execute("SELECT 1")
            return
        except (OSError, TrinoQueryError) as exc:
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Trino did not become ready within "
                    f"{TRINO_STARTUP_TIMEOUT_SECONDS}s: {exc}"
                ) from exc
            time.sleep(2)


def create_schemas() -> None:
    for schema_name in SCHEMAS:
        sql = f"CREATE SCHEMA IF NOT EXISTS {TRINO_CATALOG}.{schema_name}"
        if schema_name in SCHEMA_LOCATIONS:
            sql += f" WITH (location = '{SCHEMA_LOCATIONS[schema_name]}')"
        execute(sql)
        print(f"schema ready: {TRINO_CATALOG}.{schema_name}")


def registered_tables(schema_name: str) -> set[str]:
    return {
        row[0] for row in execute(f"SHOW TABLES FROM {TRINO_CATALOG}.{schema_name}")
    }


def register_tables() -> None:
    by_schema = {schema_name: registered_tables(schema_name) for schema_name in SCHEMAS}
    registered = 0
    existing = 0
    skipped = 0
    for table in registrations():
        qualified_name = f"{TRINO_CATALOG}.{table.schema_name}.{table.table_name}"
        if table.table_name in by_schema[table.schema_name]:
            existing += 1
            print(f"already registered: {qualified_name}")
            continue
        try:
            execute(
                f"CALL {TRINO_CATALOG}.system.register_table("
                f"schema_name => '{table.schema_name}', "
                f"table_name => '{table.table_name}', "
                f"table_location => '{table.table_location}')"
            )
        except TrinoQueryError as exc:
            if table.required:
                raise RuntimeError(
                    f"Required Delta table registration failed for {qualified_name}: {exc}"
                ) from exc
            skipped += 1
            print(f"optional table unavailable: {qualified_name} ({exc})")
            continue
        registered += 1
        by_schema[table.schema_name].add(table.table_name)
        print(f"registered: {qualified_name} -> {table.table_location}")
    print(
        f"registration complete: registered={registered}, "
        f"existing={existing}, optional_skipped={skipped}"
    )


def main() -> None:
    wait_for_trino()
    create_schemas()
    register_tables()


if __name__ == "__main__":
    main()

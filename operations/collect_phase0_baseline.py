#!/usr/bin/env python3
"""Collect a reproducible correctness, runtime, and storage baseline for the local PoC."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import re
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "quality/correctness-invariants.yaml"
WINDOWS_DOCKER_DESKTOP_BIN = Path(
    "/mnt/c/Program Files/Docker/Docker/resources/bin/docker.exe"
)

SOURCE_TABLES = (
    "prm.currencies",
    "prm.branches",
    "prm.products",
    "prm.status_codes",
    "prm.rate_parameters",
    "mms.customers",
    "mms.customer_addresses",
    "mms.customer_contacts",
    "mms.customer_relations",
    "krd.loan_applications",
    "krd.loans",
    "krd.installments",
    "krd.collaterals",
)

RAW_VAULT_TABLES = (
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
    "sat_entity_record_status",
    "sat_application_context_effectivity",
    "sat_loan_context_effectivity",
)

EVENT_ID_TABLES = (
    "sat_customer_details",
    "sat_loan_application_details",
    "sat_loan_details",
    "sat_product_details",
    "sat_branch_details",
    "sat_currency_details",
    "sat_source_record_status",
    "sat_entity_record_status",
    "sat_application_context_effectivity",
    "sat_loan_context_effectivity",
)

GOLD_GRAINS = {
    "dim_customer_current": ("customer_hk",),
    "dim_product_current": ("product_hk",),
    "dim_branch_current": ("branch_hk",),
    "dim_currency_current": ("currency_hk",),
    "fct_loan_applications_current": ("loan_application_hk",),
    "fct_loans_current": ("loan_hk",),
    "agg_customer_loan_portfolio": ("customer_hk", "currency_hk"),
    "gold_row_lineage": (
        "gold_model",
        "gold_business_key",
        "raw_vault_object",
        "source_event_id",
    ),
}

CURRENT_RAW_VAULT_TASKS = (
    "validate_bronze_and_quarantine",
    "load_incremental_hubs_links",
    "load_satellite_and_delete_history",
    "reconcile_source_bronze_silver",
)

STORAGE_PREFIXES = {
    "bronze_cdc": "bronze/cdc",
    "raw_vault": "silver/cdc_raw_vault",
    "quarantine": "silver/quarantine",
    "audit": "silver/audit",
    "gold_dbt": "gold/dbt",
}

REQUIRED_SERVICES = {
    "core-banking-source",
    "pipeline-control-postgres",
    "kafka",
    "debezium-connect",
    "bronze-cdc-writer",
    "minio",
    "spark-master",
    "spark-worker",
    "airflow-webserver",
    "airflow-scheduler",
    "hive-metastore",
    "trino",
}

ANSI_ESCAPE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class BaselineError(RuntimeError):
    """Raised when a baseline dependency or mandatory invariant fails."""


@dataclass(frozen=True)
class CommandRunner:
    docker_bin: str

    def run(
        self,
        args: Sequence[str],
        *,
        check: bool = True,
        input_text: str | None = None,
    ) -> str:
        completed = subprocess.run(
            list(args),
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
            input=input_text,
        )
        if check and completed.returncode != 0:
            command = " ".join(shlex.quote(part) for part in args)
            detail = (completed.stderr or completed.stdout).strip()
            raise BaselineError(f"Command failed ({completed.returncode}): {command}\n{detail}")
        return completed.stdout

    def compose(self, *args: str, input_text: str | None = None) -> str:
        return self.run(
            (self.docker_bin, "compose", *args),
            input_text=input_text,
        )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def discover_docker_bin(explicit: str | None = None) -> str:
    configured = explicit or os.getenv("DOCKER_BIN")
    if configured:
        return configured

    path_candidate = shutil.which("docker")
    if path_candidate and Path(path_candidate).is_file():
        return path_candidate
    if WINDOWS_DOCKER_DESKTOP_BIN.is_file():
        return str(WINDOWS_DOCKER_DESKTOP_BIN)

    raise BaselineError(
        "Docker CLI was not found. Start Docker Desktop, enable WSL integration, "
        "or pass --docker-bin/DOCKER_BIN."
    )


def clean_output(value: str) -> str:
    return ANSI_ESCAPE.sub("", value).replace("\r", "").strip()


def parse_tsv(value: str) -> list[list[str]]:
    cleaned = clean_output(value)
    if not cleaned:
        return []
    return [
        [field.strip() for field in row]
        for row in csv.reader(cleaned.splitlines(), delimiter="\t")
    ]


def parse_compose_ps(value: str) -> dict[str, dict[str, Any]]:
    cleaned = clean_output(value)
    if not cleaned:
        return {}
    documents: list[dict[str, Any]] = []
    if cleaned.startswith("["):
        documents = json.loads(cleaned)
    else:
        documents = [json.loads(line) for line in cleaned.splitlines() if line.strip()]
    return {document["Service"]: document for document in documents}


def optional_int(value: str) -> int | None:
    return int(value) if value.lstrip("-").isdigit() else None


def parse_kafka_group(value: str) -> list[dict[str, Any]]:
    partitions: list[dict[str, Any]] = []
    for line in clean_output(value).splitlines():
        fields = line.split()
        if len(fields) < 6 or not fields[2].lstrip("-").isdigit():
            continue
        partitions.append(
            {
                "group": fields[0],
                "topic": fields[1],
                "partition": int(fields[2]),
                "current_offset": optional_int(fields[3]),
                "log_end_offset": optional_int(fields[4]),
                "lag": optional_int(fields[5]),
            }
        )
    return sorted(partitions, key=lambda item: (item["topic"], item["partition"]))


def parse_mc_inventory(value: str) -> list[dict[str, Any]]:
    objects: list[dict[str, Any]] = []
    for line in clean_output(value).splitlines():
        try:
            document = json.loads(line)
        except json.JSONDecodeError:
            continue
        if document.get("status") == "error" or document.get("type") in {"folder", "directory"}:
            continue
        key = document.get("key") or document.get("name")
        size = document.get("size")
        if key is None or not isinstance(size, int):
            continue
        objects.append({"key": str(key), "size": size})
    return sorted(objects, key=lambda item: item["key"])


def percentile(sorted_values: Sequence[int], percent: float) -> int:
    if not sorted_values:
        return 0
    index = max(0, min(len(sorted_values) - 1, math.ceil(len(sorted_values) * percent) - 1))
    return sorted_values[index]


def summarize_inventory(objects: Iterable[dict[str, Any]]) -> dict[str, Any]:
    materialized = list(objects)
    sizes = sorted(int(item["size"]) for item in materialized)
    fingerprint_input = "".join(
        f"{item['key']}\t{item['size']}\n" for item in materialized
    ).encode("utf-8")
    return {
        "object_count": len(materialized),
        "total_bytes": sum(sizes),
        "min_bytes": sizes[0] if sizes else 0,
        "median_bytes": percentile(sizes, 0.50),
        "p95_bytes": percentile(sizes, 0.95),
        "max_bytes": sizes[-1] if sizes else 0,
        "objects_under_1_mib": sum(size < 1024 * 1024 for size in sizes),
        "inventory_sha256": hashlib.sha256(fingerprint_input).hexdigest(),
    }


def quote_identifier(value: str) -> str:
    if not SAFE_IDENTIFIER.fullmatch(value):
        raise BaselineError(f"Unsafe SQL identifier returned by catalog: {value!r}")
    return f'"{value}"'


def canonical_column_sql(column_name: str, column_type: str) -> str:
    identifier = quote_identifier(column_name)
    if column_type.lower().startswith("varbinary"):
        value = f"to_hex({identifier})"
    else:
        value = f"CAST({identifier} AS VARCHAR)"
    return (
        f"coalesce(concat(CAST(length({value}) AS VARCHAR), ':', {value}), '-1:')"
    )


def table_fingerprint_sql(
    qualified_table: str, columns: Sequence[tuple[str, str]]
) -> str:
    if not columns:
        raise BaselineError(f"Cannot fingerprint table without columns: {qualified_table}")
    return "SELECT " + table_fingerprint_metrics_sql(columns) + f" FROM {qualified_table}"


def table_fingerprint_metrics_sql(columns: Sequence[tuple[str, str]]) -> str:
    if not columns:
        raise BaselineError("Cannot fingerprint a table without columns")
    row_expression = ", ".join(
        canonical_column_sql(column_name, column_type)
        for column_name, column_type in columns
    )
    return (
        "count(*), "
        "coalesce(to_hex(checksum(to_utf8(concat("
        f"{row_expression})))), '')"
    )


def trino(runner: CommandRunner, sql: str) -> list[list[str]]:
    output = runner.compose(
        "exec",
        "-T",
        "trino",
        "trino",
        "--output-format",
        "TSV",
        "--file",
        "/dev/stdin",
        input_text=sql.rstrip(";\n") + ";\n",
    )
    return parse_tsv(output)


def collect_table_fingerprints(
    runner: CommandRunner,
    schema: str,
    tables: Iterable[str],
    *,
    excluded_columns: frozenset[str] = frozenset(),
) -> dict[str, dict[str, Any]]:
    table_names = tuple(tables)
    table_literals = ", ".join(f"'{table}'" for table in table_names)
    catalog_rows = trino(
        runner,
        "SELECT table_name, column_name, data_type, ordinal_position "
        "FROM lakehouse.information_schema.columns "
        f"WHERE table_schema = '{schema}' AND table_name IN ({table_literals}) "
        "ORDER BY table_name, ordinal_position",
    )
    columns_by_table: dict[str, list[tuple[str, str]]] = {
        table: [] for table in table_names
    }
    for row in catalog_rows:
        if len(row) >= 3 and row[1] not in excluded_columns:
            columns_by_table[row[0]].append((row[1], row[2]))

    statements = []
    for table in table_names:
        columns = columns_by_table[table]
        if not columns:
            raise BaselineError(f"No catalog columns found for lakehouse.{schema}.{table}")
        statements.append(
            f"SELECT '{table}', {table_fingerprint_metrics_sql(columns)} "
            f"FROM lakehouse.{schema}.{table}"
        )
    fingerprint_rows = trino(runner, " UNION ALL ".join(statements) + " ORDER BY 1")
    values_by_table = {row[0]: row[1:] for row in fingerprint_rows}

    result: dict[str, dict[str, Any]] = {}
    for table in table_names:
        values = values_by_table.get(table)
        if not values or len(values) < 2:
            raise BaselineError(
                f"Unexpected fingerprint result for lakehouse.{schema}.{table}: {values}"
            )
        columns = columns_by_table[table]
        result[table] = {
            "row_count": int(values[0]),
            "content_checksum": values[1],
            "schema_sha256": hashlib.sha256(
                "".join(f"{name}\t{data_type}\n" for name, data_type in columns).encode(
                    "utf-8"
                )
            ).hexdigest(),
            "fingerprinted_columns": [
                {"name": name, "type": data_type} for name, data_type in columns
            ],
        }
    return result


def source_count_query() -> str:
    statements = []
    for qualified in SOURCE_TABLES:
        schema, table = qualified.split(".", 1)
        statements.append(
            f"SELECT '{schema}', '{table}', count(*) FROM {schema}.{table}"
        )
    return " UNION ALL ".join(statements) + " ORDER BY 1, 2"


def collect_source_counts(runner: CommandRunner) -> dict[str, int]:
    output = runner.compose(
        "exec",
        "-T",
        "core-banking-source",
        "psql",
        "-U",
        os.getenv("CORE_BANKING_DB_USER", "core_banking"),
        "-d",
        os.getenv("CORE_BANKING_DB_NAME", "core_banking"),
        "-At",
        "-F",
        "\t",
        "-c",
        source_count_query(),
    )
    return {f"{row[0]}.{row[1]}": int(row[2]) for row in parse_tsv(output)}


def collect_services(runner: CommandRunner) -> dict[str, dict[str, Any]]:
    documents = parse_compose_ps(runner.compose("ps", "--format", "json"))
    return {
        service: {
            "state": documents.get(service, {}).get("State", "missing"),
            "health": documents.get(service, {}).get("Health", ""),
            "status": documents.get(service, {}).get("Status", ""),
        }
        for service in sorted(REQUIRED_SERVICES)
    }


def collect_kafka(runner: CommandRunner, consumer_group: str) -> dict[str, Any]:
    output = runner.compose(
        "exec",
        "-T",
        "kafka",
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server",
        "kafka:9092",
        "--group",
        consumer_group,
        "--describe",
    )
    partitions = parse_kafka_group(output)
    return {
        "consumer_group": consumer_group,
        "partition_count": len(partitions),
        "total_lag": sum(item["lag"] or 0 for item in partitions),
        "partitions": partitions,
    }


def collect_storage(runner: CommandRunner) -> dict[str, dict[str, Any]]:
    result = {}
    for name, prefix in STORAGE_PREFIXES.items():
        command = (
            "mc alias set phase0 http://localhost:9000 minioadmin minioadmin >/dev/null "
            f"&& mc ls --recursive --json phase0/lakehouse/{shlex.quote(prefix)}"
        )
        output = runner.compose("exec", "-T", "minio", "sh", "-c", command)
        result[name] = {"prefix": prefix, **summarize_inventory(parse_mc_inventory(output))}
    return result


def collect_airflow_durations(runner: CommandRunner) -> list[dict[str, Any]]:
    task_literals = ", ".join(f"'{task_id}'" for task_id in CURRENT_RAW_VAULT_TASKS)
    query = """
        WITH latest_success AS (
            SELECT run_id
            FROM dag_run
            WHERE dag_id = 'cdc_raw_vault_incremental' AND state = 'success'
            ORDER BY end_date DESC NULLS LAST
            LIMIT 1
        )
        SELECT task_id, run_id, duration, start_date, end_date
        FROM task_instance
        WHERE dag_id = 'cdc_raw_vault_incremental'
          AND run_id = (SELECT run_id FROM latest_success)
          AND task_id IN (TASK_LITERALS)
        ORDER BY task_id
    """.replace("TASK_LITERALS", task_literals)
    output = runner.compose(
        "exec",
        "-T",
        "postgres",
        "psql",
        "-U",
        "airflow",
        "-d",
        "airflow",
        "-At",
        "-F",
        "\t",
        "-c",
        query,
    )
    return [
        {
            "task_id": row[0],
            "run_id": row[1],
            "duration_seconds": float(row[2]) if row[2] else None,
            "started_at": row[3],
            "finished_at": row[4],
        }
        for row in parse_tsv(output)
    ]


def collect_pipeline_control(runner: CommandRunner) -> dict[str, Any]:
    batch_query = """
        SELECT b.batch_id, b.state, b.manifest_uri, b.manifest_sha256,
               (SELECT count(*) FROM pipeline_batch_partition p WHERE p.batch_id = b.batch_id),
               (SELECT coalesce(sum(interval_object_count), 0)
                  FROM pipeline_batch_partition p WHERE p.batch_id = b.batch_id),
               (SELECT coalesce(sum(interval_input_bytes), 0)
                  FROM pipeline_batch_partition p WHERE p.batch_id = b.batch_id),
               b.source_position_type, b.source_boundary_high,
               b.source_expected_event_count, b.source_observed_event_count,
               b.source_untracked_event_count,
               (SELECT count(*) FROM pipeline_batch_source_transaction t
                 WHERE t.batch_id = b.batch_id),
               (SELECT count(*) FROM pipeline_batch_source_transaction t
                 WHERE t.batch_id = b.batch_id AND t.status = 'FAIL')
        FROM pipeline_batch b
        WHERE b.state = 'PUBLISHED'
        ORDER BY b.published_at DESC
        LIMIT 1
    """
    batch_rows = parse_tsv(
        runner.compose(
            "exec",
            "-T",
            "pipeline-control-postgres",
            "psql",
            "-U",
            "pipeline_control",
            "-d",
            "pipeline_control",
            "-At",
            "-F",
            "\t",
            "-c",
            batch_query,
        )
    )
    if not batch_rows:
        return {"latest_published_batch": None, "latest_attempt": None}

    batch = batch_rows[0]
    attempt_query = """
        WITH latest_batch AS (
            SELECT batch_id
            FROM pipeline_batch
            WHERE state = 'PUBLISHED'
            ORDER BY published_at DESC
            LIMIT 1
        ), latest_attempt AS (
            SELECT a.*
            FROM pipeline_attempt a
            WHERE a.batch_id = (SELECT batch_id FROM latest_batch)
            ORDER BY attempt_number DESC
            LIMIT 1
        )
        SELECT a.attempt_number, a.airflow_run_id, a.state, a.manifest_sha256,
               count(e.task_id), count(DISTINCT e.manifest_sha256),
               count(*) FILTER (WHERE e.state <> 'SUCCESS'),
               count(DISTINCT e.reader_mode), min(e.reader_mode),
               min(e.selected_input_object_count), max(e.selected_input_object_count),
               min(e.selected_input_bytes), max(e.selected_input_bytes),
               (SELECT audit.state FROM pipeline_attempt_audit audit
                 WHERE audit.batch_id = a.batch_id
                   AND audit.attempt_number = a.attempt_number),
               (SELECT audit.rule_count FROM pipeline_attempt_audit audit
                 WHERE audit.batch_id = a.batch_id
                   AND audit.attempt_number = a.attempt_number),
               (SELECT audit.failed_rule_count FROM pipeline_attempt_audit audit
                 WHERE audit.batch_id = a.batch_id
                   AND audit.attempt_number = a.attempt_number),
               (SELECT audit.evidence_uri FROM pipeline_attempt_audit audit
                 WHERE audit.batch_id = a.batch_id
                   AND audit.attempt_number = a.attempt_number),
               (SELECT audit.evidence_sha256 FROM pipeline_attempt_audit audit
                 WHERE audit.batch_id = a.batch_id
                   AND audit.attempt_number = a.attempt_number),
               (SELECT count(*) FROM pipeline_audit_rule_result rule
                 WHERE rule.batch_id = a.batch_id
                   AND rule.attempt_number = a.attempt_number),
               (SELECT count(*) FROM pipeline_audit_rule_result rule
                 WHERE rule.batch_id = a.batch_id
                   AND rule.attempt_number = a.attempt_number
                   AND rule.status = 'FAIL')
        FROM latest_attempt a
        LEFT JOIN pipeline_task_evidence e
          ON e.batch_id = a.batch_id AND e.attempt_number = a.attempt_number
        GROUP BY a.batch_id, a.attempt_number, a.airflow_run_id, a.state,
                 a.manifest_sha256
    """
    attempt_rows = parse_tsv(
        runner.compose(
            "exec",
            "-T",
            "pipeline-control-postgres",
            "psql",
            "-U",
            "pipeline_control",
            "-d",
            "pipeline_control",
            "-At",
            "-F",
            "\t",
            "-c",
            attempt_query,
        )
    )
    attempt = attempt_rows[0] if attempt_rows else None
    return {
        "latest_published_batch": {
            "batch_id": batch[0],
            "state": batch[1],
            "manifest_uri": batch[2],
            "manifest_sha256": batch[3],
            "partition_count": int(batch[4]),
            "interval_object_count": int(batch[5]),
            "interval_input_bytes": int(batch[6]),
            "source_position_type": batch[7],
            "source_boundary_high": int(batch[8]) if batch[8] else None,
            "source_expected_event_count": int(batch[9]),
            "source_observed_event_count": int(batch[10]),
            "source_untracked_event_count": int(batch[11]),
            "source_transaction_count": int(batch[12]),
            "failed_source_transaction_count": int(batch[13]),
        },
        "latest_attempt": (
            {
                "attempt_number": int(attempt[0]),
                "airflow_run_id": attempt[1],
                "state": attempt[2],
                "manifest_sha256": attempt[3],
                "task_evidence_count": int(attempt[4]),
                "distinct_task_manifest_count": int(attempt[5]),
                "failed_task_evidence_count": int(attempt[6]),
                "distinct_reader_mode_count": int(attempt[7]),
                "reader_mode": attempt[8],
                "min_selected_input_object_count": int(attempt[9]),
                "max_selected_input_object_count": int(attempt[10]),
                "min_selected_input_bytes": int(attempt[11]),
                "max_selected_input_bytes": int(attempt[12]),
                "audit_state": attempt[13],
                "audit_rule_count": int(attempt[14]) if attempt[14] else 0,
                "audit_failed_rule_count": int(attempt[15]) if attempt[15] else 0,
                "audit_evidence_uri": attempt[16],
                "audit_evidence_sha256": attempt[17],
                "persisted_audit_rule_count": int(attempt[18]),
                "persisted_failed_audit_rule_count": int(attempt[19]),
            }
            if attempt
            else None
        ),
    }


def collect_event_identity(runner: CommandRunner) -> dict[str, dict[str, int]]:
    statements = [
        (
            f"SELECT '{table}', count(*), count(DISTINCT source_event_id) "
            f"FROM lakehouse.cdc_raw_vault.{table}"
        )
        for table in EVENT_ID_TABLES
    ]
    rows = trino(runner, " UNION ALL ".join(statements) + " ORDER BY 1")
    return {
        row[0]: {"row_count": int(row[1]), "distinct_source_event_ids": int(row[2])}
        for row in rows
    }


def collect_gold_grains(runner: CommandRunner) -> dict[str, dict[str, Any]]:
    result = {}
    for table, keys in GOLD_GRAINS.items():
        row_value = ", ".join(quote_identifier(key) for key in keys)
        distinct_expression = (
            quote_identifier(keys[0])
            if len(keys) == 1
            else f"ROW({row_value})"
        )
        rows = trino(
            runner,
            f"SELECT count(*), count(DISTINCT {distinct_expression}) "
            f"FROM lakehouse.gold_dbt.{table}",
        )
        row_count, distinct_count = int(rows[0][0]), int(rows[0][1])
        result[table] = {
            "grain_columns": list(keys),
            "row_count": row_count,
            "distinct_grain_count": distinct_count,
            "valid": row_count == distinct_count,
        }
    return result


def collect_reconciliation(runner: CommandRunner) -> dict[str, Any]:
    rows = trino(
        runner,
        """
        WITH latest_batch AS (
            SELECT load_batch_id
            FROM lakehouse.audit.cdc_raw_vault_reconciliation
            ORDER BY checked_at DESC
            LIMIT 1
        )
        SELECT load_batch_id, count(*), count_if(NOT passed), max(checked_at)
        FROM lakehouse.audit.cdc_raw_vault_reconciliation
        WHERE load_batch_id = (SELECT load_batch_id FROM latest_batch)
        GROUP BY load_batch_id
        """,
    )
    if len(rows) != 1:
        raise BaselineError(f"No unique latest reconciliation batch found: {rows}")
    return {
        "load_batch_id": rows[0][0],
        "check_count": int(rows[0][1]),
        "failure_count": int(rows[0][2]),
        "checked_at": rows[0][3],
    }


def build_checks(report: dict[str, Any]) -> list[dict[str, Any]]:
    control_batch = report["pipeline_control"]["latest_published_batch"]
    control_attempt = report["pipeline_control"]["latest_attempt"]
    checks = {
        "required_services_are_running": all(
            value["state"] == "running" and value["health"] != "unhealthy"
            for value in report["services"].values()
        ),
        "source_tables_are_non_empty": all(
            count > 0 for count in report["source"]["table_counts"].values()
        ),
        "kafka_lag_is_zero": report["kafka"]["total_lag"] == 0,
        "bronze_inventory_non_empty": report["storage"]["bronze_cdc"]["object_count"] > 0,
        "storage_inventory_is_recorded": all(
            value["object_count"] > 0 for value in report["storage"].values()
        ),
        "latest_reconciliation_passes": (
            report["reconciliation"]["check_count"] > 0
            and report["reconciliation"]["failure_count"] == 0
        ),
        "raw_vault_event_identity_is_unique": all(
            value["row_count"] == value["distinct_source_event_ids"]
            for value in report["raw_vault_event_identity"].values()
        ),
        "gold_current_grains_are_unique": all(
            value["valid"] for value in report["gold_grains"].values()
        ),
        "pipeline_control_latest_attempt_is_published": (
            control_batch is not None
            and control_attempt is not None
            and control_batch["state"] == "PUBLISHED"
            and control_batch["partition_count"] > 0
            and control_attempt["state"] == "SUCCESS"
            and control_attempt["task_evidence_count"] == len(CURRENT_RAW_VAULT_TASKS)
            and control_attempt["distinct_task_manifest_count"] == 1
            and control_attempt["failed_task_evidence_count"] == 0
            and control_attempt["manifest_sha256"] == control_batch["manifest_sha256"]
        ),
        "pipeline_control_bounded_input_evidence_is_consistent": (
            control_batch is not None
            and control_attempt is not None
            and control_attempt.get("reader_mode") == "bounded_object_list"
            and control_attempt.get("distinct_reader_mode_count") == 1
            and control_attempt.get("min_selected_input_object_count")
            == control_attempt.get("max_selected_input_object_count")
            == control_batch.get("interval_object_count")
            and control_attempt.get("min_selected_input_bytes")
            == control_attempt.get("max_selected_input_bytes")
            == control_batch.get("interval_input_bytes")
        ),
        "pipeline_control_point_in_time_audit_passes": (
            control_batch is not None
            and control_attempt is not None
            and control_batch.get("source_position_type") == "postgres_lsn"
            and control_batch.get("source_expected_event_count")
            == control_batch.get("source_observed_event_count")
            and control_batch.get("source_untracked_event_count") == 0
            and control_batch.get("failed_source_transaction_count") == 0
            and control_attempt.get("audit_state") == "PASS"
            and control_attempt.get("audit_rule_count", 0) > 0
            and control_attempt.get("audit_failed_rule_count") == 0
            and control_attempt.get("persisted_audit_rule_count")
            == control_attempt.get("audit_rule_count")
            and control_attempt.get("persisted_failed_audit_rule_count") == 0
            and bool(control_attempt.get("audit_evidence_uri"))
            and len(control_attempt.get("audit_evidence_sha256") or "") == 64
        ),
    }
    return [
        {"id": check_id, "passed": passed}
        for check_id, passed in sorted(checks.items())
    ]


def stable_projection(report: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_table_counts": report["source"]["table_counts"],
        "bronze_inventory": {
            key: report["storage"]["bronze_cdc"][key]
            for key in ("object_count", "total_bytes", "inventory_sha256")
        },
        "raw_vault_tables": report["tables"]["raw_vault"],
        "gold_tables": report["tables"]["gold"],
        "raw_vault_event_identity": report["raw_vault_event_identity"],
        "gold_grains": report["gold_grains"],
    }


def compare_reports(previous: dict[str, Any], current: dict[str, Any]) -> dict[str, Any]:
    previous_projection = stable_projection(previous)
    current_projection = stable_projection(current)
    differences = []

    def visit(path: str, left: Any, right: Any) -> None:
        if isinstance(left, dict) and isinstance(right, dict):
            for key in sorted(set(left) | set(right)):
                visit(f"{path}.{key}" if path else key, left.get(key), right.get(key))
        elif left != right:
            differences.append({"path": path, "before": left, "after": right})

    visit("", previous_projection, current_projection)
    return {"matches": not differences, "differences": differences}


def git_metadata(runner: CommandRunner) -> dict[str, Any]:
    revision = clean_output(runner.run(("git", "rev-parse", "HEAD")))
    status = clean_output(runner.run(("git", "status", "--porcelain")))
    return {"revision": revision, "dirty": bool(status)}


def collect_report(runner: CommandRunner, consumer_group: str) -> dict[str, Any]:
    contract_bytes = CONTRACT_PATH.read_bytes()
    report: dict[str, Any] = {
        "schema_version": 1,
        "collected_at": utc_now(),
        "contract": {
            "path": str(CONTRACT_PATH.relative_to(ROOT)),
            "sha256": hashlib.sha256(contract_bytes).hexdigest(),
        },
        "git": git_metadata(runner),
    }
    report["services"] = collect_services(runner)
    source_counts = collect_source_counts(runner)
    report["source"] = {
        "table_counts": source_counts,
        "total_rows": sum(source_counts.values()),
    }
    report["kafka"] = collect_kafka(runner, consumer_group)
    report["storage"] = collect_storage(runner)
    report["pipeline_control"] = collect_pipeline_control(runner)
    latest_tasks = collect_airflow_durations(runner)
    report["airflow"] = {"latest_successful_tasks": latest_tasks}
    bronze_inventory = report["storage"]["bronze_cdc"]
    report["processing_cost_proxy"] = {
        "measurement": "current_recursive_bronze_reader_logical_input",
        "raw_vault_phase_count": len(CURRENT_RAW_VAULT_TASKS),
        "bronze_candidate_files_per_phase": bronze_inventory["object_count"],
        "bronze_candidate_bytes_per_phase": bronze_inventory["total_bytes"],
        "estimated_files_across_phases": (
            len(CURRENT_RAW_VAULT_TASKS) * bronze_inventory["object_count"]
        ),
        "estimated_bytes_across_phases": (
            len(CURRENT_RAW_VAULT_TASKS) * bronze_inventory["total_bytes"]
        ),
        "latest_dag_duration_seconds": sum(
            task["duration_seconds"] or 0 for task in latest_tasks
        ),
    }
    report["tables"] = {
        "raw_vault": collect_table_fingerprints(
            runner, "cdc_raw_vault", RAW_VAULT_TABLES
        ),
        "gold": collect_table_fingerprints(
            runner,
            "gold_dbt",
            GOLD_GRAINS,
            excluded_columns=frozenset({"dbt_loaded_at"}),
        ),
    }
    report["raw_vault_event_identity"] = collect_event_identity(runner)
    report["gold_grains"] = collect_gold_grains(runner)
    report["reconciliation"] = collect_reconciliation(runner)
    report["checks"] = build_checks(report)
    report["status"] = (
        "PASS" if all(check["passed"] for check in report["checks"]) else "FAIL"
    )
    return report


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker-bin", help="Docker CLI path; defaults to DOCKER_BIN/PATH")
    parser.add_argument(
        "--consumer-group",
        default=os.getenv("BRONZE_CDC_CONSUMER_GROUP", "bronze-cdc-writer-v1"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / "tests/results/phase0-baseline.json",
    )
    parser.add_argument(
        "--compare-to",
        type=Path,
        help="Compare stable data fingerprints with a previous baseline JSON",
    )
    parser.add_argument(
        "--allow-failed-checks",
        action="store_true",
        help="Write evidence but return success even if a mandatory check fails",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        runner = CommandRunner(discover_docker_bin(args.docker_bin))
        report = collect_report(runner, args.consumer_group)
        if args.compare_to:
            previous = json.loads(args.compare_to.read_text(encoding="utf-8"))
            report["comparison"] = compare_reports(previous, report)
            if not report["comparison"]["matches"]:
                report["status"] = "FAIL"
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        print(
            f"Phase 0 baseline {report['status']}: {args.output} "
            f"checks={len(report['checks'])}"
        )
        if report["status"] != "PASS" and not args.allow_failed_checks:
            return 1
        return 0
    except (BaselineError, OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"Phase 0 baseline failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

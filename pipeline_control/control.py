"""Persist CDC batch, attempt, task, and state history in the control database."""

from __future__ import annotations

import hashlib
import os
import re
from contextlib import closing
from typing import Any

from pipeline_control.audit import audit_evidence_uri, read_audit_evidence
from pipeline_control.manifest import (
    build_manifest_document,
    build_manifest_payload,
    discover_bronze_objects,
    read_manifest,
    s3_client,
    summarize_bronze_transactions,
    write_immutable_manifest,
)

CONTROL_DSN = os.getenv(
    "PIPELINE_CONTROL_DSN",
    "postgresql://pipeline_control:pipeline_control_local@pipeline-control-postgres/"
    "pipeline_control",
)
SOURCE_CONTROL_DSN = os.getenv(
    "SOURCE_CONTROL_DSN",
    "postgresql://core_banking:core_banking_local@core-banking-source:5432/core_banking",
)
S3_BUCKET = os.getenv("S3_BUCKET", "lakehouse")
BRONZE_PREFIX = os.getenv("BRONZE_CONTROL_SOURCE_PREFIX", "bronze/cdc/source=core_banking")
MANIFEST_PREFIX = os.getenv("PIPELINE_MANIFEST_PREFIX", "bronze/_control/manifests")
SOURCE_ADAPTER = os.getenv("CDC_SOURCE_ADAPTER", "postgres-debezium")
SCHEMA_CONTRACT_VERSION = os.getenv("CDC_SCHEMA_CONTRACT_VERSION", "cdc-envelope-v1")

EXPECTED_RAW_VAULT_TASKS = (
    "validate_bronze_and_quarantine",
    "load_incremental_hubs_links",
    "load_satellite_and_delete_history",
    "reconcile_source_bronze_silver",
)

ALLOWED_BATCH_TRANSITIONS = {
    ("CREATED", "RUNNING"),
    ("FAILED", "RUNNING"),
    ("RUNNING", "VALIDATED"),
    ("VALIDATED", "PUBLISHED"),
    ("RUNNING", "FAILED"),
    ("CREATED", "SUPERSEDED"),
    ("FAILED", "SUPERSEDED"),
}


class ControlPlaneError(RuntimeError):
    """Raised when a control-plane state transition is invalid."""


def validate_transition(from_state: str, to_state: str) -> None:
    if (from_state, to_state) not in ALLOWED_BATCH_TRANSITIONS:
        raise ControlPlaneError(f"Invalid batch transition: {from_state} -> {to_state}")


def _connect():
    import psycopg2

    return psycopg2.connect(CONTROL_DSN)


def _connect_source():
    import psycopg2

    return psycopg2.connect(SOURCE_CONTROL_DSN)


def _ledger_rows_through_observed_boundary(
    ledger_rows: list[tuple[Any, ...]], observed_txids: set[int]
) -> tuple[list[tuple[Any, ...]], int | None]:
    observed_boundaries = [
        int(row[5]) for row in ledger_rows if int(row[3]) in observed_txids
    ]
    if not observed_boundaries:
        return [], None
    boundary_high = max(observed_boundaries)
    return [row for row in ledger_rows if int(row[5]) <= boundary_high], boundary_high


def _build_source_control(
    cursor: Any,
    client: Any,
    selected_objects: list[dict[str, Any]],
) -> dict[str, Any]:
    cursor.execute(
        """
        SELECT max(t.source_boundary_position)
        FROM pipeline_batch_source_transaction t
        JOIN pipeline_batch b ON b.batch_id = t.batch_id
        WHERE b.state = 'PUBLISHED'
        """
    )
    previous_boundary = cursor.fetchone()[0]
    previous_boundary = int(previous_boundary) if previous_boundary is not None else None

    observed = summarize_bronze_transactions(client, S3_BUCKET, selected_objects)
    observed_by_txid = {
        int(item["source_txid"]): item for item in observed["transactions"]
    }
    with closing(_connect_source()) as source_connection, source_connection.cursor() as source:
        source.execute(
            """
            SELECT run_id, event_key, expected_cdc_event_count, source_txid,
                   source_boundary_lsn::TEXT,
                   pg_wal_lsn_diff(source_boundary_lsn, '0/0')::BIGINT
            FROM simulator.workload_events
            WHERE status = 'COMPLETED'
              AND expected_cdc_event_count IS NOT NULL
              AND source_txid IS NOT NULL
              AND source_boundary_lsn IS NOT NULL
              AND (%s IS NULL OR pg_wal_lsn_diff(source_boundary_lsn, '0/0') > %s)
            ORDER BY source_txid
            """,
            (previous_boundary, previous_boundary),
        )
        ledger_rows = source.fetchall()

    ledger_rows, observed_boundary_high = _ledger_rows_through_observed_boundary(
        ledger_rows, set(observed_by_txid)
    )

    transactions = []
    ledger_txids = set()
    for row in ledger_rows:
        txid = int(row[3])
        ledger_txids.add(txid)
        event_summary = observed_by_txid.get(txid, {})
        expected_count = int(row[2])
        observed_count = int(event_summary.get("observed_event_count", 0))
        transactions.append(
            {
                "source_txid": txid,
                "workload_run_id": row[0],
                "workload_event_key": row[1],
                "source_boundary_lsn": row[4],
                "source_boundary_position": int(row[5]),
                "expected_event_count": expected_count,
                "observed_event_count": observed_count,
                "event_lsn_low": event_summary.get("event_lsn_low"),
                "event_lsn_high": event_summary.get("event_lsn_high"),
                "status": "PASS" if expected_count == observed_count else "FAIL",
            }
        )

    untracked_count = int(observed["untracked_event_count"]) + sum(
        int(item["observed_event_count"])
        for txid, item in observed_by_txid.items()
        if txid not in ledger_txids
    )
    boundary_high = (
        observed_boundary_high
        if observed_boundary_high is not None
        else previous_boundary
    )
    return {
        "contract_version": "postgres-workload-ledger-v1",
        "source_position_type": "postgres_lsn",
        "previous_boundary_position": previous_boundary,
        "boundary_high_position": boundary_high,
        "expected_event_count": sum(
            item["expected_event_count"] for item in transactions
        ),
        "observed_event_count": sum(
            item["observed_event_count"] for item in transactions
        ),
        "untracked_event_count": untracked_count,
        "table_event_counts": observed["table_event_counts"],
        "operation_counts": observed["operation_counts"],
        "transactions": transactions,
    }


def manifest_uri(batch_id: str) -> str:
    readable = re.sub(r"[^A-Za-z0-9._-]+", "-", batch_id).strip("-.")[:80] or "batch"
    digest = hashlib.sha256(batch_id.encode("utf-8")).hexdigest()[:16]
    return f"s3://{S3_BUCKET}/{MANIFEST_PREFIX}/{readable}-{digest}.json"


def _batch_result(cursor: Any, batch_id: str) -> dict[str, Any]:
    cursor.execute(
        """
        SELECT batch_id, manifest_uri, manifest_sha256, source_adapter,
               schema_contract_version, state, created_at
        FROM pipeline_batch
        WHERE batch_id = %s
        """,
        (batch_id,),
    )
    row = cursor.fetchone()
    if row is None:
        raise ControlPlaneError(f"Unknown pipeline batch: {batch_id}")
    return {
        "batch_id": row[0],
        "manifest_uri": row[1],
        "manifest_sha256": row[2],
        "source_adapter": row[3],
        "schema_contract_version": row[4],
        "state": row[5],
        "created_at": row[6].isoformat(),
    }


def create_or_get_batch(batch_id: str, airflow_run_id: str) -> dict[str, Any]:
    """Create one immutable manifest or return the existing batch idempotently."""
    if not batch_id or len(batch_id) > 250:
        raise ControlPlaneError("batch_id must contain between 1 and 250 characters")

    client = s3_client()
    objects = discover_bronze_objects(client, S3_BUCKET, BRONZE_PREFIX)
    with closing(_connect()) as connection, connection, connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext('cdc_raw_vault_manifest'))")
        cursor.execute("SELECT 1 FROM pipeline_batch WHERE batch_id = %s", (batch_id,))
        if cursor.fetchone():
            result = _batch_result(cursor, batch_id)
            read_manifest(
                client,
                result["manifest_uri"],
                expected_sha256=result["manifest_sha256"],
                expected_batch_id=batch_id,
            )
            return result

        cursor.execute(
            "SELECT airflow_run_id FROM pipeline_attempt WHERE state = 'RUNNING' LIMIT 1"
        )
        running_attempt = cursor.fetchone()
        if running_attempt:
            raise ControlPlaneError(
                f"Attempt {running_attempt[0]!r} is still running; a new batch cannot be sealed"
            )
        cursor.execute(
            """
            SELECT batch_id, state
            FROM pipeline_batch
            WHERE state NOT IN ('PUBLISHED', 'SUPERSEDED')
            ORDER BY created_at
            LIMIT 1
            """
        )
        active = cursor.fetchone()
        if active:
            raise ControlPlaneError(
                f"Unfinished batch {active[0]!r} is {active[1]}; retry or supersede it "
                "before creating another batch"
            )

        cursor.execute(
            """
            SELECT p.topic, p.partition_id, p.watermark_high
            FROM pipeline_batch_partition p
            JOIN pipeline_batch b ON b.batch_id = p.batch_id
            WHERE b.state = 'PUBLISHED'
              AND b.published_at = (SELECT max(published_at) FROM pipeline_batch WHERE state = 'PUBLISHED')
            """
        )
        previous_highs = {(row[0], int(row[1])): int(row[2]) for row in cursor.fetchall()}
        preliminary_payload = build_manifest_payload(
            batch_id=batch_id,
            objects=objects,
            previous_highs=previous_highs,
            source_adapter=SOURCE_ADAPTER,
            schema_contract_version=SCHEMA_CONTRACT_VERSION,
        )
        source_control = _build_source_control(
            cursor,
            client,
            preliminary_payload["objects"],
        )
        payload = build_manifest_payload(
            batch_id=batch_id,
            objects=objects,
            previous_highs=previous_highs,
            source_adapter=SOURCE_ADAPTER,
            schema_contract_version=SCHEMA_CONTRACT_VERSION,
            source_control=source_control,
            created_at=preliminary_payload["created_at"],
        )
        document = build_manifest_document(payload)
        uri = manifest_uri(batch_id)
        write_immutable_manifest(client, uri, document)

        cursor.execute(
            """
            INSERT INTO pipeline_batch (
                batch_id, airflow_run_id, manifest_uri, manifest_sha256,
                source_adapter, schema_contract_version, source_position_type,
                source_boundary_high, source_expected_event_count,
                source_observed_event_count, source_untracked_event_count, state
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 'CREATED')
            """,
            (
                batch_id,
                airflow_run_id,
                uri,
                document["manifest_sha256"],
                SOURCE_ADAPTER,
                SCHEMA_CONTRACT_VERSION,
                source_control["source_position_type"],
                source_control["boundary_high_position"],
                source_control["expected_event_count"],
                source_control["observed_event_count"],
                source_control["untracked_event_count"],
            ),
        )
        for bound in payload["partitions"]:
            cursor.execute(
                """
                INSERT INTO pipeline_batch_partition (
                    batch_id, topic, partition_id, watermark_low, watermark_high,
                    interval_object_count, interval_input_bytes
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    batch_id,
                    bound["topic"],
                    bound["partition"],
                    bound["watermark_low"],
                    bound["watermark_high"],
                    bound["interval_object_count"],
                    bound["interval_input_bytes"],
                ),
            )
        for transaction in source_control["transactions"]:
            cursor.execute(
                """
                INSERT INTO pipeline_batch_source_transaction (
                    batch_id, source_txid, workload_run_id, workload_event_key,
                    source_position_type, source_boundary_position,
                    source_boundary_lsn, expected_event_count, observed_event_count,
                    event_lsn_low, event_lsn_high, status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    batch_id,
                    transaction["source_txid"],
                    transaction["workload_run_id"],
                    transaction["workload_event_key"],
                    source_control["source_position_type"],
                    transaction["source_boundary_position"],
                    transaction["source_boundary_lsn"],
                    transaction["expected_event_count"],
                    transaction["observed_event_count"],
                    transaction["event_lsn_low"],
                    transaction["event_lsn_high"],
                    transaction["status"],
                ),
            )
        cursor.execute(
            """
            INSERT INTO pipeline_batch_state_event (batch_id, from_state, to_state, reason)
            VALUES (%s, NULL, 'CREATED', 'immutable manifest persisted and verified')
            """,
            (batch_id,),
        )
        return _batch_result(cursor, batch_id)


def start_attempt(batch_id: str, airflow_run_id: str) -> dict[str, Any]:
    with closing(_connect()) as connection, connection, connection.cursor() as cursor:
        cursor.execute("SELECT pg_advisory_xact_lock(hashtext('cdc_raw_vault_manifest'))")
        cursor.execute(
            "SELECT batch_id, attempt_number, state FROM pipeline_attempt WHERE airflow_run_id = %s",
            (airflow_run_id,),
        )
        existing = cursor.fetchone()
        if existing:
            if existing[0] != batch_id:
                raise ControlPlaneError("Airflow run ID is already assigned to another batch")
            result = _batch_result(cursor, batch_id)
            result.update({"attempt_number": int(existing[1]), "attempt_state": existing[2]})
            return result

        cursor.execute(
            """
            SELECT state, manifest_sha256, created_at
            FROM pipeline_batch
            WHERE batch_id = %s
            FOR UPDATE
            """,
            (batch_id,),
        )
        row = cursor.fetchone()
        if row is None:
            raise ControlPlaneError(f"Unknown pipeline batch: {batch_id}")
        if row[0] not in {"CREATED", "FAILED", "PUBLISHED"}:
            raise ControlPlaneError(f"Batch {batch_id!r} cannot start from state {row[0]}")
        cursor.execute(
            """
            SELECT airflow_run_id
            FROM pipeline_attempt
            WHERE state = 'RUNNING' AND airflow_run_id <> %s
            LIMIT 1
            """,
            (airflow_run_id,),
        )
        running_attempt = cursor.fetchone()
        if running_attempt:
            raise ControlPlaneError(f"Another attempt is running: {running_attempt[0]}")
        if row[0] == "PUBLISHED":
            cursor.execute(
                "SELECT 1 FROM pipeline_batch WHERE created_at > %s LIMIT 1",
                (row[2],),
            )
            if cursor.fetchone():
                raise ControlPlaneError("Only the latest published batch can be replayed")
        else:
            validate_transition(row[0], "RUNNING")
        cursor.execute(
            "SELECT coalesce(max(attempt_number), 0) + 1 FROM pipeline_attempt WHERE batch_id = %s",
            (batch_id,),
        )
        attempt_number = int(cursor.fetchone()[0])
        cursor.execute(
            """
            INSERT INTO pipeline_attempt (
                batch_id, attempt_number, airflow_run_id, manifest_sha256, state
            ) VALUES (%s, %s, %s, %s, 'RUNNING')
            """,
            (batch_id, attempt_number, airflow_run_id, row[1]),
        )
        if row[0] != "PUBLISHED":
            cursor.execute(
                "UPDATE pipeline_batch SET state = 'RUNNING', updated_at = now() WHERE batch_id = %s",
                (batch_id,),
            )
            cursor.execute(
                """
                INSERT INTO pipeline_batch_state_event (
                    batch_id, attempt_number, from_state, to_state, reason
                ) VALUES (%s, %s, %s, 'RUNNING', 'Airflow attempt started')
                """,
                (batch_id, attempt_number, row[0]),
            )
        result = _batch_result(cursor, batch_id)
        result.update({"attempt_number": attempt_number, "attempt_state": "RUNNING"})
        return result


def record_task_success(
    batch_id: str,
    airflow_run_id: str,
    task_id: str,
    manifest_uri: str,
    manifest_sha256: str,
) -> None:
    if task_id not in EXPECTED_RAW_VAULT_TASKS:
        raise ControlPlaneError(f"Unexpected Raw Vault task evidence: {task_id}")
    manifest = read_manifest(
        s3_client(),
        manifest_uri,
        expected_sha256=manifest_sha256,
        expected_batch_id=batch_id,
    )
    reader_mode = str(manifest["reader_mode"])
    selected_objects = int(
        manifest.get(
            "interval_object_count",
            sum(int(bound["interval_object_count"]) for bound in manifest["partitions"]),
        )
    )
    selected_bytes = int(manifest.get("interval_input_bytes", 0))
    failed_audit_rules = 0
    with closing(_connect()) as connection, connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT a.attempt_number, a.manifest_sha256, a.state, b.state
            FROM pipeline_attempt a
            JOIN pipeline_batch b ON b.batch_id = a.batch_id
            WHERE a.batch_id = %s AND a.airflow_run_id = %s
            FOR UPDATE
            """,
            (batch_id, airflow_run_id),
        )
        row = cursor.fetchone()
        if row is None or row[2] != "RUNNING" or row[3] not in {"RUNNING", "PUBLISHED"}:
            raise ControlPlaneError("Task evidence requires a running batch attempt")
        if row[1] != manifest_sha256:
            raise ControlPlaneError("Task evidence manifest checksum differs from the attempt")
        if task_id == "reconcile_source_bronze_silver":
            from psycopg2.extras import Json

            attempt_number = int(row[0])
            evidence_uri = audit_evidence_uri(S3_BUCKET, batch_id, attempt_number)
            audit, evidence_sha256 = read_audit_evidence(
                s3_client(),
                evidence_uri,
                expected_batch_id=batch_id,
                expected_attempt_number=attempt_number,
                expected_airflow_run_id=airflow_run_id,
                expected_manifest_sha256=manifest_sha256,
            )
            failed_audit_rules = sum(rule["status"] == "FAIL" for rule in audit["rules"])
            audit_state = "FAIL" if failed_audit_rules else "PASS"
            cursor.execute(
                """
                INSERT INTO pipeline_attempt_audit (
                    batch_id, attempt_number, evidence_uri, evidence_sha256,
                    rule_version, state, rule_count, failed_rule_count
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (batch_id, attempt_number) DO NOTHING
                """,
                (
                    batch_id,
                    attempt_number,
                    evidence_uri,
                    evidence_sha256,
                    audit["rule_version"],
                    audit_state,
                    len(audit["rules"]),
                    failed_audit_rules,
                ),
            )
            cursor.execute(
                """
                SELECT evidence_sha256, state
                FROM pipeline_attempt_audit
                WHERE batch_id = %s AND attempt_number = %s
                """,
                (batch_id, attempt_number),
            )
            persisted_audit = cursor.fetchone()
            if persisted_audit != (evidence_sha256, audit_state):
                raise ControlPlaneError("Attempt audit evidence is immutable and conflicts")
            for rule in audit["rules"]:
                cursor.execute(
                    """
                    INSERT INTO pipeline_audit_rule_result (
                        batch_id, attempt_number, rule_id, object_name, rule_version,
                        scope, expected_value, observed_value, difference, status,
                        watermark, evidence_uri, details
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (batch_id, attempt_number, rule_id, object_name)
                    DO NOTHING
                    """,
                    (
                        batch_id,
                        attempt_number,
                        rule["rule_id"],
                        rule["object_name"],
                        audit["rule_version"],
                        rule["scope"],
                        rule["expected_value"],
                        rule["observed_value"],
                        rule["difference"],
                        rule["status"],
                        Json(audit["watermark"]),
                        evidence_uri,
                        Json(rule.get("details", {})),
                    ),
                )
        if not failed_audit_rules:
            cursor.execute(
                """
                INSERT INTO pipeline_task_evidence (
                    batch_id, attempt_number, task_id, manifest_sha256, reader_mode,
                    selected_input_object_count, selected_input_bytes, state, finished_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'SUCCESS', now())
                ON CONFLICT (batch_id, attempt_number, task_id) DO UPDATE
                SET manifest_sha256 = EXCLUDED.manifest_sha256,
                    reader_mode = EXCLUDED.reader_mode,
                    selected_input_object_count = EXCLUDED.selected_input_object_count,
                    selected_input_bytes = EXCLUDED.selected_input_bytes,
                    state = 'SUCCESS', finished_at = EXCLUDED.finished_at
                """,
                (
                    batch_id,
                    row[0],
                    task_id,
                    manifest_sha256,
                    reader_mode,
                    selected_objects,
                    selected_bytes,
                ),
            )
    if failed_audit_rules:
        raise ControlPlaneError(
            f"Point-in-time reconciliation failed {failed_audit_rules} audit rules"
        )


def publish_batch(batch_id: str, airflow_run_id: str, manifest_sha256: str) -> None:
    with closing(_connect()) as connection, connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT a.attempt_number, a.manifest_sha256, a.state, b.state
            FROM pipeline_attempt a
            JOIN pipeline_batch b ON b.batch_id = a.batch_id
            WHERE a.batch_id = %s AND a.airflow_run_id = %s
            FOR UPDATE
            """,
            (batch_id, airflow_run_id),
        )
        row = cursor.fetchone()
        if row and row[2:] == ("SUCCESS", "PUBLISHED"):
            if row[1] != manifest_sha256:
                raise ControlPlaneError("Published attempt manifest checksum changed")
            return
        if row is None or row[2] != "RUNNING" or row[3] not in {"RUNNING", "PUBLISHED"}:
            raise ControlPlaneError("Publish requires a running batch attempt")
        if row[1] != manifest_sha256:
            raise ControlPlaneError("Publish manifest checksum differs from the attempt")
        cursor.execute(
            """
            SELECT task_id, manifest_sha256, state
            FROM pipeline_task_evidence
            WHERE batch_id = %s AND attempt_number = %s
            """,
            (batch_id, row[0]),
        )
        evidence = {value[0]: (value[1], value[2]) for value in cursor.fetchall()}
        expected = set(EXPECTED_RAW_VAULT_TASKS)
        if set(evidence) != expected or any(
            value != (manifest_sha256, "SUCCESS") for value in evidence.values()
        ):
            raise ControlPlaneError("All Raw Vault tasks must prove the same manifest before publish")
        cursor.execute(
            """
            SELECT state, failed_rule_count
            FROM pipeline_attempt_audit
            WHERE batch_id = %s AND attempt_number = %s
            """,
            (batch_id, row[0]),
        )
        audit = cursor.fetchone()
        if audit != ("PASS", 0):
            raise ControlPlaneError("Publish requires a passing point-in-time audit")

        if row[3] == "PUBLISHED":
            cursor.execute(
                """
                UPDATE pipeline_attempt
                SET state = 'SUCCESS', finished_at = now()
                WHERE batch_id = %s AND attempt_number = %s
                """,
                (batch_id, row[0]),
            )
            return

        validate_transition("RUNNING", "VALIDATED")
        cursor.execute(
            "UPDATE pipeline_batch SET state = 'VALIDATED', updated_at = now() WHERE batch_id = %s",
            (batch_id,),
        )
        cursor.execute(
            """
            INSERT INTO pipeline_batch_state_event (
                batch_id, attempt_number, from_state, to_state, reason
            ) VALUES (%s, %s, 'RUNNING', 'VALIDATED', 'all task evidence accepted')
            """,
            (batch_id, row[0]),
        )
        validate_transition("VALIDATED", "PUBLISHED")
        cursor.execute(
            """
            INSERT INTO pipeline_published_interval (
                batch_id, topic, partition_id, watermark_low, watermark_high
            )
            SELECT batch_id, topic, partition_id, watermark_low, watermark_high
            FROM pipeline_batch_partition
            WHERE batch_id = %s AND watermark_high > watermark_low
            ON CONFLICT (batch_id, topic, partition_id) DO NOTHING
            """,
            (batch_id,),
        )
        cursor.execute(
            """
            UPDATE pipeline_attempt
            SET state = 'SUCCESS', finished_at = now()
            WHERE batch_id = %s AND attempt_number = %s
            """,
            (batch_id, row[0]),
        )
        cursor.execute(
            """
            UPDATE pipeline_batch
            SET state = 'PUBLISHED', published_at = now(), updated_at = now()
            WHERE batch_id = %s
            """,
            (batch_id,),
        )
        cursor.execute(
            """
            INSERT INTO pipeline_batch_state_event (
                batch_id, attempt_number, from_state, to_state, reason
            ) VALUES (%s, %s, 'VALIDATED', 'PUBLISHED', 'Raw Vault batch published')
            """,
            (batch_id, row[0]),
        )


def fail_airflow_run(airflow_run_id: str, reason: str) -> None:
    """Fail the active attempt without hiding the original Airflow exception."""
    try:
        with closing(_connect()) as connection, connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT a.batch_id, a.attempt_number, a.state, b.state
                FROM pipeline_attempt a
                JOIN pipeline_batch b ON b.batch_id = a.batch_id
                WHERE a.airflow_run_id = %s
                FOR UPDATE
                """,
                (airflow_run_id,),
            )
            row = cursor.fetchone()
            if row is None or row[2] != "RUNNING":
                return
            cursor.execute(
                """
                UPDATE pipeline_attempt
                SET state = 'FAILED', finished_at = now(), error_message = %s
                WHERE batch_id = %s AND attempt_number = %s
                """,
                (reason[:4000], row[0], row[1]),
            )
            if row[3] == "RUNNING":
                validate_transition("RUNNING", "FAILED")
                cursor.execute(
                    "UPDATE pipeline_batch SET state = 'FAILED', updated_at = now() WHERE batch_id = %s",
                    (row[0],),
                )
                cursor.execute(
                    """
                    INSERT INTO pipeline_batch_state_event (
                        batch_id, attempt_number, from_state, to_state, reason
                    ) VALUES (%s, %s, 'RUNNING', 'FAILED', %s)
                    """,
                    (row[0], row[1], reason[:4000]),
                )
    except Exception as error:
        print(f"Control-plane failure callback could not persist state: {error}")


def supersede_batch(batch_id: str, reason: str) -> None:
    with closing(_connect()) as connection, connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT state FROM pipeline_batch WHERE batch_id = %s FOR UPDATE",
            (batch_id,),
        )
        row = cursor.fetchone()
        if row is None or row[0] not in {"CREATED", "FAILED"}:
            raise ControlPlaneError("Only CREATED or FAILED batches can be superseded")
        validate_transition(row[0], "SUPERSEDED")
        cursor.execute(
            "UPDATE pipeline_batch SET state = 'SUPERSEDED', updated_at = now() WHERE batch_id = %s",
            (batch_id,),
        )
        cursor.execute(
            """
            INSERT INTO pipeline_batch_state_event (batch_id, from_state, to_state, reason)
            VALUES (%s, %s, 'SUPERSEDED', %s)
            """,
            (batch_id, row[0], reason[:4000]),
        )

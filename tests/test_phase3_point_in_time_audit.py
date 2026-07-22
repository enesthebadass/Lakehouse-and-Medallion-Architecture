# ruff: noqa: E402, I001

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline_control.audit import (  # noqa: E402
    AuditEvidenceError,
    audit_evidence_uri,
    build_audit_payload,
    validate_audit_payload,
)
from pipeline_control.control import (  # noqa: E402
    _ledger_rows_through_observed_boundary,
)
from pipeline_control.manifest import (  # noqa: E402
    build_manifest_payload,
    summarize_bronze_transactions,
)


class Body:
    def __init__(self, value):
        self.value = value

    def read(self):
        return self.value


class FakeS3:
    def __init__(self, objects):
        self.objects = objects

    def get_object(self, Bucket, Key):
        return {"Body": Body(self.objects[(Bucket, Key)])}


def bronze_object(key, txid, lsn, operation="u"):
    value = {
        "_metadata": {
            "object_key": key,
            "source_schema": "mms",
            "source_table": "customers",
            "operation": operation,
        },
        "value": {"source": {"txId": txid, "lsn": lsn}},
    }
    return (json.dumps(value) + "\n").encode()


def source_control(expected=2, observed=2):
    return {
        "contract_version": "postgres-workload-ledger-v1",
        "source_position_type": "postgres_lsn",
        "previous_boundary_position": 100,
        "boundary_high_position": 200,
        "expected_event_count": expected,
        "observed_event_count": observed,
        "untracked_event_count": 0,
        "table_event_counts": {"mms.customers": observed},
        "operation_counts": {"u": observed},
        "transactions": [
            {
                "source_txid": 42,
                "workload_run_id": "phase3-test",
                "workload_event_key": "customer_change",
                "source_boundary_lsn": "0/C8",
                "source_boundary_position": 200,
                "expected_event_count": expected,
                "observed_event_count": observed,
                "event_lsn_low": 150,
                "event_lsn_high": 160,
                "status": "PASS" if expected == observed else "FAIL",
            }
        ],
    }


def manifest_fixture():
    topic = "bank.core.mms.customers"
    objects = []
    for offset in (10, 11):
        objects.append(
            {
                "object_key": (
                    "bronze/cdc/source=core_banking/schema=mms/table=customers/"
                    f"event_date=2026-07-21/topic={topic}/partition=0/"
                    f"offset={offset:020d}.json"
                ),
                "size_bytes": 100,
                "storage_etag": f"etag-{offset}",
            }
        )
    return build_manifest_payload(
        batch_id="phase3-test",
        objects=objects,
        previous_highs={(topic, 0): 9},
        source_adapter="postgres-debezium",
        schema_contract_version="cdc-envelope-v1",
        source_control=source_control(),
        created_at="2026-07-21T12:00:00+00:00",
    )


def passing_rule():
    return {
        "rule_id": "SOURCE_LEDGER_TO_BRONZE_EVENTS",
        "object_name": "postgres_workload_ledger",
        "scope": "point_in_time_batch",
        "expected_value": 2,
        "observed_value": 2,
        "difference": 0,
        "status": "PASS",
        "details": {},
    }


def test_bronze_transaction_summary_uses_txid_and_lsn_from_selected_objects():
    first = "bronze/topic=bank.core.mms.customers/partition=0/offset=10.json"
    second = "bronze/topic=bank.core.mms.customers/partition=0/offset=11.json"
    client = FakeS3(
        {
            ("lakehouse", first): bronze_object(first, 42, 150),
            ("lakehouse", second): bronze_object(second, 42, 160),
        }
    )
    summary = summarize_bronze_transactions(
        client,
        "lakehouse",
        [{"object_key": first}, {"object_key": second}],
    )

    assert summary["untracked_event_count"] == 0
    assert summary["transactions"] == [
        {
            "source_txid": 42,
            "observed_event_count": 2,
            "event_lsn_low": 150,
            "event_lsn_high": 160,
        }
    ]


def test_manifest_v3_embeds_point_in_time_source_control():
    manifest = manifest_fixture()

    assert manifest["schema_version"] == 3
    assert manifest["source_control"]["expected_event_count"] == 2
    assert manifest["source_control"]["boundary_high_position"] == 200


def test_audit_payload_is_attempt_specific_and_deterministic():
    manifest = manifest_fixture()
    payload = build_audit_payload(
        batch_id="phase3-test",
        attempt_number=2,
        airflow_run_id="phase3-replay",
        manifest_sha256="a" * 64,
        manifest=manifest,
        rules=[passing_rule()],
    )

    validated = validate_audit_payload(
        payload,
        expected_batch_id="phase3-test",
        expected_attempt_number=2,
        expected_airflow_run_id="phase3-replay",
        expected_manifest_sha256="a" * 64,
    )
    assert validated == payload
    assert audit_evidence_uri("lakehouse", "phase3-test", 2).endswith(
        "/attempt=2.json"
    )


def test_audit_rejects_status_that_disagrees_with_observed_value():
    manifest = manifest_fixture()
    rule = passing_rule()
    rule["observed_value"] = 1
    rule["difference"] = -1
    payload = build_audit_payload(
        batch_id="phase3-test",
        attempt_number=1,
        airflow_run_id="phase3-run",
        manifest_sha256="b" * 64,
        manifest=manifest,
        rules=[rule],
    )

    try:
        validate_audit_payload(
            payload,
            expected_batch_id="phase3-test",
            expected_attempt_number=1,
            expected_airflow_run_id="phase3-run",
            expected_manifest_sha256="b" * 64,
        )
    except AuditEvidenceError as error:
        assert "status" in str(error)
    else:
        raise AssertionError("Inconsistent audit status unexpectedly passed")


def test_source_workload_ledger_captures_expected_events_txid_and_lsn():
    schema = (ROOT / "source/init/004_create_workload_control.sql").read_text()
    workload = (ROOT / "source/workload/workload.py").read_text()

    for column in (
        "expected_cdc_event_count",
        "source_txid",
        "source_boundary_lsn",
    ):
        assert column in schema
        assert column in workload
    assert "txid_current()" in workload
    assert "pg_current_wal_insert_lsn()" in workload


def test_source_boundary_excludes_transactions_not_yet_observed_in_bronze():
    rows = [
        ("run-1", "first", 3, 100, "0/64", 100),
        ("run-1", "missing-middle", 1, 101, "0/6E", 110),
        ("run-2", "post-boundary", 2, 102, "0/78", 120),
    ]

    bounded, boundary_high = _ledger_rows_through_observed_boundary(rows, {100})

    assert boundary_high == 100
    assert [row[3] for row in bounded] == [100]

    bounded, boundary_high = _ledger_rows_through_observed_boundary(rows, {100, 102})

    assert boundary_high == 120
    assert [row[3] for row in bounded] == [100, 101, 102]


def test_control_plane_retains_append_only_rule_and_source_transaction_history():
    schema = (ROOT / "pipeline_control/init/001_schema.sql").read_text()
    control = (ROOT / "pipeline_control/control.py").read_text()

    for table in (
        "pipeline_batch_source_transaction",
        "pipeline_attempt_audit",
        "pipeline_audit_rule_result",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in schema
    assert "Publish requires a passing point-in-time audit" in control
    assert "ON CONFLICT (batch_id, attempt_number, rule_id, object_name)" in control


def test_spark_reconciliation_covers_boundary_conservation_and_classification():
    script = (ROOT / "scripts/4_process_cdc_raw_vault.py").read_text()

    for rule in (
        "MANIFEST_TO_BRONZE_EVENT_COUNT",
        "MANIFEST_TO_UNIQUE_KAFKA_COORDINATES",
        "SOURCE_LEDGER_TO_BRONZE_EVENTS",
        "SOURCE_TRANSACTION_COMPLETENESS",
        "UNTRACKED_SOURCE_EVENTS",
        "BRONZE_CLASSIFICATION_CONSERVATION",
    ):
        assert rule in script
    assert 'how="left_anti"' in script
    assert "persist_audit_evidence" in script


def test_airflow_passes_attempt_identity_to_every_spark_phase():
    dag = (ROOT / "dags/cdc_raw_vault_dag.py").read_text()

    assert "ATTEMPT_NUMBER_TEMPLATE" in dag
    assert '"--attempt-number"' in dag
    assert '"--airflow-run-id"' in dag


def test_gold_lineage_retains_source_event_and_batch_coordinates():
    model = (ROOT / "dbt/models/gold/gold_row_lineage.sql").read_text()
    model_contract = (ROOT / "dbt/models/gold/_gold_models.yml").read_text()
    baseline = (ROOT / "operations/collect_phase0_baseline.py").read_text()

    for gold_model in (
        "dim_customer_current",
        "fct_loan_applications_current",
        "fct_loans_current",
        "agg_customer_loan_portfolio",
    ):
        assert f"'{gold_model}'" in model
    for field in (
        "source_event_id",
        "source_position",
        "bronze_object_key",
        "source_lsn",
        "load_batch_id",
    ):
        assert field in model
        assert f"name: {field}" in model_contract
    assert '"pipeline_control_point_in_time_audit_passes"' in baseline
    completeness_test = (
        ROOT / "dbt/tests/assert_gold_row_lineage_complete.sql"
    ).read_text()
    assert "expected_gold_rows" in completeness_test
    assert "where lineage.gold_business_key is null" in completeness_test


if __name__ == "__main__":
    tests = [
        value
        for name, value in sorted(globals().items())
        if name.startswith("test_") and callable(value)
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"PASS {len(tests)} phase 3 point-in-time audit tests")

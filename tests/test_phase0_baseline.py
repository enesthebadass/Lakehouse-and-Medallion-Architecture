import importlib.util
import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "operations/collect_phase0_baseline.py"
CONTRACT_PATH = ROOT / "quality/correctness-invariants.yaml"


def load_module():
    spec = importlib.util.spec_from_file_location("phase0_baseline", MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


BASELINE = load_module()


def sample_report():
    return {
        "services": {"trino": {"state": "running", "health": "healthy"}},
        "source": {"table_counts": {"mms.customers": 100}},
        "kafka": {"total_lag": 0},
        "storage": {
            "bronze_cdc": {
                "object_count": 2,
                "total_bytes": 30,
                "inventory_sha256": "abc",
            },
            "raw_vault": {"object_count": 2},
        },
        "reconciliation": {"check_count": 5, "failure_count": 0},
        "pipeline_control": {
            "latest_published_batch": {
                "state": "PUBLISHED",
                "manifest_sha256": "manifest",
                "partition_count": 2,
                "interval_object_count": 2,
                "interval_input_bytes": 30,
                "source_position_type": "postgres_lsn",
                "source_boundary_high": 200,
                "source_expected_event_count": 2,
                "source_observed_event_count": 2,
                "source_untracked_event_count": 0,
                "source_transaction_count": 1,
                "failed_source_transaction_count": 0,
            },
            "latest_attempt": {
                "state": "SUCCESS",
                "manifest_sha256": "manifest",
                "task_evidence_count": 4,
                "distinct_task_manifest_count": 1,
                "failed_task_evidence_count": 0,
                "distinct_reader_mode_count": 1,
                "reader_mode": "bounded_object_list",
                "min_selected_input_object_count": 2,
                "max_selected_input_object_count": 2,
                "min_selected_input_bytes": 30,
                "max_selected_input_bytes": 30,
                "audit_state": "PASS",
                "audit_rule_count": 6,
                "audit_failed_rule_count": 0,
                "audit_evidence_uri": "s3://lakehouse/audit/phase3.json",
                "audit_evidence_sha256": "a" * 64,
                "persisted_audit_rule_count": 6,
                "persisted_failed_audit_rule_count": 0,
            },
        },
        "raw_vault_event_identity": {
            "sat_customer_details": {
                "row_count": 2,
                "distinct_source_event_ids": 2,
            }
        },
        "gold_grains": {
            "dim_customer_current": {
                "row_count": 2,
                "distinct_grain_count": 2,
                "valid": True,
            }
        },
        "tables": {
            "raw_vault": {
                "hub_customer": {"row_count": 2, "content_checksum": "raw"}
            },
            "gold": {
                "dim_customer_current": {
                    "row_count": 2,
                    "content_checksum": "gold",
                }
            },
        },
    }


def test_invariant_contract_is_complete_and_machine_readable():
    contract = yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))
    invariants = contract["invariants"]
    ids = [invariant["id"] for invariant in invariants]

    assert contract["schema_version"] == 1
    assert len(ids) == len(set(ids))
    assert {
        "batch.input_boundary_is_immutable",
        "incremental.work_is_bounded_by_new_events",
        "reconciliation.uses_one_source_boundary",
        "gold.current_excludes_deleted_entities",
        "gold.publish_is_atomic",
        "schema.incompatible_event_is_isolated",
        "storage.growth_is_measured_and_bounded",
    } <= set(ids)

    allowed_statuses = set(contract["status_values"])
    allowed_severities = set(contract["severity_values"])
    required_fields = {
        field
        for fields in contract["required_technical_fields"].values()
        for field in fields
    }
    assert required_fields == set(contract["technical_field_semantics"])
    for invariant in invariants:
        assert invariant["status"] in allowed_statuses
        assert invariant["severity"] in allowed_severities
        assert invariant["owner"]
        assert invariant["scope"]
        assert invariant["statement"]
        assert invariant["evidence"]
        assert 0 <= invariant["phase"] <= 10


def test_implemented_invariants_have_executable_or_historical_evidence():
    contract = yaml.safe_load(CONTRACT_PATH.read_text(encoding="utf-8"))
    for invariant in contract["invariants"]:
        if invariant["status"] != "implemented_poc":
            continue
        evidence = invariant["evidence"]
        assert {"automated_test", "baseline_check", "historical_result"} & set(evidence)


def test_kafka_group_parser_preserves_partition_boundaries():
    output = """
GROUP bronze-cdc-writer-v1 has no active members.
GROUP TOPIC PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG CONSUMER-ID HOST CLIENT-ID
bronze-cdc-writer-v1 bank.core.mms.customers 0 101 101 0 - - -
bronze-cdc-writer-v1 bank.core.krd.loans 2 45 48 3 - - -
"""
    partitions = BASELINE.parse_kafka_group(output)

    assert len(partitions) == 2
    assert partitions[0]["topic"] == "bank.core.krd.loans"
    assert partitions[0]["partition"] == 2
    assert partitions[0]["lag"] == 3
    assert partitions[1]["current_offset"] == 101


def test_docker_discovery_honors_explicit_cli():
    assert BASELINE.discover_docker_bin("/custom/docker") == "/custom/docker"


def test_minio_inventory_summary_is_deterministic_and_measures_small_files():
    output = "\n".join(
        (
            json.dumps({"status": "success", "type": "file", "key": "b", "size": 20}),
            "not-json",
            json.dumps({"status": "success", "type": "file", "key": "a", "size": 10}),
            json.dumps({"status": "success", "type": "folder", "key": "c", "size": 0}),
        )
    )
    objects = BASELINE.parse_mc_inventory(output)
    summary = BASELINE.summarize_inventory(objects)

    assert [item["key"] for item in objects] == ["a", "b"]
    assert summary["object_count"] == 2
    assert summary["total_bytes"] == 30
    assert summary["median_bytes"] == 10
    assert summary["p95_bytes"] == 20
    assert summary["objects_under_1_mib"] == 2
    assert len(summary["inventory_sha256"]) == 64


def test_table_fingerprint_sql_is_deterministic_and_quotes_columns():
    sql = BASELINE.table_fingerprint_sql(
        "lakehouse.gold_dbt.dim_customer_current",
        [("customer_hk", "varchar"), ("source_updated_at", "timestamp(3) with time zone")],
    )

    assert 'CAST("customer_hk" AS VARCHAR)' in sql
    assert 'CAST("source_updated_at" AS VARCHAR)' in sql
    assert "checksum" in sql
    assert sql.count("(") == sql.count(")")
    assert sql.endswith("FROM lakehouse.gold_dbt.dim_customer_current")


def test_trino_uses_stdin_instead_of_command_line_sql():
    class RecordingRunner:
        def __init__(self):
            self.args = ()
            self.input_text = None

        def compose(self, *args, input_text=None):
            self.args = args
            self.input_text = input_text
            return "1\n"

    runner = RecordingRunner()
    rows = BASELINE.trino(runner, "SELECT 1")

    assert "--execute" not in runner.args
    assert runner.args[-2:] == ("--file", "/dev/stdin")
    assert runner.input_text == "SELECT 1;\n"
    assert rows == [["1"]]


def test_baseline_checks_fail_closed_on_duplicate_event_identity():
    report = sample_report()
    report["raw_vault_event_identity"]["sat_customer_details"][
        "distinct_source_event_ids"
    ] = 1

    checks = {item["id"]: item["passed"] for item in BASELINE.build_checks(report)}

    assert checks["required_services_are_running"]
    assert checks["latest_reconciliation_passes"]
    assert checks["pipeline_control_latest_attempt_is_published"]
    assert checks["pipeline_control_point_in_time_audit_passes"]
    assert not checks["raw_vault_event_identity_is_unique"]


def test_baseline_comparison_detects_business_data_change_but_ignores_runtime_metadata():
    previous = sample_report()
    current = sample_report()
    previous["collected_at"] = "2026-01-01T00:00:00Z"
    current["collected_at"] = "2026-01-02T00:00:00Z"

    assert BASELINE.compare_reports(previous, current)["matches"]

    current["tables"]["gold"]["dim_customer_current"]["content_checksum"] = "changed"
    comparison = BASELINE.compare_reports(previous, current)

    assert not comparison["matches"]
    assert comparison["differences"][0]["path"].endswith("content_checksum")


def test_collector_scope_matches_registered_raw_vault_and_dbt_models():
    register_text = (ROOT / "trino/register_tables.py").read_text(encoding="utf-8")
    for table in BASELINE.RAW_VAULT_TABLES:
        assert f'"{table}"' in register_text

    model_names = {
        path.stem
        for path in (ROOT / "dbt/models/gold").glob("*.sql")
    }
    assert set(BASELINE.GOLD_GRAINS) == model_names
    assert len(BASELINE.SOURCE_TABLES) == 13


if __name__ == "__main__":
    tests = [
        value
        for name, value in sorted(globals().items())
        if name.startswith("test_") and callable(value)
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"PASS {len(tests)} phase 0 baseline tests")

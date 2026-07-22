# ruff: noqa: E402, I001

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline_control.manifest import (  # noqa: E402
    BOUNDED_READER_MODE,
    ManifestError,
    build_manifest_document,
    build_manifest_payload,
    validate_manifest_document,
)


def object_descriptor(topic, offset, size):
    schema, table = topic.split(".")[-2:]
    return {
        "object_key": (
            f"bronze/cdc/source=core_banking/schema={schema}/table={table}/"
            f"event_date=2026-07-21/topic={topic}/partition=0/"
            f"offset={offset:020d}.json"
        ),
        "size_bytes": size,
        "storage_etag": f"etag-{offset}",
    }


def bounded_payload(previous_high=1):
    topic = "bank.core.mms.customers"
    return build_manifest_payload(
        batch_id="phase2-test",
        objects=[object_descriptor(topic, offset, 100 + offset) for offset in range(4)],
        previous_highs={(topic, 0): previous_high},
        source_adapter="postgres-debezium",
        schema_contract_version="cdc-envelope-v1",
        created_at="2026-07-21T12:00:00+00:00",
    )


def test_manifest_contains_only_low_exclusive_high_inclusive_objects():
    payload = bounded_payload(previous_high=1)

    assert payload["reader_mode"] == BOUNDED_READER_MODE
    assert [
        int(item["object_key"].split("offset=")[1].split(".")[0])
        for item in payload["objects"]
    ] == [2, 3]
    assert payload["interval_object_count"] == 2
    assert payload["interval_input_bytes"] == 205


def test_manifest_rejects_an_object_outside_declared_partition_bounds():
    payload = bounded_payload(previous_high=1)
    payload["partitions"][0]["watermark_low"] = 2
    document = build_manifest_document(payload)

    try:
        validate_manifest_document(document)
    except ManifestError as error:
        assert "outside" in str(error)
    else:
        raise AssertionError("Out-of-bound object unexpectedly passed manifest validation")


def test_noop_manifest_has_no_physical_input_objects_or_bytes():
    payload = bounded_payload(previous_high=3)

    assert payload["partitions"][0]["watermark_low"] == 3
    assert payload["partitions"][0]["watermark_high"] == 3
    assert payload["objects"] == []
    assert payload["interval_object_count"] == 0
    assert payload["interval_input_bytes"] == 0


def test_control_schema_prevents_published_offset_overlap():
    schema = (ROOT / "pipeline_control/init/001_schema.sql").read_text()
    migration = (ROOT / "pipeline_control/init/002_bounded_input_evidence.sql").read_text()

    for sql in (schema, migration):
        assert "pipeline_published_interval" in sql
        assert "EXCLUDE USING gist" in sql
        assert "offset_range WITH &&" in sql


def test_task_evidence_retains_bounded_input_cost():
    schema = (ROOT / "pipeline_control/init/001_schema.sql").read_text()
    control = (ROOT / "pipeline_control/control.py").read_text()
    dag = (ROOT / "dags/cdc_raw_vault_dag.py").read_text()

    assert "selected_input_object_count" in schema
    assert "selected_input_bytes" in schema
    assert "reader_mode" in schema
    assert 'manifest.get("interval_input_bytes", 0)' in control
    assert '"manifest_uri": MANIFEST_URI_TEMPLATE' in dag


def test_spark_bounded_reader_never_discovers_history_before_exact_read():
    script = (ROOT / "scripts/4_process_cdc_raw_vault.py").read_text()
    bounded_branch = script.split("def read_cdc_events", 1)[1].split(
        "def source_records", 1
    )[0]

    assert "bounded_object_paths" in bounded_branch
    assert "return spark.read.json(paths)" in bounded_branch
    assert bounded_branch.index("return spark.read.json(paths)") < bounded_branch.index(
        "recursiveFileLookup"
    )


def test_satellite_boundary_uses_persisted_hashdiff_and_source_position():
    script = (ROOT / "scripts/4_process_cdc_raw_vault.py").read_text()

    assert "filter_against_persisted_state" in script
    assert 'state_column="hashdiff"' in script
    assert "Out-of-order source position crossed the persisted boundary" in script
    assert 'F.col("load_batch_id") != F.lit(LOAD_BATCH_ID)' in script


def test_merge_metrics_do_not_count_the_full_target_before_and_after():
    script = (ROOT / "scripts/4_process_cdc_raw_vault.py").read_text()
    merge_function = script.split("def merge_insert_only", 1)[1].split(
        "def assert_unique_key", 1
    )[0]

    assert "before_count" not in merge_function
    assert "after_count" not in merge_function
    assert "numTargetRowsInserted" in merge_function
    assert ".history(1)" in merge_function


if __name__ == "__main__":
    tests = [
        value
        for name, value in sorted(globals().items())
        if name.startswith("test_") and callable(value)
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"PASS {len(tests)} phase 2 bounded-incremental tests")

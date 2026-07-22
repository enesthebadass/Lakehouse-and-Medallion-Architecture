# ruff: noqa: E402, I001

import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline_control.control import (  # noqa: E402
    ALLOWED_BATCH_TRANSITIONS,
    ControlPlaneError,
    manifest_uri,
    validate_transition,
)
from pipeline_control.manifest import (  # noqa: E402
    BOUNDED_READER_MODE,
    LEGACY_READER_MODE,
    ManifestError,
    build_manifest_document,
    build_manifest_payload,
    canonical_json,
    parse_bronze_coordinate,
    read_manifest,
    validate_manifest_document,
    write_immutable_manifest,
)

class MissingObject(Exception):
    response = {"Error": {"Code": "NoSuchKey"}}


class Body:
    def __init__(self, value):
        self.value = value

    def read(self):
        return self.value


class FakeS3:
    def __init__(self):
        self.objects = {}

    def get_object(self, Bucket, Key):
        try:
            value = self.objects[(Bucket, Key)]
        except KeyError as error:
            raise MissingObject from error
        return {"Body": Body(value)}

    def put_object(self, Bucket, Key, Body, **_kwargs):
        self.objects[(Bucket, Key)] = Body


def bronze_object(topic, offset, size_bytes):
    schema, table = topic.split(".")[-2:]
    return {
        "object_key": (
            f"bronze/cdc/source=core_banking/schema={schema}/table={table}/"
            f"event_date=2026-07-21/topic={topic}/partition=0/"
            f"offset={offset:020d}.json"
        ),
        "size_bytes": size_bytes,
        "storage_etag": f"etag-{topic}-{offset}",
    }


def manifest_fixture(previous_high=1):
    loans = "bank.core.krd.loans"
    customers = "bank.core.mms.customers"
    payload = build_manifest_payload(
        batch_id="phase1-test",
        objects=[
            bronze_object(loans, 0, 100),
            bronze_object(loans, 1, 101),
            bronze_object(loans, 2, 102),
            bronze_object(customers, 0, 200),
            bronze_object(customers, 1, 201),
            bronze_object(customers, 2, 202),
            bronze_object(customers, 3, 203),
        ],
        previous_highs={
            ("bank.core.krd.loans", 0): previous_high,
            ("bank.core.mms.customers", 0): 2,
        },
        source_adapter="postgres-debezium",
        schema_contract_version="cdc-envelope-v1",
        created_at="2026-07-21T12:00:00+00:00",
    )
    return build_manifest_document(payload)


def test_bronze_coordinate_parser_uses_immutable_object_identity():
    key = (
        "bronze/cdc/source=core_banking/schema=mms/table=customers/"
        "event_date=2026-07-21/topic=bank.core.mms.customers/partition=2/"
        "offset=00000000000000000123.json"
    )
    assert parse_bronze_coordinate(key) == ("bank.core.mms.customers", 2, 123)
    assert parse_bronze_coordinate(key + ".tmp") is None


def test_manifest_bounds_are_sorted_and_low_is_exclusive():
    document = manifest_fixture()
    payload = validate_manifest_document(document)

    assert payload["interval_semantics"] == "(watermark_low, watermark_high]"
    assert payload["reader_mode"] == BOUNDED_READER_MODE
    assert payload["partitions"] == [
        {
            "topic": "bank.core.krd.loans",
            "partition": 0,
            "watermark_low": 1,
            "watermark_high": 2,
            "interval_object_count": 1,
            "interval_input_bytes": 102,
        },
        {
            "topic": "bank.core.mms.customers",
            "partition": 0,
            "watermark_low": 2,
            "watermark_high": 3,
            "interval_object_count": 1,
            "interval_input_bytes": 203,
        },
    ]
    assert payload["interval_object_count"] == 2
    assert payload["interval_input_bytes"] == 305
    assert [item["size_bytes"] for item in payload["objects"]] == [102, 203]


def test_noop_manifest_has_zero_interval_objects_but_keeps_snapshot_high():
    document = manifest_fixture(previous_high=2)
    first = document["payload"]["partitions"][0]

    assert first["watermark_low"] == first["watermark_high"] == 2
    assert first["interval_object_count"] == 0
    assert first["interval_input_bytes"] == 0
    assert document["payload"]["interval_object_count"] == 1


def test_manifest_checksum_detects_any_payload_mutation():
    document = manifest_fixture()
    document["payload"]["partitions"][0]["watermark_high"] = 99

    try:
        validate_manifest_document(document)
    except ManifestError as error:
        assert "checksum" in str(error)
    else:
        raise AssertionError("Mutated manifest unexpectedly passed checksum validation")


def test_manifest_write_is_idempotent_and_conflicting_overwrite_fails():
    client = FakeS3()
    uri = "s3://lakehouse/bronze/_control/manifests/test.json"
    document = manifest_fixture()

    write_immutable_manifest(client, uri, document)
    write_immutable_manifest(client, uri, document)
    loaded = read_manifest(
        client,
        uri,
        expected_sha256=document["manifest_sha256"],
        expected_batch_id="phase1-test",
    )
    assert loaded == document["payload"]

    conflicting = manifest_fixture()
    conflicting["payload"]["created_at"] = "2026-07-22T00:00:00+00:00"
    conflicting = build_manifest_document(conflicting["payload"])
    try:
        write_immutable_manifest(client, uri, conflicting)
    except ManifestError as error:
        assert "different bytes" in str(error)
    else:
        raise AssertionError("Conflicting manifest overwrite unexpectedly succeeded")


def test_manifest_uri_is_stable_and_does_not_embed_unsafe_run_id_characters():
    first = manifest_uri("manual__2026-07-21T12:34:56+00:00 / retry")
    second = manifest_uri("manual__2026-07-21T12:34:56+00:00 / retry")

    assert first == second
    assert first.startswith("s3://lakehouse/bronze/_control/manifests/")
    assert " " not in first and "+" not in first


def test_state_machine_allows_only_declared_transitions():
    assert ("FAILED", "RUNNING") in ALLOWED_BATCH_TRANSITIONS
    for transition in ALLOWED_BATCH_TRANSITIONS:
        validate_transition(*transition)

    try:
        validate_transition("PUBLISHED", "RUNNING")
    except ControlPlaneError as error:
        assert "Invalid batch transition" in str(error)
    else:
        raise AssertionError("Invalid state transition unexpectedly succeeded")


def test_control_database_is_separate_and_queryable_from_trino():
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    service = compose["services"]["pipeline-control-postgres"]

    assert service["image"] == "postgres:15-alpine"
    assert "pipeline-control-db-volume:/var/lib/postgresql/data" in service["volumes"]
    assert compose["services"]["trino"]["depends_on"]["pipeline-control-postgres"]
    catalog = (ROOT / "trino/etc/catalog/pipeline_control.properties").read_text()
    assert "connector.name=postgresql" in catalog
    assert "pipeline-control-postgres:5432/pipeline_control" in catalog


def test_control_schema_retains_batch_attempt_task_and_transition_history():
    sql = (ROOT / "pipeline_control/init/001_schema.sql").read_text(encoding="utf-8")
    for table in (
        "pipeline_batch",
        "pipeline_batch_partition",
        "pipeline_attempt",
        "pipeline_task_evidence",
        "pipeline_batch_state_event",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table}" in sql
    assert "uq_pipeline_batch_unfinished" in sql
    assert "v_pipeline_batch_history" in sql


def test_spark_reader_uses_exact_object_paths_for_bounded_manifests():
    script = (ROOT / "scripts/4_process_cdc_raw_vault.py").read_text(encoding="utf-8")

    assert "bounded_object_paths" in script
    assert "return spark.read.json(paths)" in script
    assert "selected_input_objects=0" in script
    assert "BOUNDED_BATCH_TO_TARGET_MISSING_KEYS" in script


def test_legacy_v1_manifest_remains_replayable():
    payload = {
        "schema_version": 1,
        "batch_id": "legacy-replay",
        "source_adapter": "postgres-debezium",
        "schema_contract_version": "cdc-envelope-v1",
        "created_at": "2026-07-21T12:00:00+00:00",
        "interval_semantics": "(watermark_low, watermark_high]",
        "reader_mode": LEGACY_READER_MODE,
        "partitions": [
            {
                "topic": "bank.core.krd.loans",
                "partition": 0,
                "watermark_low": -1,
                "watermark_high": 2,
                "interval_object_count": 3,
            }
        ],
    }

    assert validate_manifest_document(build_manifest_document(payload)) == payload


def test_canonical_manifest_json_is_stable():
    document = manifest_fixture()
    reparsed = json.loads(canonical_json(document))

    assert reparsed == document
    assert canonical_json(document) == canonical_json(reparsed)


if __name__ == "__main__":
    tests = [
        value
        for name, value in sorted(globals().items())
        if name.startswith("test_") and callable(value)
    ]
    for test in tests:
        test()
        print(f"PASS {test.__name__}")
    print(f"PASS {len(tests)} phase 1 control-plane tests")

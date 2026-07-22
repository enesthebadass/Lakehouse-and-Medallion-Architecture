"""Build, persist, and verify immutable CDC offset manifests."""

from __future__ import annotations

import hashlib
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping
from urllib.parse import urlparse

MANIFEST_SCHEMA_VERSION = 3
SUPPORTED_MANIFEST_SCHEMA_VERSIONS = frozenset({1, 2, MANIFEST_SCHEMA_VERSION})
BOUNDED_READER_MODE = "bounded_object_list"
LEGACY_READER_MODE = "history_snapshot_capped_at_high"
BRONZE_KEY_PATTERN = re.compile(
    r"(?:^|/)topic=(?P<topic>[^/]+)/partition=(?P<partition>\d+)/"
    r"offset=(?P<offset>\d+)\.json$"
)


class ManifestError(RuntimeError):
    """Raised when a manifest cannot be created or verified safely."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def canonical_json(value: Mapping[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def payload_sha256(payload: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(payload)).hexdigest()


def parse_bronze_coordinate(object_key: str) -> tuple[str, int, int] | None:
    match = BRONZE_KEY_PATTERN.search(object_key)
    if not match:
        return None
    return (
        match.group("topic"),
        int(match.group("partition")),
        int(match.group("offset")),
    )


def discover_bronze_coordinates(
    s3_client: Any,
    bucket: str,
    prefix: str,
) -> list[tuple[str, int, int]]:
    coordinates = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        for item in page.get("Contents", []):
            coordinate = parse_bronze_coordinate(str(item["Key"]))
            if coordinate is not None:
                coordinates.append(coordinate)
    if not coordinates:
        raise ManifestError(f"No Bronze CDC coordinates found at s3://{bucket}/{prefix}")
    if len(coordinates) != len(set(coordinates)):
        raise ManifestError("Duplicate topic/partition/offset coordinates found in Bronze")
    return sorted(coordinates)


def discover_bronze_objects(
    s3_client: Any,
    bucket: str,
    prefix: str,
) -> list[dict[str, Any]]:
    """Return immutable object identities and storage sizes for manifest sealing."""
    objects = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        for item in page.get("Contents", []):
            key = str(item["Key"])
            coordinate = parse_bronze_coordinate(key)
            if coordinate is None:
                continue
            topic, partition, offset = coordinate
            objects.append(
                {
                    "object_key": key,
                    "size_bytes": int(item["Size"]),
                    "storage_etag": str(item.get("ETag", "")).strip('"'),
                    "topic": topic,
                    "partition": partition,
                    "offset": offset,
                }
            )
    if not objects:
        raise ManifestError(f"No Bronze CDC objects found at s3://{bucket}/{prefix}")
    coordinates = [
        (item["topic"], item["partition"], item["offset"]) for item in objects
    ]
    if len(coordinates) != len(set(coordinates)):
        raise ManifestError("Duplicate topic/partition/offset coordinates found in Bronze")
    if len(objects) != len({item["object_key"] for item in objects}):
        raise ManifestError("Duplicate Bronze object keys found during manifest discovery")
    return sorted(
        objects,
        key=lambda item: (
            item["topic"],
            item["partition"],
            item["offset"],
            item["object_key"],
        ),
    )


def build_manifest_payload(
    *,
    batch_id: str,
    objects: Iterable[Mapping[str, Any]],
    previous_highs: Mapping[tuple[str, int], int],
    source_adapter: str,
    schema_contract_version: str,
    source_control: Mapping[str, Any] | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    grouped: dict[tuple[str, int], list[dict[str, Any]]] = defaultdict(list)
    for item in objects:
        key = str(item["object_key"])
        coordinate = parse_bronze_coordinate(key)
        if coordinate is None:
            raise ManifestError(f"Bronze object key has no immutable coordinate: {key}")
        topic, partition, offset = coordinate
        grouped[(topic, partition)].append(
            {
                "object_key": key,
                "size_bytes": int(item["size_bytes"]),
                "storage_etag": str(item.get("storage_etag", "")),
                "topic": topic,
                "partition": partition,
                "offset": offset,
            }
        )
    if not grouped:
        raise ManifestError("A manifest requires at least one Bronze object")

    bounds = []
    bounded_objects = []
    for topic_partition in sorted(grouped):
        topic, partition = topic_partition
        partition_objects = sorted(
            grouped[topic_partition],
            key=lambda item: (item["offset"], item["object_key"]),
        )
        offsets = [item["offset"] for item in partition_objects]
        if len(offsets) != len(set(offsets)):
            raise ManifestError(
                f"Duplicate offsets found for Bronze partition {topic}/{partition}"
            )
        low = int(previous_highs.get(topic_partition, -1))
        high = offsets[-1]
        if high < low:
            raise ManifestError(
                f"Bronze high watermark regressed for {topic}/{partition}: {high} < {low}"
            )
        selected = [item for item in partition_objects if low < item["offset"] <= high]
        bounded_objects.extend(selected)
        bounds.append(
            {
                "topic": topic,
                "partition": partition,
                "watermark_low": low,
                "watermark_high": high,
                "interval_object_count": len(selected),
                "interval_input_bytes": sum(item["size_bytes"] for item in selected),
            }
        )

    payload = {
        "schema_version": 3 if source_control is not None else 2,
        "batch_id": batch_id,
        "source_adapter": source_adapter,
        "schema_contract_version": schema_contract_version,
        "created_at": created_at or utc_now(),
        "interval_semantics": "(watermark_low, watermark_high]",
        "reader_mode": BOUNDED_READER_MODE,
        "partitions": bounds,
        "objects": [
            {
                "object_key": item["object_key"],
                "size_bytes": item["size_bytes"],
                "storage_etag": item["storage_etag"],
            }
            for item in bounded_objects
        ],
        "interval_object_count": len(bounded_objects),
        "interval_input_bytes": sum(item["size_bytes"] for item in bounded_objects),
    }
    if source_control is not None:
        payload["source_control"] = dict(source_control)
    return payload


def build_manifest_document(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "manifest_sha256": payload_sha256(payload),
        "payload": dict(payload),
    }


def validate_manifest_document(
    document: Mapping[str, Any],
    *,
    expected_sha256: str | None = None,
    expected_batch_id: str | None = None,
) -> dict[str, Any]:
    if set(document) != {"manifest_sha256", "payload"}:
        raise ManifestError("Manifest document must contain only manifest_sha256 and payload")
    payload = document["payload"]
    if not isinstance(payload, dict):
        raise ManifestError("Manifest payload must be an object")
    actual_sha256 = payload_sha256(payload)
    if document["manifest_sha256"] != actual_sha256:
        raise ManifestError("Manifest payload checksum does not match the document")
    if expected_sha256 and actual_sha256 != expected_sha256:
        raise ManifestError("Manifest checksum does not match the orchestrator value")
    if expected_batch_id and payload.get("batch_id") != expected_batch_id:
        raise ManifestError("Manifest batch_id does not match the orchestrator value")
    schema_version = payload.get("schema_version")
    if schema_version not in SUPPORTED_MANIFEST_SCHEMA_VERSIONS:
        raise ManifestError("Unsupported manifest schema version")
    if payload.get("interval_semantics") != "(watermark_low, watermark_high]":
        raise ManifestError("Unsupported watermark interval semantics")

    partitions = payload.get("partitions")
    if not isinstance(partitions, list) or not partitions:
        raise ManifestError("Manifest partitions must be a non-empty list")
    identities = []
    for bound in partitions:
        if not isinstance(bound, dict):
            raise ManifestError("Manifest partition bound must be an object")
        identity = (bound.get("topic"), bound.get("partition"))
        identities.append(identity)
        low = bound.get("watermark_low")
        high = bound.get("watermark_high")
        if not isinstance(identity[0], str) or not identity[0]:
            raise ManifestError("Manifest topic must be a non-empty string")
        if not isinstance(identity[1], int) or identity[1] < 0:
            raise ManifestError("Manifest partition must be a non-negative integer")
        if not isinstance(low, int) or not isinstance(high, int) or high < low:
            raise ManifestError("Manifest watermark bounds are invalid")
    if identities != sorted(set(identities)):
        raise ManifestError("Manifest partitions must be unique and sorted")

    reader_mode = payload.get("reader_mode")
    if schema_version == 1:
        if reader_mode != LEGACY_READER_MODE:
            raise ManifestError("Legacy manifest has an unsupported reader mode")
        return payload
    if reader_mode != BOUNDED_READER_MODE:
        raise ManifestError("Bounded manifest has an unsupported reader mode")

    objects = payload.get("objects")
    if not isinstance(objects, list):
        raise ManifestError("Bounded manifest objects must be a list")
    bound_by_identity = {
        (bound["topic"], bound["partition"]): bound for bound in partitions
    }
    observed: dict[tuple[str, int], dict[str, int]] = defaultdict(
        lambda: {"count": 0, "bytes": 0}
    )
    object_order = []
    object_keys = []
    coordinates = []
    for item in objects:
        if not isinstance(item, dict):
            raise ManifestError("Bounded manifest object descriptor must be an object")
        key = item.get("object_key")
        size = item.get("size_bytes")
        etag = item.get("storage_etag")
        if not isinstance(key, str) or not key:
            raise ManifestError("Bounded manifest object key must be a non-empty string")
        if not isinstance(size, int) or size < 0:
            raise ManifestError("Bounded manifest object size must be non-negative")
        if not isinstance(etag, str):
            raise ManifestError("Bounded manifest object ETag must be a string")
        coordinate = parse_bronze_coordinate(key)
        if coordinate is None:
            raise ManifestError(f"Bounded manifest object key has no coordinate: {key}")
        topic, partition, offset = coordinate
        bound = bound_by_identity.get((topic, partition))
        if bound is None or not (
            bound["watermark_low"] < offset <= bound["watermark_high"]
        ):
            raise ManifestError(f"Object coordinate is outside its manifest bound: {key}")
        observed[(topic, partition)]["count"] += 1
        observed[(topic, partition)]["bytes"] += size
        object_order.append((topic, partition, offset, key))
        object_keys.append(key)
        coordinates.append(coordinate)
    if object_order != sorted(object_order):
        raise ManifestError("Bounded manifest objects must be sorted by coordinate")
    if len(object_keys) != len(set(object_keys)):
        raise ManifestError("Bounded manifest object keys must be unique")
    if len(coordinates) != len(set(coordinates)):
        raise ManifestError("Bounded manifest object coordinates must be unique")

    for identity, bound in bound_by_identity.items():
        if bound.get("interval_object_count") != observed[identity]["count"]:
            raise ManifestError("Partition interval object count does not match object list")
        if bound.get("interval_input_bytes") != observed[identity]["bytes"]:
            raise ManifestError("Partition interval byte count does not match object list")
    if payload.get("interval_object_count") != len(objects):
        raise ManifestError("Manifest interval object count does not match object list")
    if payload.get("interval_input_bytes") != sum(
        int(item["size_bytes"]) for item in objects
    ):
        raise ManifestError("Manifest interval byte count does not match object list")
    if schema_version == 3:
        _validate_source_control(payload.get("source_control"))
    return payload


def _validate_source_control(value: Any) -> None:
    if not isinstance(value, dict):
        raise ManifestError("Manifest v3 requires a source_control object")
    if value.get("contract_version") != "postgres-workload-ledger-v1":
        raise ManifestError("Unsupported source control contract")
    if value.get("source_position_type") != "postgres_lsn":
        raise ManifestError("Unsupported source position type")
    for name in (
        "expected_event_count",
        "observed_event_count",
        "untracked_event_count",
    ):
        if not isinstance(value.get(name), int) or value[name] < 0:
            raise ManifestError(f"Source control {name} must be non-negative")
    transactions = value.get("transactions")
    if not isinstance(transactions, list):
        raise ManifestError("Source control transactions must be a list")
    transaction_ids = []
    expected_total = 0
    observed_total = 0
    for item in transactions:
        if not isinstance(item, dict):
            raise ManifestError("Source control transaction must be an object")
        source_txid = item.get("source_txid")
        expected = item.get("expected_event_count")
        observed = item.get("observed_event_count")
        boundary = item.get("source_boundary_position")
        if not isinstance(source_txid, int) or source_txid < 0:
            raise ManifestError("Source control transaction ID must be non-negative")
        if not isinstance(expected, int) or expected < 0:
            raise ManifestError("Source transaction expected count must be non-negative")
        if not isinstance(observed, int) or observed < 0:
            raise ManifestError("Source transaction observed count must be non-negative")
        if not isinstance(boundary, int) or boundary < 0:
            raise ManifestError("Source transaction boundary must be non-negative")
        transaction_ids.append(source_txid)
        expected_total += expected
        observed_total += observed
    if transaction_ids != sorted(set(transaction_ids)):
        raise ManifestError("Source control transactions must be unique and sorted")
    if expected_total != value["expected_event_count"]:
        raise ManifestError("Source control expected total does not match transactions")
    if observed_total != value["observed_event_count"]:
        raise ManifestError("Source control observed total does not match transactions")


def summarize_bronze_transactions(
    client: Any,
    bucket: str,
    objects: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Read only selected objects and summarize immutable source transaction identity."""
    transactions: dict[int, dict[str, Any]] = {}
    untracked_event_count = 0
    table_event_counts: dict[str, int] = defaultdict(int)
    operation_counts: dict[str, int] = defaultdict(int)
    for item in objects:
        key = str(item["object_key"])
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        try:
            document = json.loads(body)
        except (TypeError, json.JSONDecodeError) as error:
            raise ManifestError(f"Bronze object is not valid JSON: {key}") from error
        metadata = document.get("_metadata")
        value = document.get("value")
        if not isinstance(metadata, dict) or metadata.get("object_key") != key:
            raise ManifestError(f"Bronze object metadata identity mismatch: {key}")
        source = value.get("source") if isinstance(value, dict) else None
        source_txid = source.get("txId") if isinstance(source, dict) else None
        source_lsn = source.get("lsn") if isinstance(source, dict) else None
        table_name = f"{metadata.get('source_schema')}.{metadata.get('source_table')}"
        table_event_counts[table_name] += 1
        operation_counts[str(metadata.get("operation"))] += 1
        if source_txid is None:
            untracked_event_count += 1
            continue
        try:
            txid = int(source_txid)
            lsn = int(source_lsn)
        except (TypeError, ValueError) as error:
            raise ManifestError(f"Invalid source transaction position in Bronze object: {key}") from error
        summary = transactions.setdefault(
            txid,
            {
                "source_txid": txid,
                "observed_event_count": 0,
                "event_lsn_low": lsn,
                "event_lsn_high": lsn,
            },
        )
        summary["observed_event_count"] += 1
        summary["event_lsn_low"] = min(summary["event_lsn_low"], lsn)
        summary["event_lsn_high"] = max(summary["event_lsn_high"], lsn)
    return {
        "transactions": [transactions[key] for key in sorted(transactions)],
        "untracked_event_count": untracked_event_count,
        "table_event_counts": dict(sorted(table_event_counts.items())),
        "operation_counts": dict(sorted(operation_counts.items())),
    }


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc or not parsed.path.strip("/"):
        raise ManifestError(f"Invalid S3 manifest URI: {uri!r}")
    return parsed.netloc, parsed.path.lstrip("/")


def s3_client() -> Any:
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "minioadmin"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin"),
        region_name=os.getenv("AWS_REGION", "us-east-1"),
    )


def _is_missing_object(error: Exception) -> bool:
    response = getattr(error, "response", {})
    code = str(response.get("Error", {}).get("Code", ""))
    return code in {"404", "NoSuchKey", "NotFound"}


def write_immutable_manifest(
    client: Any,
    uri: str,
    document: Mapping[str, Any],
) -> None:
    bucket, key = parse_s3_uri(uri)
    body = canonical_json(document) + b"\n"
    try:
        existing = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as error:
        if not _is_missing_object(error):
            raise
    else:
        if existing != body:
            raise ManifestError(f"Immutable manifest already exists with different bytes: {uri}")
        return

    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
        Metadata={"manifest-sha256": str(document["manifest_sha256"])},
    )
    persisted = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    if persisted != body:
        raise ManifestError(f"Manifest read-after-write verification failed: {uri}")


def write_immutable_json(client: Any, uri: str, value: Mapping[str, Any]) -> str:
    bucket, key = parse_s3_uri(uri)
    body = canonical_json(value) + b"\n"
    checksum = hashlib.sha256(canonical_json(value)).hexdigest()
    try:
        existing = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as error:
        if not _is_missing_object(error):
            raise
    else:
        if existing != body:
            raise ManifestError(f"Immutable JSON evidence conflicts with existing bytes: {uri}")
        return checksum
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json",
        Metadata={"evidence-sha256": checksum},
    )
    persisted = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    if persisted != body:
        raise ManifestError(f"JSON evidence read-after-write verification failed: {uri}")
    return checksum


def read_immutable_json(
    client: Any,
    uri: str,
    *,
    expected_sha256: str | None = None,
) -> tuple[dict[str, Any], str]:
    bucket, key = parse_s3_uri(uri)
    body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    try:
        value = json.loads(body)
    except (TypeError, json.JSONDecodeError) as error:
        raise ManifestError(f"Immutable JSON evidence is not valid JSON: {uri}") from error
    if not isinstance(value, dict):
        raise ManifestError("Immutable JSON evidence root must be an object")
    checksum = hashlib.sha256(canonical_json(value)).hexdigest()
    if expected_sha256 is not None and checksum != expected_sha256:
        raise ManifestError("Immutable JSON evidence checksum mismatch")
    if body != canonical_json(value) + b"\n":
        raise ManifestError("Immutable JSON evidence is not canonically encoded")
    return value, checksum


def read_manifest(
    client: Any,
    uri: str,
    *,
    expected_sha256: str | None = None,
    expected_batch_id: str | None = None,
) -> dict[str, Any]:
    bucket, key = parse_s3_uri(uri)
    body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    try:
        document = json.loads(body)
    except (TypeError, json.JSONDecodeError) as error:
        raise ManifestError(f"Manifest is not valid JSON: {uri}") from error
    return validate_manifest_document(
        document,
        expected_sha256=expected_sha256,
        expected_batch_id=expected_batch_id,
    )

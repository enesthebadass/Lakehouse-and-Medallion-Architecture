"""Persist Debezium Kafka records to immutable, replay-safe MinIO Bronze objects."""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import signal
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import PurePosixPath
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from confluent_kafka.admin import AdminClient

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "bronze-cdc-writer-v1")
KAFKA_TOPIC_PATTERN = os.getenv(
    "KAFKA_TOPIC_PATTERN",
    r"^bank\.core\.(mms|MMS|krd|KRD|prm|PRM)\..+$",
)
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET = os.getenv("S3_BUCKET", "lakehouse")
S3_PREFIX = os.getenv("S3_PREFIX", "bronze/cdc")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
SOURCE_SYSTEM = os.getenv("SOURCE_SYSTEM", "core_banking")
ALLOWED_SCHEMAS = frozenset(
    item.strip()
    for item in os.getenv("ALLOWED_SCHEMAS", "mms,krd,prm").split(",")
    if item.strip()
)

logger = logging.getLogger("bronze-cdc-writer")


@dataclass(frozen=True)
class BronzeRecord:
    object_key: str
    event_id: str
    payload_sha256: str
    body: bytes
    source_schema: str
    source_table: str
    operation: str


def configure_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(message)s"))
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())


def log_event(level: int, event: str, **fields: Any) -> None:
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": logging.getLevelName(level),
        "event": event,
        **fields,
    }
    logger.log(level, json.dumps(payload, ensure_ascii=True, default=str))


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def json_value(raw: bytes | None, field_name: str) -> Any:
    if raw is None:
        return None
    try:
        return json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Kafka {field_name} is not valid UTF-8 JSON") from exc


def safe_path_component(value: str, field_name: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", value):
        raise ValueError(f"Unsafe {field_name}: {value!r}")
    return value


def topic_source(topic: str) -> tuple[str, str]:
    parts = topic.split(".")
    if len(parts) < 4:
        raise ValueError(f"Cannot derive schema and table from topic {topic!r}")
    return parts[-2].lower(), parts[-1].lower()


def event_datetime(value: Any, kafka_timestamp_ms: int | None) -> datetime:
    source_ts_ms = None
    if isinstance(value, dict) and isinstance(value.get("source"), dict):
        source_ts_ms = value["source"].get("ts_ms")
    for candidate in (source_ts_ms, kafka_timestamp_ms):
        if candidate is not None:
            try:
                return datetime.fromtimestamp(int(candidate) / 1000, tz=timezone.utc)
            except (TypeError, ValueError, OSError):
                continue
    # A deterministic fallback keeps the object key stable even when a record has
    # neither a Debezium source timestamp nor a Kafka timestamp.
    return datetime(1970, 1, 1, tzinfo=timezone.utc)


def build_record(message: Message, ingested_at: datetime | None = None) -> BronzeRecord:
    topic = safe_path_component(message.topic(), "Kafka topic")
    partition = message.partition()
    offset = message.offset()
    raw_key = message.key()
    raw_value = message.value()
    key = json_value(raw_key, "key")
    value = json_value(raw_value, "value")

    if value is None:
        source_schema, source_table = topic_source(topic)
        operation = "tombstone"
    else:
        if not isinstance(value, dict):
            raise ValueError("Debezium value must be a JSON object or tombstone")
        required_fields = {"before", "after", "op", "source"}
        missing_fields = sorted(required_fields.difference(value))
        if missing_fields:
            raise ValueError(f"Debezium envelope is missing fields: {missing_fields}")
        source = value["source"]
        if not isinstance(source, dict):
            raise ValueError("Debezium source metadata must be a JSON object")
        source_schema = source.get("schema")
        source_table = source.get("table")
        operation = value.get("op")
        if not all(
            isinstance(item, str) and item
            for item in (source_schema, source_table, operation)
        ):
            raise ValueError(
                "Debezium schema, table and operation must be non-empty strings"
            )
        source_schema = source_schema.lower()
        source_table = source_table.lower()

    source_schema = safe_path_component(source_schema, "source schema")
    source_table = safe_path_component(source_table, "source table")
    if source_schema not in ALLOWED_SCHEMAS:
        raise ValueError(f"Source schema {source_schema!r} is not allowed")

    kafka_timestamp_ms = message.timestamp()[1]
    if kafka_timestamp_ms is not None and kafka_timestamp_ms < 0:
        kafka_timestamp_ms = None
    event_time = event_datetime(value, kafka_timestamp_ms)
    ingestion_time = ingested_at or datetime.now(timezone.utc)
    event_coordinate = f"{topic}:{partition}:{offset}"
    event_id = hashlib.sha256(event_coordinate.encode("utf-8")).hexdigest()
    payload_sha256 = hashlib.sha256(
        (raw_key or b"") + b"\x00" + (raw_value or b"")
    ).hexdigest()

    object_key = str(
        PurePosixPath(S3_PREFIX)
        / f"source={safe_path_component(SOURCE_SYSTEM, 'source system')}"
        / f"schema={source_schema}"
        / f"table={source_table}"
        / f"event_date={event_time.date().isoformat()}"
        / f"topic={topic}"
        / f"partition={partition}"
        / f"offset={offset:020d}.json"
    )
    document = {
        "_metadata": {
            "event_id": event_id,
            "source_system": SOURCE_SYSTEM,
            "source_schema": source_schema,
            "source_table": source_table,
            "operation": operation,
            "kafka_topic": topic,
            "kafka_partition": partition,
            "kafka_offset": offset,
            "kafka_timestamp_ms": kafka_timestamp_ms,
            "event_timestamp": event_time.isoformat(),
            "ingested_at": ingestion_time.isoformat(),
            "payload_sha256": payload_sha256,
            "consumer_group": KAFKA_CONSUMER_GROUP,
            "object_key": object_key,
        },
        "key": key,
        "value": value,
    }
    body = (json.dumps(document, ensure_ascii=False, sort_keys=True) + "\n").encode(
        "utf-8"
    )
    return BronzeRecord(
        object_key=object_key,
        event_id=event_id,
        payload_sha256=payload_sha256,
        body=body,
        source_schema=source_schema,
        source_table=source_table,
        operation=operation,
    )


def is_precondition_failure(exc: ClientError) -> bool:
    response = exc.response
    return (
        response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 412
        or response.get("Error", {}).get("Code") == "PreconditionFailed"
    )


def verify_existing_object(client, record: BronzeRecord) -> None:
    response = client.head_object(Bucket=S3_BUCKET, Key=record.object_key)
    metadata = response.get("Metadata", {})
    if (
        metadata.get("event-id") != record.event_id
        or metadata.get("payload-sha256") != record.payload_sha256
    ):
        raise RuntimeError(
            f"Existing object conflicts with Kafka event at s3://{S3_BUCKET}/{record.object_key}"
        )


def persist_record(client, record: BronzeRecord) -> bool:
    try:
        client.put_object(
            Bucket=S3_BUCKET,
            Key=record.object_key,
            Body=record.body,
            ContentType="application/json",
            Metadata={
                "event-id": record.event_id,
                "payload-sha256": record.payload_sha256,
            },
            IfNoneMatch="*",
        )
        return True
    except ClientError as exc:
        if not is_precondition_failure(exc):
            raise
        verify_existing_object(client, record)
        return False


class BronzeWriter:
    def __init__(self) -> None:
        self.running = True
        self.client = s3_client()
        self.consumer = Consumer(
            {
                "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
                "group.id": KAFKA_CONSUMER_GROUP,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "auto.offset.reset": "earliest",
                "client.id": "bronze-cdc-writer",
                "topic.metadata.refresh.interval.ms": 10000,
            }
        )

    def stop(self, _signum: int, _frame: Any) -> None:
        self.running = False

    def check_dependencies(self) -> None:
        self.client.head_bucket(Bucket=S3_BUCKET)
        AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}).list_topics(
            timeout=10
        )
        with open("/tmp/healthy", "w", encoding="ascii") as health_file:
            health_file.write(datetime.now(timezone.utc).isoformat())

    def run(self) -> None:
        self.check_dependencies()
        self.consumer.subscribe([KAFKA_TOPIC_PATTERN])
        log_event(
            logging.INFO,
            "writer_started",
            consumer_group=KAFKA_CONSUMER_GROUP,
            topic_pattern=KAFKA_TOPIC_PATTERN,
            bucket=S3_BUCKET,
            prefix=S3_PREFIX,
        )
        try:
            while self.running:
                message = self.consumer.poll(1.0)
                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(message.error())

                record = build_record(message)
                created = persist_record(self.client, record)
                self.consumer.commit(message=message, asynchronous=False)
                log_event(
                    logging.INFO,
                    (
                        "bronze_object_created"
                        if created
                        else "bronze_object_already_exists"
                    ),
                    topic=message.topic(),
                    partition=message.partition(),
                    offset=message.offset(),
                    object_key=record.object_key,
                    operation=record.operation,
                )
        finally:
            self.consumer.close()
            log_event(logging.INFO, "writer_stopped")


def main() -> None:
    configure_logging()
    writer = BronzeWriter()
    signal.signal(signal.SIGTERM, writer.stop)
    signal.signal(signal.SIGINT, writer.stop)
    try:
        writer.run()
    except Exception as exc:
        log_event(
            logging.ERROR,
            "writer_failed",
            error_type=type(exc).__name__,
            error=str(exc),
        )
        raise


if __name__ == "__main__":
    main()

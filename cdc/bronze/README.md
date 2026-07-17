# Bronze CDC Landing Contract

`bronze-cdc-writer` consumes the allowlisted Debezium table topics and persists each
Kafka record as one raw JSON object in MinIO. It performs no business transformation.
The complete Debezium value and Kafka key are retained, including delete tombstones.

## Object Layout

Objects use this deterministic layout:

```text
bronze/cdc/
  source=core_banking/
    schema=<schema>/
      table=<table>/
        event_date=<UTC date>/
          topic=<Kafka topic>/
            partition=<partition>/
              offset=<20-digit offset>.json
```

Example:

```text
bronze/cdc/source=core_banking/schema=mms/table=customers/
event_date=2026-07-17/topic=bank.core.mms.customers/partition=0/
offset=00000000000000000100.json
```

Each object contains:

- `_metadata`: event ID, source, operation, event and ingestion timestamps, Kafka
  topic/partition/offset, payload checksum, consumer group, and object key
- `key`: the original Debezium Kafka key
- `value`: the complete original Debezium envelope, or `null` for a tombstone

The event ID is the SHA-256 hash of `topic:partition:offset`. The object metadata also
stores the event ID and a checksum of the original Kafka key and value bytes.

## Delivery And Failure Semantics

Kafka auto-commit is disabled. For every record, the writer:

1. creates the deterministic object with the conditional `If-None-Match: *` request;
2. commits the Kafka offset only after MinIO confirms the write;
3. if the object already exists, verifies its event ID and payload checksum before
   treating the replay as successful.

A crash after the MinIO write but before the Kafka commit therefore replays the same
offset without creating a second object. A conflicting object at the same coordinate
stops processing instead of silently overwriting raw data. This is application-level
immutability for the local pilot; production storage still needs access controls,
versioning/object lock decisions, retention, encryption, and audit policies.

## Operations

Check service health and recent writes:

```bash
docker compose ps bronze-cdc-writer
docker compose logs --tail=20 bronze-cdc-writer
```

Inspect consumer lag:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --group bronze-cdc-writer-v1 \
  --describe
```

Count Bronze CDC objects:

```bash
docker compose exec minio sh -c \
  "mc alias set demo http://localhost:9000 minioadmin minioadmin >/dev/null && \
   mc find demo/lakehouse/bronze/cdc --name '*.json' | wc -l"
```

## Replay Procedure

The consumer must be stopped before its committed offsets are reset:

```bash
docker compose stop bronze-cdc-writer

docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:9092 \
  --group bronze-cdc-writer-v1 \
  --reset-offsets --to-earliest --all-topics --execute

docker compose start bronze-cdc-writer
```

After the writer catches up, the object count must be unchanged, consumer lag must be
zero, and logs must contain `bronze_object_already_exists`. This demonstrates duplicate
suppression for Kafka replay.

One object per event is intentionally simple and auditable for this local pilot. A
production design must address small-file compaction separately without mutating or
losing the raw event history.

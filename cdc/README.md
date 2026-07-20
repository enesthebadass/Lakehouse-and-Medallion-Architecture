# Local CDC Contract

This directory contains the local PostgreSQL Debezium connector contract. It is a
development proof of the CDC pattern, not a production Kafka deployment or proof of
Oracle LogMiner readiness.

The local runtime is pinned to Apache Kafka `4.0.0` and an immutable Debezium
container digest containing Debezium `3.2.4.Final` with Kafka Connect `4.0.0`.

## Captured Data

Only the explicitly allowlisted business schemas are captured:

- `mms`: customers, addresses, contacts, and customer relations
- `krd`: applications, loans, installments, and collaterals
- `prm`: currencies, branches, products, status codes, and rate parameters

The `simulator` control schema and Airflow metadata database are excluded.

## Topic And Event Contract

Source table topics use this format:

```text
bank.core.<schema>.<table>
```

Examples:

```text
bank.core.mms.customers
bank.core.krd.loans
bank.core.prm.rate_parameters
```

The local broker uses one partition and seven-day delete retention for CDC topics.
Debezium serializes the source primary key as the Kafka message key, so changes for
the same row are routed consistently. Production partition counts and retention must
be sized from volume, replay, recovery, and regulatory requirements.

Values are schema-less JSON but retain the Debezium envelope, including:

- `before` and `after` row images
- `op`: `r` snapshot read, `c` create, `u` update, or `d` delete
- `source`: connector, database, schema, table, timestamp, LSN, and snapshot metadata
- `transaction`: transaction ID and ordering metadata when available
- event processing timestamps such as `ts_ms`

Decimal values are emitted as exact strings, and source temporal values are emitted
as ISO-8601 strings. This avoids floating-point loss and opaque epoch-day values in
the raw Bronze contract.

PostgreSQL tables use `REPLICA IDENTITY FULL` in this local pilot so complete old row
images are available for updates and deletes. This increases WAL volume and requires
DBA review before production use.

## Operations

Check Kafka Connect and the connector:

```bash
curl -fsS http://localhost:8083/connectors
curl -fsS http://localhost:8083/connectors/core-banking-postgres-cdc/status
```

List topics:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --list
```

Inspect a topic from the beginning:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic bank.core.mms.customers \
  --from-beginning \
  --max-messages 1 \
  --property print.key=true
```

Kafka Connect stores connector configuration, source offsets, and status in compacted
internal topics named `lakehouse_connect_configs`, `lakehouse_connect_offsets`, and
`lakehouse_connect_statuses`. The PostgreSQL replication slot is
`lakehouse_cdc_slot`; do not delete it while the connector is active.

Reapply an updated connector configuration idempotently:

```bash
docker compose run --rm debezium-init
```

The downstream `bronze-cdc-writer` contract, object layout, failure semantics, and
replay procedure are documented in [`bronze/README.md`](bronze/README.md).

The downstream Raw Vault business-key, hash, Hub, Link, Satellite, delete, quarantine,
and reconciliation contract is documented in
[`DATA_VAULT_MAPPING.md`](DATA_VAULT_MAPPING.md).

## Oracle Production Adapter Boundary

The PostgreSQL connector proves the local CDC and downstream idempotency pattern; it
does not validate Oracle LogMiner. The proposed production source adapter is defined
by `connectors/core-banking-oracle-logminer.template.json` and the machine-readable
readiness gates in `oracle-readiness.yaml`. It deliberately remains non-deployable
until Oracle DBA, source-owner, security, network, and license reviews are complete.

The complete Turkish readiness guide is available in
[`../ORACLE_CDC_READINESS.md`](../ORACLE_CDC_READINESS.md).

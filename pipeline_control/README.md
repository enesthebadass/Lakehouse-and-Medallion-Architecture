# CDC Batch Control Plane

The local production-shaped PoC keeps orchestration state outside both the Airflow
metadata database and the synthetic operational source. PostgreSQL stores current
batch state plus append-only attempt, task evidence, partition boundary, and state
transition history. MinIO stores the canonical immutable manifest.

For each topic and partition, the manifest records `(watermark_low,
watermark_high]`, source adapter, schema contract version, creation time, reader mode,
and a SHA-256 of the canonical payload. Version 2 records the exact object key,
storage ETag, and byte size for every selected event. Version 3 also freezes the
PostgreSQL workload-ledger transactions, LSN boundary, and expected/observed event
totals. Every Raw Vault Spark task
receives the same manifest URI and checksum through Airflow XCom.

Inspect the control plane through Trino:

```sql
SELECT *
FROM pipeline_control.public.v_pipeline_batch_history
ORDER BY created_at DESC;

SELECT batch_id, attempt_number, airflow_run_id, state, manifest_sha256
FROM pipeline_control.public.pipeline_attempt
ORDER BY started_at DESC;

SELECT batch_id, attempt_number, count(*) AS tasks,
       count(DISTINCT manifest_sha256) AS manifest_count,
       min(reader_mode) AS reader_mode,
       min(selected_input_object_count) AS selected_objects,
       min(selected_input_bytes) AS selected_bytes
FROM pipeline_control.public.pipeline_task_evidence
GROUP BY 1, 2;

SELECT batch_id, source_txid, workload_event_key,
       expected_event_count, observed_event_count, status
FROM pipeline_control.public.pipeline_batch_source_transaction
ORDER BY source_boundary_position DESC;

SELECT batch_id, attempt_number, state, rule_count, failed_rule_count,
       evidence_uri, evidence_sha256
FROM pipeline_control.public.pipeline_attempt_audit
ORDER BY recorded_at DESC;
```

Trigger a new batch:

```bash
docker compose exec airflow-webserver airflow dags trigger \
  --run-id phase1-example cdc_raw_vault_incremental
```

Replay the latest published manifest with a new attempt:

```bash
docker compose exec airflow-webserver airflow dags trigger \
  --run-id phase1-example-replay \
  --conf '{"batch_id":"phase1-example"}' \
  cdc_raw_vault_incremental
```

Only the latest published batch can be replayed. Its batch state remains
`PUBLISHED`; replay success or failure is recorded in `pipeline_attempt`. New batch
creation is blocked while an attempt is running.

New manifests use `bounded_object_list`: Spark opens only the exact `(low, high]`
objects and a zero-object batch exits before creating a Spark session. Legacy version
1 manifests remain replayable with `history_snapshot_capped_at_high`. Manifest sealing
still lists Bronze object metadata; a production inventory/index is a later scaling
gate. The local PostgreSQL service is not a production HA deployment; see
`docs/adr/0001-pipeline-control-plane.md` for ownership and external gates.

The reconciliation task writes one immutable JSON artifact per attempt under
`bronze/_control/audit/`. Publishing is blocked unless its source-ledger, manifest,
Kafka-coordinate, accepted/quarantine conservation, and Raw Vault effect rules all
pass. Audit rows are append-only at `(batch_id, attempt_number, rule_id, object_name)`.

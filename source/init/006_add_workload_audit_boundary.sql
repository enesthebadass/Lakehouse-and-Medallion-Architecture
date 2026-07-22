ALTER TABLE simulator.workload_events
    ADD COLUMN IF NOT EXISTS expected_cdc_event_count INTEGER
    CHECK (expected_cdc_event_count >= 0);

ALTER TABLE simulator.workload_events
    ADD COLUMN IF NOT EXISTS source_txid BIGINT;

ALTER TABLE simulator.workload_events
    ADD COLUMN IF NOT EXISTS source_boundary_lsn PG_LSN;

CREATE UNIQUE INDEX IF NOT EXISTS ux_workload_events_source_txid
    ON simulator.workload_events (source_txid)
    WHERE source_txid IS NOT NULL;

COMMENT ON COLUMN simulator.workload_events.expected_cdc_event_count IS
    'Expected Debezium data-change events committed by this synthetic source transaction.';
COMMENT ON COLUMN simulator.workload_events.source_txid IS
    'PostgreSQL transaction ID exposed by Debezium value.source.txId.';
COMMENT ON COLUMN simulator.workload_events.source_boundary_lsn IS
    'WAL insert position captured after source DML and before the transaction commit.';

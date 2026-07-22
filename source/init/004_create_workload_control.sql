CREATE SCHEMA IF NOT EXISTS simulator;

COMMENT ON SCHEMA simulator IS
    'Control metadata for the local synthetic workload generator; excluded from CDC capture.';

CREATE TABLE IF NOT EXISTS simulator.workload_runs (
    run_id VARCHAR(80) PRIMARY KEY,
    workload_type VARCHAR(20) NOT NULL
        CHECK (workload_type IN ('snapshot', 'changes', 'current-state')),
    random_seed INTEGER NOT NULL CHECK (random_seed >= 0),
    status VARCHAR(20) NOT NULL CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED')),
    config JSONB NOT NULL,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    CHECK (
        (status = 'COMPLETED' AND completed_at IS NOT NULL)
        OR status <> 'COMPLETED'
    )
);

CREATE TABLE IF NOT EXISTS simulator.workload_events (
    event_id VARCHAR(160) PRIMARY KEY,
    run_id VARCHAR(80) NOT NULL REFERENCES simulator.workload_runs (run_id),
    event_key VARCHAR(80) NOT NULL,
    scenario VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED')),
    expected_result TEXT NOT NULL,
    actual_result JSONB,
    expected_cdc_event_count INTEGER CHECK (expected_cdc_event_count >= 0),
    source_txid BIGINT,
    source_boundary_lsn PG_LSN,
    started_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMPTZ,
    error_message TEXT,
    UNIQUE (run_id, event_key),
    CHECK (
        (status = 'COMPLETED' AND completed_at IS NOT NULL AND actual_result IS NOT NULL)
        OR status <> 'COMPLETED'
    )
);

CREATE INDEX IF NOT EXISTS ix_workload_events_run_status
    ON simulator.workload_events (run_id, status);

CREATE UNIQUE INDEX IF NOT EXISTS ux_workload_events_source_txid
    ON simulator.workload_events (source_txid)
    WHERE source_txid IS NOT NULL;

COMMENT ON TABLE simulator.workload_runs IS
    'Idempotency and execution status for deterministic local source workloads.';
COMMENT ON TABLE simulator.workload_events IS
    'Expected and actual outcomes for each committed source transaction scenario.';

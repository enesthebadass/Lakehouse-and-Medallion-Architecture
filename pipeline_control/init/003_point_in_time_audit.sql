ALTER TABLE pipeline_batch
    ADD COLUMN IF NOT EXISTS source_position_type VARCHAR(50);
ALTER TABLE pipeline_batch
    ADD COLUMN IF NOT EXISTS source_boundary_high BIGINT;
ALTER TABLE pipeline_batch
    ADD COLUMN IF NOT EXISTS source_expected_event_count BIGINT NOT NULL DEFAULT 0;
ALTER TABLE pipeline_batch
    ADD COLUMN IF NOT EXISTS source_observed_event_count BIGINT NOT NULL DEFAULT 0;
ALTER TABLE pipeline_batch
    ADD COLUMN IF NOT EXISTS source_untracked_event_count BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS pipeline_batch_source_transaction (
    batch_id VARCHAR(250) NOT NULL REFERENCES pipeline_batch(batch_id),
    source_txid BIGINT NOT NULL,
    workload_run_id VARCHAR(80) NOT NULL,
    workload_event_key VARCHAR(80) NOT NULL,
    source_position_type VARCHAR(50) NOT NULL,
    source_boundary_position BIGINT NOT NULL CHECK (source_boundary_position >= 0),
    source_boundary_lsn TEXT NOT NULL,
    expected_event_count BIGINT NOT NULL CHECK (expected_event_count >= 0),
    observed_event_count BIGINT NOT NULL CHECK (observed_event_count >= 0),
    event_lsn_low BIGINT,
    event_lsn_high BIGINT,
    status VARCHAR(20) NOT NULL CHECK (status IN ('PASS', 'FAIL')),
    PRIMARY KEY (batch_id, source_txid)
);

CREATE TABLE IF NOT EXISTS pipeline_attempt_audit (
    batch_id VARCHAR(250) NOT NULL,
    attempt_number INTEGER NOT NULL,
    evidence_uri TEXT NOT NULL,
    evidence_sha256 CHAR(64) NOT NULL,
    rule_version VARCHAR(100) NOT NULL,
    state VARCHAR(20) NOT NULL CHECK (state IN ('PASS', 'FAIL')),
    rule_count INTEGER NOT NULL CHECK (rule_count > 0),
    failed_rule_count INTEGER NOT NULL CHECK (failed_rule_count >= 0),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_id, attempt_number),
    FOREIGN KEY (batch_id, attempt_number)
        REFERENCES pipeline_attempt(batch_id, attempt_number),
    CHECK (evidence_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS pipeline_audit_rule_result (
    batch_id VARCHAR(250) NOT NULL,
    attempt_number INTEGER NOT NULL,
    rule_id VARCHAR(150) NOT NULL,
    object_name VARCHAR(250) NOT NULL,
    rule_version VARCHAR(100) NOT NULL,
    scope VARCHAR(100) NOT NULL,
    expected_value BIGINT NOT NULL,
    observed_value BIGINT NOT NULL,
    difference BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('PASS', 'FAIL')),
    watermark JSONB NOT NULL,
    evidence_uri TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::JSONB,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_id, attempt_number, rule_id, object_name),
    FOREIGN KEY (batch_id, attempt_number)
        REFERENCES pipeline_attempt(batch_id, attempt_number)
);

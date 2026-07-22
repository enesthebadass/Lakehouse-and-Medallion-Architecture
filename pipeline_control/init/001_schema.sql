CREATE TABLE IF NOT EXISTS pipeline_batch (
    batch_id VARCHAR(250) PRIMARY KEY,
    airflow_run_id VARCHAR(250) NOT NULL,
    manifest_uri TEXT NOT NULL UNIQUE,
    manifest_sha256 CHAR(64) NOT NULL,
    source_adapter VARCHAR(100) NOT NULL,
    schema_contract_version VARCHAR(100) NOT NULL,
    source_position_type VARCHAR(50),
    source_boundary_high BIGINT,
    source_expected_event_count BIGINT NOT NULL DEFAULT 0,
    source_observed_event_count BIGINT NOT NULL DEFAULT 0,
    source_untracked_event_count BIGINT NOT NULL DEFAULT 0,
    state VARCHAR(20) NOT NULL CHECK (
        state IN ('CREATED', 'RUNNING', 'VALIDATED', 'PUBLISHED', 'FAILED', 'SUPERSEDED')
    ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ,
    CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_pipeline_batch_unfinished
    ON pipeline_batch ((1))
    WHERE state NOT IN ('PUBLISHED', 'SUPERSEDED');

CREATE TABLE IF NOT EXISTS pipeline_batch_partition (
    batch_id VARCHAR(250) NOT NULL REFERENCES pipeline_batch(batch_id),
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    watermark_low BIGINT NOT NULL,
    watermark_high BIGINT NOT NULL,
    interval_object_count BIGINT NOT NULL CHECK (interval_object_count >= 0),
    interval_input_bytes BIGINT NOT NULL DEFAULT 0 CHECK (interval_input_bytes >= 0),
    PRIMARY KEY (batch_id, topic, partition_id),
    CHECK (watermark_high >= watermark_low)
);

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

CREATE TABLE IF NOT EXISTS pipeline_attempt (
    batch_id VARCHAR(250) NOT NULL REFERENCES pipeline_batch(batch_id),
    attempt_number INTEGER NOT NULL CHECK (attempt_number > 0),
    airflow_run_id VARCHAR(250) NOT NULL UNIQUE,
    manifest_sha256 CHAR(64) NOT NULL,
    state VARCHAR(20) NOT NULL CHECK (state IN ('RUNNING', 'SUCCESS', 'FAILED')),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    error_message TEXT,
    PRIMARY KEY (batch_id, attempt_number),
    CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS pipeline_task_evidence (
    batch_id VARCHAR(250) NOT NULL,
    attempt_number INTEGER NOT NULL,
    task_id VARCHAR(250) NOT NULL,
    manifest_sha256 CHAR(64) NOT NULL,
    reader_mode VARCHAR(100) NOT NULL DEFAULT 'history_snapshot_capped_at_high',
    selected_input_object_count BIGINT NOT NULL DEFAULT 0 CHECK (
        selected_input_object_count >= 0
    ),
    selected_input_bytes BIGINT NOT NULL DEFAULT 0 CHECK (selected_input_bytes >= 0),
    state VARCHAR(20) NOT NULL CHECK (state IN ('SUCCESS', 'FAILED')),
    finished_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (batch_id, attempt_number, task_id),
    FOREIGN KEY (batch_id, attempt_number)
        REFERENCES pipeline_attempt(batch_id, attempt_number),
    CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS pipeline_batch_state_event (
    event_id BIGSERIAL PRIMARY KEY,
    batch_id VARCHAR(250) NOT NULL REFERENCES pipeline_batch(batch_id),
    attempt_number INTEGER,
    from_state VARCHAR(20),
    to_state VARCHAR(20) NOT NULL CHECK (
        to_state IN ('CREATED', 'RUNNING', 'VALIDATED', 'PUBLISHED', 'FAILED', 'SUPERSEDED')
    ),
    reason TEXT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
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

CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE IF NOT EXISTS pipeline_published_interval (
    batch_id VARCHAR(250) NOT NULL REFERENCES pipeline_batch(batch_id),
    topic TEXT NOT NULL,
    partition_id INTEGER NOT NULL CHECK (partition_id >= 0),
    watermark_low BIGINT NOT NULL,
    watermark_high BIGINT NOT NULL,
    offset_range INT8RANGE GENERATED ALWAYS AS (
        int8range(watermark_low, watermark_high, '(]')
    ) STORED,
    PRIMARY KEY (batch_id, topic, partition_id),
    CHECK (watermark_high > watermark_low),
    EXCLUDE USING gist (
        topic WITH =,
        partition_id WITH =,
        offset_range WITH &&
    )
);

CREATE OR REPLACE VIEW v_pipeline_batch_history AS
SELECT
    b.batch_id,
    b.state AS batch_state,
    b.manifest_uri,
    b.manifest_sha256,
    b.source_adapter,
    b.schema_contract_version,
    b.created_at,
    b.published_at,
    (
        SELECT count(*)
        FROM pipeline_batch_partition p
        WHERE p.batch_id = b.batch_id
    ) AS partition_count,
    (
        SELECT coalesce(sum(p.interval_object_count), 0)
        FROM pipeline_batch_partition p
        WHERE p.batch_id = b.batch_id
    ) AS interval_object_count,
    (
        SELECT coalesce(sum(p.interval_input_bytes), 0)
        FROM pipeline_batch_partition p
        WHERE p.batch_id = b.batch_id
    ) AS interval_input_bytes,
    (
        SELECT coalesce(max(a.attempt_number), 0)
        FROM pipeline_attempt a
        WHERE a.batch_id = b.batch_id
    ) AS latest_attempt_number
FROM pipeline_batch b;

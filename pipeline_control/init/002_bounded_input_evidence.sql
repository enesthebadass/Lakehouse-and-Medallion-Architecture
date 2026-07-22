ALTER TABLE pipeline_batch_partition
    ADD COLUMN IF NOT EXISTS interval_input_bytes BIGINT NOT NULL DEFAULT 0
    CHECK (interval_input_bytes >= 0);

ALTER TABLE pipeline_task_evidence
    ADD COLUMN IF NOT EXISTS reader_mode VARCHAR(100) NOT NULL
    DEFAULT 'history_snapshot_capped_at_high';

ALTER TABLE pipeline_task_evidence
    ADD COLUMN IF NOT EXISTS selected_input_object_count BIGINT NOT NULL DEFAULT 0
    CHECK (selected_input_object_count >= 0);

ALTER TABLE pipeline_task_evidence
    ADD COLUMN IF NOT EXISTS selected_input_bytes BIGINT NOT NULL DEFAULT 0
    CHECK (selected_input_bytes >= 0);

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

INSERT INTO pipeline_published_interval (
    batch_id, topic, partition_id, watermark_low, watermark_high
)
SELECT p.batch_id, p.topic, p.partition_id, p.watermark_low, p.watermark_high
FROM pipeline_batch_partition p
JOIN pipeline_batch b ON b.batch_id = p.batch_id
WHERE b.state = 'PUBLISHED'
  AND p.watermark_high > p.watermark_low
ON CONFLICT (batch_id, topic, partition_id) DO NOTHING;

DROP VIEW IF EXISTS v_pipeline_batch_history;

CREATE VIEW v_pipeline_batch_history AS
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

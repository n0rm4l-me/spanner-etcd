-- spanner-etcd DDL schema
-- Apply with: gcloud spanner databases ddl update DATABASE \
--   --instance=INSTANCE --project=PROJECT --ddl-file=ddl/schema.sql

CREATE SEQUENCE IF NOT EXISTS kv_seq OPTIONS (
    sequence_kind = 'bit_reversed_positive'
);

-- Main KV log. rev uses PENDING_COMMIT_TIMESTAMP() — no serialization bottleneck.
CREATE TABLE IF NOT EXISTS kv (
    id               INT64     NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE kv_seq)),
    rev              TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
    key              STRING(2048) NOT NULL,
    value            BYTES(MAX),
    old_value        BYTES(MAX),
    lease_id         INT64,
    deleted          BOOL NOT NULL DEFAULT (false),
    created          BOOL NOT NULL DEFAULT (false),
    create_revision  TIMESTAMP OPTIONS (allow_commit_timestamp = true),
    prev_revision    TIMESTAMP OPTIONS (allow_commit_timestamp = true)
) PRIMARY KEY (id);

-- Stores only the compact revision timestamp (not the current revision).
CREATE TABLE IF NOT EXISTS kv_rev (
    id  INT64     NOT NULL,
    rev TIMESTAMP NOT NULL
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS kv_lease (
    lease_id   INT64 NOT NULL,
    ttl_sec    INT64 NOT NULL,
    granted_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true)
) PRIMARY KEY (lease_id);

CREATE TABLE IF NOT EXISTS kv_cs_cursors (
    replica_id       STRING(128) NOT NULL,
    partition_token  STRING(MAX) NOT NULL,
    resume_timestamp TIMESTAMP   NOT NULL,
    updated_at       TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true)
) PRIMARY KEY (replica_id, partition_token);

CREATE INDEX IF NOT EXISTS kv_key_rev   ON kv (key, rev DESC);
CREATE INDEX IF NOT EXISTS kv_rev_idx   ON kv (rev);
CREATE INDEX IF NOT EXISTS kv_lease_idx ON kv (lease_id) STORING (key, rev);

CREATE CHANGE STREAM IF NOT EXISTS kv_changes
    FOR kv
    OPTIONS (
        retention_period = '7d',
        value_capture_type = 'NEW_ROW'
    );

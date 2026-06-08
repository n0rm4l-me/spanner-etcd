-- spanner-etcd DDL schema
-- Apply with: gcloud spanner databases ddl update DATABASE \
--   --instance=INSTANCE --project=PROJECT --ddl-file=ddl/schema.sql
--
-- NOTE: CREATE INDEX IF NOT EXISTS does NOT update an existing index definition.
-- To rebuild indexes on an existing database, DROP and recreate them manually.
-- If upgrading from an older schema, also drop the obsolete kv_rev_idx:
--   DROP INDEX kv_rev_idx;

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

-- Covering index: enables index-only reads for Get/List when the optimizer chooses it,
-- avoiding a back-join to the base table.
-- STORING value/old_value (BYTES(MAX)) doubles write amplification for large values.
-- For workloads with values >1MB, remove value/old_value from STORING.
CREATE INDEX IF NOT EXISTS kv_key_rev ON kv (key, rev DESC)
    STORING (value, old_value, lease_id, deleted, created, create_revision, prev_revision);

-- Descending revision index: lets CurrentRevision() seek O(1) instead of full scan.
CREATE INDEX IF NOT EXISTS kv_rev_desc  ON kv (rev DESC);
CREATE INDEX IF NOT EXISTS kv_lease_idx ON kv (lease_id) STORING (key, rev);

CREATE CHANGE STREAM IF NOT EXISTS kv_changes
    FOR kv
    OPTIONS (
        retention_period = '7d',
        value_capture_type = 'NEW_ROW'
    );

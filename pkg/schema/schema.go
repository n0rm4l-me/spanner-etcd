// Package schema manages the Spanner DDL for spanner-etcd.
//
// # Table design
//
//	kv            — main KV log (append-only, one row per write event)
//	kv_rev        — stores only the compact revision (not the current revision)
//	kv_lease      — active leases with TTL
//	kv_cs_cursors — Change Stream partition cursors for resume after restart
//
// # Revision strategy: PENDING_COMMIT_TIMESTAMP
//
// The `rev` column uses PENDING_COMMIT_TIMESTAMP() — Spanner's TrueTime-based
// commit timestamp. Every write sets rev = PENDING_COMMIT_TIMESTAMP() and the
// resulting timestamp is globally unique and monotonically increasing across all
// transactions.
//
// Benefits vs the previous kv_rev integer counter:
//   - No serialization point: every transaction writes independently, no lock on kv_rev
//   - Write throughput scales with Spanner processing units
//   - Revision is a nanosecond-precision timestamp (int64 UnixNano) — valid etcd int64
//
// The `rev` column is stored as TIMESTAMP internally but exposed as int64 (UnixNano)
// to etcd clients. Spanner guarantees commit timestamps are strictly monotonic.
package schema

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	CompactRevRow = int64(1) // kv_rev row that stores the last compact revision
)

// RevCounterRow kept for backward compatibility during migration.
// With PCT this row is no longer used for current revision.
const RevCounterRow = CompactRevRow

// DDL statements — all idempotent (IF NOT EXISTS / OR REPLACE).
var statements = []string{
	`CREATE SEQUENCE IF NOT EXISTS kv_seq OPTIONS (
		sequence_kind = 'bit_reversed_positive'
	)`,

	// rev, create_revision, prev_revision are commit timestamps.
	// All TIMESTAMP columns that receive PENDING_COMMIT_TIMESTAMP() must have
	// OPTIONS (allow_commit_timestamp = true).
	`CREATE TABLE IF NOT EXISTS kv (
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
	) PRIMARY KEY (id)`,

	// kv_rev stores only the compact revision timestamp.
	// Current revision = MAX(rev) FROM kv (no lock needed).
	`CREATE TABLE IF NOT EXISTS kv_rev (
		id  INT64     NOT NULL,
		rev TIMESTAMP NOT NULL
	) PRIMARY KEY (id)`,

	`CREATE TABLE IF NOT EXISTS kv_lease (
		lease_id   INT64 NOT NULL,
		ttl_sec    INT64 NOT NULL,
		granted_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true)
	) PRIMARY KEY (lease_id)`,

	// STORING includes value/old_value (BYTES(MAX)) to eliminate back-joins on reads.
	// Trade-off: doubles write amplification for large values. For workloads with
	// values >1MB consider removing value/old_value from STORING and accepting the
	// back-join cost on those columns.
	`CREATE INDEX IF NOT EXISTS kv_key_rev ON kv (key, rev DESC)
	   STORING (value, old_value, lease_id, deleted, created, create_revision, prev_revision)`,

	`CREATE INDEX IF NOT EXISTS kv_rev_desc  ON kv (rev DESC)`,
	`CREATE INDEX IF NOT EXISTS kv_lease_idx ON kv (lease_id) STORING (key, rev)`,

	`CREATE CHANGE STREAM IF NOT EXISTS kv_changes
		FOR kv
		OPTIONS (
			retention_period = '7d',
			value_capture_type = 'NEW_ROW'
		)`,

	`CREATE TABLE IF NOT EXISTS kv_cs_cursors (
		replica_id       STRING(128) NOT NULL,
		partition_token  STRING(MAX) NOT NULL,
		resume_timestamp TIMESTAMP   NOT NULL,
		updated_at       TIMESTAMP   NOT NULL OPTIONS (allow_commit_timestamp = true)
	) PRIMARY KEY (replica_id, partition_token)`,
}

// Ensure creates or updates the schema.
func Ensure(ctx context.Context, adminClient *database.DatabaseAdminClient, dbPath string, log *zap.Logger) error {
	log.Info("ensuring schema", zap.String("database", dbPath))

	op, err := adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   dbPath,
		Statements: statements,
	})
	if err != nil {
		if isPermissionDenied(err) {
			log.Warn("no DDL permission — assuming schema is managed externally",
				zap.String("database", dbPath))
			return nil
		}
		return fmt.Errorf("update DDL: %w", err)
	}
	if err := op.Wait(ctx); err != nil {
		if isPermissionDenied(err) {
			log.Warn("no DDL permission — assuming schema is managed externally",
				zap.String("database", dbPath))
			return nil
		}
		return fmt.Errorf("wait DDL: %w", err)
	}

	log.Info("schema up to date")
	return nil
}

func isPermissionDenied(err error) bool {
	if s, ok := status.FromError(err); ok {
		return s.Code() == codes.PermissionDenied
	}
	return false
}

// SeedCompactRev inserts the compact revision row with the epoch timestamp
// if it doesn't already exist. With PCT, the current revision is derived from
// MAX(kv.rev) and needs no explicit seeding — the first write sets it.
func SeedCompactRev(ctx context.Context, client *spanner.Client) error {
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.ReadRow(ctx, "kv_rev", spanner.Key{CompactRevRow}, []string{"rev"})
		if err == nil {
			return nil
		}
		if spanner.ErrCode(err) != codes.NotFound {
			return err
		}
		// Seed with Unix epoch so compact_rev < any real write.
		epoch := time.Unix(0, 1) // 1 nanosecond past epoch
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.Insert("kv_rev", []string{"id", "rev"}, []interface{}{CompactRevRow, epoch}),
		})
	})
	return err
}

// SeedRevCounter is kept for backward compatibility.
// With PCT it delegates to SeedCompactRev.
func SeedRevCounter(ctx context.Context, client *spanner.Client) error {
	return SeedCompactRev(ctx, client)
}

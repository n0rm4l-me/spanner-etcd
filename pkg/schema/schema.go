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
	"strings"
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
	// CompactLockRow is the kv_rev row used as a lease on the background
	// compactor: its rev is the time (Spanner clock) until which one replica holds
	// the right to compact. It is created on first use.
	CompactLockRow = int64(2)
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

	// expires_at is the lease deadline in Spanner time, extended by every
	// keepalive. It is nullable: rows written before it existed fall back to
	// granted_at + ttl_sec.
	`CREATE TABLE IF NOT EXISTS kv_lease (
		lease_id   INT64 NOT NULL,
		ttl_sec    INT64 NOT NULL,
		granted_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
		expires_at TIMESTAMP
	) PRIMARY KEY (lease_id)`,

	// CREATE TABLE IF NOT EXISTS does not alter an existing table, so databases
	// created by an older version get the column here. No-op on new databases.
	`ALTER TABLE kv_lease ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP`,

	// STORING includes value/old_value (BYTES(MAX)) so Spanner can serve Get/List
	// reads from the index without a back-join when the optimizer chooses this index.
	// Trade-off: doubles write amplification for large values. For workloads with
	// values >1MB consider removing value/old_value from STORING.
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

// Pauses between attempts to apply the schema while another replica is changing
// it, and how long to keep trying. The first schema change of a new database
// (tables, indexes, the Change Stream) can take a minute or two on a real
// instance, and the replicas that lost the race wait for it.
const (
	ensureRetryMin    = 500 * time.Millisecond
	ensureRetryMax    = 5 * time.Second
	ensureRetryWindow = 5 * time.Minute
)

// Ensure creates or updates the schema.
//
// Replicas of a new deployment start together and all call this. Spanner
// rejects a schema change while another one is in progress, so the replicas
// that lose the race retry: the statements are idempotent, and once the winner
// has finished the retry changes nothing.
//
// Note: CREATE INDEX IF NOT EXISTS does not update an existing index definition.
// Upgrading an existing database requires manually dropping and recreating
// kv_key_rev (to add STORING) and dropping the obsolete kv_rev_idx if present.
// See ddl/schema.sql for details.
func Ensure(ctx context.Context, adminClient *database.DatabaseAdminClient, dbPath string, log *zap.Logger) error {
	log.Info("ensuring schema", zap.String("database", dbPath))

	deadline := time.Now().Add(ensureRetryWindow)
	delay := ensureRetryMin
	for {
		err := ensureOnce(ctx, adminClient, dbPath, log)
		if err == nil {
			return nil
		}
		if !isConcurrentSchemaChange(err) || time.Now().Add(delay).After(deadline) {
			return err
		}
		log.Warn("another schema change is in progress, retrying",
			zap.Error(err), zap.Duration("retry_in", delay))
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return fmt.Errorf("%w (while waiting to retry: %v)", err, ctx.Err())
		}
		if delay *= 2; delay > ensureRetryMax {
			delay = ensureRetryMax
		}
	}
}

// ensureOnce applies the schema statements once and waits for the result.
func ensureOnce(ctx context.Context, adminClient *database.DatabaseAdminClient, dbPath string, log *zap.Logger) error {
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

// isConcurrentSchemaChange reports whether err means that another schema change
// or transaction is in progress, which goes away by itself: Aborted, or
// FailedPrecondition with "concurrent" in the message. Other
// FailedPrecondition errors are real and are not retried.
func isConcurrentSchemaChange(err error) bool {
	s, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch s.Code() {
	case codes.Aborted:
		return true
	case codes.FailedPrecondition:
		return strings.Contains(strings.ToLower(s.Message()), "concurrent")
	}
	return false
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

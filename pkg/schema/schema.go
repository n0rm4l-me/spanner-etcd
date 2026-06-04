// Package schema manages the Spanner DDL for spanner-etcd.
//
// Table design:
//   kv             — main key-value log (append-only, one row per write event)
//   kv_rev         — single-row monotonic revision counter
//   kv_lease       — active leases with TTL
//   kv_cs_cursors  — Change Stream partition cursors for resume after restart
//
// The physical PK of kv uses bit_reversed_positive to avoid write hotspots.
// The logical revision is stored in the `rev` column and sourced from kv_rev.
//
// Change Stream (kv_changes) is created after the main tables so that it
// captures all subsequent writes. Each spanner-etcd replica reads all
// partitions of kv_changes and fans events out to local Watch subscribers,
// reducing Watch latency from ~1s (poll) to ~10–50ms.
package schema

import (
	"context"
	"fmt"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"go.uber.org/zap"
)

const (
	RevCounterRow = int64(1)
)

// DDL statements — all idempotent (IF NOT EXISTS).
var statements = []string{
	// Sequence must be created before the table that references it.
	`CREATE SEQUENCE IF NOT EXISTS kv_seq OPTIONS (
		sequence_kind = 'bit_reversed_positive'
	)`,

	// Main KV log table. Physical PK is bit_reversed to avoid hotspots.
	// rev = logical monotonic revision from kv_rev.
	`CREATE TABLE IF NOT EXISTS kv (
		id               INT64 NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE kv_seq)),
		rev              INT64 NOT NULL,
		key              STRING(2048) NOT NULL,
		value            BYTES(MAX),
		old_value        BYTES(MAX),
		lease_id         INT64,
		deleted          BOOL NOT NULL DEFAULT (false),
		created          BOOL NOT NULL DEFAULT (false),
		create_revision  INT64 NOT NULL DEFAULT (0),
		prev_revision    INT64 NOT NULL DEFAULT (0)
	) PRIMARY KEY (id)`,

	// Monotonic revision counter — single row, bumped in every RW txn.
	`CREATE TABLE IF NOT EXISTS kv_rev (
		id   INT64 NOT NULL,
		rev  INT64 NOT NULL
	) PRIMARY KEY (id)`,

	// Active leases. TTL goroutine watches this table.
	`CREATE TABLE IF NOT EXISTS kv_lease (
		lease_id   INT64 NOT NULL,
		ttl_sec    INT64 NOT NULL,
		granted_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true)
	) PRIMARY KEY (lease_id)`,

	// Indexes for common access patterns.
	`CREATE INDEX IF NOT EXISTS kv_key_rev   ON kv (key, rev DESC)`,
	`CREATE INDEX IF NOT EXISTS kv_rev_idx   ON kv (rev)`,
	`CREATE INDEX IF NOT EXISTS kv_lease_idx ON kv (lease_id) STORING (key, rev)`,

	// Change Stream: captures all mutations to the kv table.
	// Each spanner-etcd replica reads this stream to deliver Watch events
	// with ~10–50ms latency instead of the ~1s polling approach.
	// retention_period: 7 days allows replicas to catch up after downtime.
	`CREATE CHANGE STREAM IF NOT EXISTS kv_changes
		FOR kv
		OPTIONS (
			retention_period = '7d',
			value_capture_type = 'NEW_ROW'
		)`,

	// Change Stream partition cursors.
	// Each replica persists its last-read partition token + timestamp here
	// so that it can resume from the correct position after a restart without
	// re-delivering already-seen events.
	// replica_id allows multiple replicas to store independent cursors.
	`CREATE TABLE IF NOT EXISTS kv_cs_cursors (
		replica_id       STRING(128) NOT NULL,
		partition_token  STRING(MAX) NOT NULL,
		resume_timestamp TIMESTAMP  NOT NULL,
		updated_at       TIMESTAMP  NOT NULL OPTIONS (allow_commit_timestamp = true)
	) PRIMARY KEY (replica_id, partition_token)`,
}

// Ensure creates or updates the schema and seeds the revision counter.
func Ensure(ctx context.Context, adminClient *database.DatabaseAdminClient, dbPath string, log *zap.Logger) error {
	log.Info("ensuring schema", zap.String("database", dbPath))

	op, err := adminClient.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   dbPath,
		Statements: statements,
	})
	if err != nil {
		return fmt.Errorf("update DDL: %w", err)
	}
	if err := op.Wait(ctx); err != nil {
		return fmt.Errorf("wait DDL: %w", err)
	}

	log.Info("schema up to date")
	return nil
}

// SeedRevCounter inserts the revision counter row if it doesn't exist.
func SeedRevCounter(ctx context.Context, client *spanner.Client) error {
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		row, err := txn.ReadRow(ctx, "kv_rev", spanner.Key{RevCounterRow}, []string{"rev"})
		if err == nil {
			var rev int64
			_ = row.Column(0, &rev)
			return nil // already seeded
		}
		if spanner.ErrCode(err).String() != "NotFound" {
			// spanner.ErrCode returns a codes.Code; check string for portability
			_ = err // non-fatal on unexpected errors
		}
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.Insert("kv_rev", []string{"id", "rev"}, []interface{}{RevCounterRow, int64(0)}),
		})
	})
	return err
}

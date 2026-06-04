// Package store implements the core Spanner-backed key-value store.
// All operations are linearizable: reads use strong reads, writes use
// read-write transactions that atomically bump the global revision counter.
package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"

	"github.com/paas/spanner-etcd/pkg/schema"
)

// ErrKeyNotFound is returned by Get when the key does not exist.
var ErrKeyNotFound = errors.New("key not found")

// ErrKeyExists is returned by Create when the key already exists.
var ErrKeyExists = errors.New("key already exists")

// ErrRevisionMismatch is returned by Update/Delete when the CAS check fails.
var ErrRevisionMismatch = errors.New("revision mismatch")

// ErrCompacted is returned when a requested revision has been compacted away.
var ErrCompacted = errors.New("requested revision has been compacted")

// KV represents one key-value entry as seen by callers.
type KV struct {
	Key            string
	Value          []byte
	OldValue       []byte
	LeaseID        int64
	Deleted        bool
	Created        bool
	Rev            int64 // ModRevision
	CreateRevision int64
	PrevRevision   int64
}

// Event is a single mutation event emitted by the Watch stream.
type Event struct {
	KV   *KV
	Type EventType
}

// EventType mirrors the etcd event types.
type EventType int

const (
	EventPut    EventType = 0
	EventDelete EventType = 1
)

// Store is the central Spanner-backed store. It is safe for concurrent use.
type Store struct {
	client    *spanner.Client
	log       *zap.Logger
	bgCtx     context.Context // server-lifetime context for background operations
	watcher   *Watcher
	leasesMgr *LeaseManager
}

// New creates a Store. The caller is responsible for calling Close.
func New(ctx context.Context, client *spanner.Client, log *zap.Logger) (*Store, error) {
	s := &Store{
		client: client,
		log:    log,
		bgCtx:  ctx,
	}
	s.watcher = newWatcher(ctx, s, log)
	s.leasesMgr = newLeaseManager(ctx, s, log)
	return s, nil
}

// Close shuts down background goroutines.
func (s *Store) Close() {
	s.watcher.close()
	s.leasesMgr.close()
}

// Leases returns the lease manager for use by the gRPC lease server.
func (s *Store) Leases() *LeaseManager {
	return s.leasesMgr
}

// CurrentRevision returns the latest global revision.
func (s *Store) CurrentRevision(ctx context.Context) (int64, error) {
	row, err := s.client.Single().ReadRow(ctx, "kv_rev",
		spanner.Key{schema.RevCounterRow}, []string{"rev"})
	if err != nil {
		return 0, fmt.Errorf("read kv_rev: %w", err)
	}
	var rev int64
	if err := row.Column(0, &rev); err != nil {
		return 0, err
	}
	return rev, nil
}

// Get returns the current value of key, or the value at a specific revision.
// revision=0 means current.
func (s *Store) Get(ctx context.Context, key string, revision int64) (currentRev int64, kv *KV, err error) {
	currentRev, err = s.CurrentRevision(ctx)
	if err != nil {
		return 0, nil, err
	}

	stmt := spanner.Statement{
		SQL: `SELECT rev, key, value, old_value, lease_id, deleted, created, create_revision, prev_revision
		      FROM kv
		      WHERE key = @key
		        AND rev = (
		          SELECT MAX(rev) FROM kv
		          WHERE key = @key AND rev <= @rev_cap
		        )`,
		Params: map[string]interface{}{
			"key":     key,
			"rev_cap": revCap(revision, currentRev),
		},
	}

	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	row, err := iter.Next()
	if errors.Is(err, iterator.Done) {
		return currentRev, nil, nil
	}
	if err != nil {
		return 0, nil, fmt.Errorf("get %s: %w", key, err)
	}

	kv, err = scanKV(row)
	if err != nil {
		return 0, nil, err
	}
	if kv.Deleted {
		return currentRev, nil, nil
	}
	return currentRev, kv, nil
}

// List returns all keys matching the prefix, as of a given revision.
// revision=0 means current. Returns (currentRev, compactRev, kvs, err).
func (s *Store) List(ctx context.Context, prefix, startKey string, limit, revision int64) (int64, int64, []*KV, error) {
	currentRev, err := s.CurrentRevision(ctx)
	if err != nil {
		return 0, 0, nil, err
	}
	cap := revCap(revision, currentRev)

	stmt := spanner.Statement{
		SQL: `SELECT kv.rev, kv.key, kv.value, kv.old_value, kv.lease_id,
		             kv.deleted, kv.created, kv.create_revision, kv.prev_revision
		      FROM kv
		      INNER JOIN (
		        SELECT key, MAX(rev) AS max_rev
		        FROM kv
		        WHERE key LIKE @prefix AND key >= @start_key
		          AND rev <= @cap
		        GROUP BY key
		      ) AS latest ON kv.key = latest.key AND kv.rev = latest.max_rev
		      WHERE kv.deleted = false
		      ORDER BY kv.key ASC` + limitClause(limit),
		Params: map[string]interface{}{
			"prefix":    likePrefix(prefix),
			"start_key": startKey,
			"cap":       cap,
		},
	}

	compactRev, err := s.compactRevision(ctx)
	if err != nil {
		return 0, 0, nil, err
	}

	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var kvs []*KV
	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return 0, 0, nil, fmt.Errorf("list %s: %w", prefix, err)
		}
		kv, err := scanKV(row)
		if err != nil {
			return 0, 0, nil, err
		}
		kvs = append(kvs, kv)
	}
	return currentRev, compactRev, kvs, nil
}

// Count returns (currentRev, count) for keys matching prefix.
func (s *Store) Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error) {
	currentRev, err := s.CurrentRevision(ctx)
	if err != nil {
		return 0, 0, err
	}
	cap := revCap(revision, currentRev)

	stmt := spanner.Statement{
		SQL: `SELECT COUNT(*) FROM (
		        SELECT key FROM kv
		        INNER JOIN (
		          SELECT key AS k2, MAX(rev) AS max_rev
		          FROM kv WHERE key LIKE @prefix AND key >= @start_key AND rev <= @cap
		          GROUP BY key
		        ) AS latest ON kv.key = latest.k2 AND kv.rev = latest.max_rev
		        WHERE kv.deleted = false
		      )`,
		Params: map[string]interface{}{
			"prefix":    likePrefix(prefix),
			"start_key": startKey,
			"cap":       cap,
		},
	}

	row, err := s.client.Single().Query(ctx, stmt).Next()
	if err != nil {
		return 0, 0, fmt.Errorf("count %s: %w", prefix, err)
	}
	var count int64
	if err := row.Column(0, &count); err != nil {
		return 0, 0, err
	}
	return currentRev, count, nil
}

// Create inserts key only if it does not currently exist.
// Returns (newRevision, error). Returns ErrKeyExists if key already present.
func (s *Store) Create(ctx context.Context, key string, value []byte, leaseID int64) (int64, error) {
	var newRev int64
	_, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Check key does not exist (not deleted).
		exists, err := s.keyExistsTxn(ctx, txn, key)
		if err != nil {
			return err
		}
		if exists {
			return ErrKeyExists
		}

		newRev, err = s.bumpRevTxn(ctx, txn)
		if err != nil {
			return err
		}

		// Omit "id" — the DEFAULT expression (GET_NEXT_SEQUENCE_VALUE) fills it.
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.Insert("kv",
				[]string{"rev", "key", "value", "old_value", "lease_id", "deleted", "created", "create_revision", "prev_revision"},
				[]interface{}{newRev, key, value, []byte(nil), leaseID, false, true, newRev, int64(0)},
			),
		})
	})
	if err != nil {
		return 0, err
	}
	s.watcher.notify(newRev)
	return newRev, nil
}

// Update replaces key at the given revision (CAS). Returns ErrRevisionMismatch if revision doesn't match.
func (s *Store) Update(ctx context.Context, key string, value []byte, revision, leaseID int64) (int64, *KV, bool, error) {
	var newRev int64
	var prev *KV

	_, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		var err error
		prev, err = s.getLatestTxn(ctx, txn, key)
		if err != nil {
			return err
		}
		if prev == nil {
			return ErrKeyNotFound
		}
		if prev.Rev != revision {
			return ErrRevisionMismatch
		}

		newRev, err = s.bumpRevTxn(ctx, txn)
		if err != nil {
			return err
		}

		return txn.BufferWrite([]*spanner.Mutation{
			spanner.Insert("kv",
				[]string{"rev", "key", "value", "old_value", "lease_id", "deleted", "created", "create_revision", "prev_revision"},
				[]interface{}{newRev, key, value, prev.Value, leaseID, false, false, prev.CreateRevision, prev.Rev},
			),
		})
	})
	if errors.Is(err, ErrRevisionMismatch) || errors.Is(err, ErrKeyNotFound) {
		curRev, _ := s.CurrentRevision(ctx)
		return curRev, prev, false, nil
	}
	if err != nil {
		return 0, nil, false, err
	}
	s.watcher.notify(newRev)
	return newRev, prev, true, nil
}

// Delete removes key at the given revision (CAS). revision=0 means unconditional.
func (s *Store) Delete(ctx context.Context, key string, revision int64) (int64, *KV, bool, error) {
	var newRev int64
	var prev *KV

	_, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		var err error
		prev, err = s.getLatestTxn(ctx, txn, key)
		if err != nil {
			return err
		}
		if prev == nil {
			return ErrKeyNotFound
		}
		if revision != 0 && prev.Rev != revision {
			return ErrRevisionMismatch
		}

		newRev, err = s.bumpRevTxn(ctx, txn)
		if err != nil {
			return err
		}

		return txn.BufferWrite([]*spanner.Mutation{
			spanner.Insert("kv",
				[]string{"rev", "key", "value", "old_value", "lease_id", "deleted", "created", "create_revision", "prev_revision"},
				[]interface{}{newRev, key, []byte(nil), prev.Value, int64(0), true, false, prev.CreateRevision, prev.Rev},
			),
		})
	})
	if errors.Is(err, ErrRevisionMismatch) || errors.Is(err, ErrKeyNotFound) {
		curRev, _ := s.CurrentRevision(ctx)
		return curRev, prev, false, nil
	}
	if err != nil {
		return 0, nil, false, err
	}
	s.watcher.notify(newRev)
	return newRev, prev, true, nil
}

// After returns all events with rev > afterRev matching prefix, up to limit.
// This is the core of the Watch poll loop.
func (s *Store) After(ctx context.Context, prefix string, afterRev, limit int64) (int64, []*Event, error) {
	currentRev, err := s.CurrentRevision(ctx)
	if err != nil {
		return 0, nil, err
	}

	stmt := spanner.Statement{
		SQL: `SELECT rev, key, value, old_value, lease_id, deleted, created, create_revision, prev_revision
		      FROM kv
		      WHERE key LIKE @prefix
		        AND rev > @after_rev
		      ORDER BY rev ASC` + limitClause(limit),
		Params: map[string]interface{}{
			"prefix":    likePrefix(prefix),
			"after_rev": afterRev,
		},
	}

	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var events []*Event
	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return 0, nil, fmt.Errorf("after %s: %w", prefix, err)
		}
		kv, err := scanKV(row)
		if err != nil {
			return 0, nil, err
		}
		evType := EventPut
		if kv.Deleted {
			evType = EventDelete
		}
		events = append(events, &Event{KV: kv, Type: evType})
	}
	return currentRev, events, nil
}

// Compact marks all old revisions up to targetRev as eligible for GC.
// In this implementation, compact deletes superseded and deleted rows.
func (s *Store) Compact(ctx context.Context, targetRev int64) (int64, error) {
	_, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// Record compact revision in a known key so Watch can detect ErrCompacted.
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.InsertOrUpdateMap("kv_rev", map[string]interface{}{
				"id":  schema.RevCounterRow + 1, // row 2 = compact rev
				"rev": targetRev,
			}),
		})
	})
	if err != nil {
		return 0, err
	}

	// Async deletion of old rows.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		s.compactRows(ctx, targetRev)
	}()

	return targetRev, nil
}

// CompactRevision returns the last compacted revision (0 if never compacted).
func (s *Store) CompactRevision(ctx context.Context) (int64, error) {
	return s.compactRevision(ctx)
}

// Watch returns a channel of events for keys matching prefix, starting at afterRev.
func (s *Store) Watch(ctx context.Context, prefix string, afterRev int64) <-chan []*Event {
	return s.watcher.subscribe(ctx, prefix, afterRev)
}

// ─── helpers ────────────────────────────────────────────────────────────────

// bumpRevTxn atomically increments kv_rev.rev and returns the new value.
//
// Performance note: we use a single DML UPDATE...THEN RETURN statement which
// both increments and reads the counter server-side, avoiding the two-RPC
// read-modify-write pattern. The Spanner Go client executes this via Query()
// (not Update()) because THEN RETURN produces a result set.
func (s *Store) bumpRevTxn(ctx context.Context, txn *spanner.ReadWriteTransaction) (int64, error) {
	iter := txn.Query(ctx, spanner.Statement{
		SQL: `UPDATE kv_rev SET rev = rev + 1 WHERE id = @id THEN RETURN rev`,
		Params: map[string]interface{}{"id": schema.RevCounterRow},
	})
	defer iter.Stop()

	row, err := iter.Next()
	if errors.Is(err, iterator.Done) {
		return 0, fmt.Errorf("kv_rev row not found")
	}
	if err != nil {
		return 0, fmt.Errorf("bump kv_rev: %w", err)
	}
	var newRev int64
	if err := row.Columns(&newRev); err != nil {
		return 0, err
	}
	return newRev, nil
}

// keyExistsTxn checks whether key has a non-deleted current row.
func (s *Store) keyExistsTxn(ctx context.Context, txn *spanner.ReadWriteTransaction, key string) (bool, error) {
	stmt := spanner.Statement{
		SQL:    `SELECT deleted FROM kv WHERE key = @key ORDER BY rev DESC LIMIT 1`,
		Params: map[string]interface{}{"key": key},
	}
	iter := txn.Query(ctx, stmt)
	defer iter.Stop()
	row, err := iter.Next()
	if errors.Is(err, iterator.Done) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var deleted bool
	if err := row.Column(0, &deleted); err != nil {
		return false, err
	}
	return !deleted, nil
}

// getLatestTxn returns the most recent KV row for key within a txn.
// Uses the kv_key_rev index (key, rev DESC) for an efficient latest-row lookup.
func (s *Store) getLatestTxn(ctx context.Context, txn *spanner.ReadWriteTransaction, key string) (*KV, error) {
	stmt := spanner.Statement{
		SQL: `SELECT rev, key, value, old_value, lease_id, deleted, created, create_revision, prev_revision
		      FROM kv@{FORCE_INDEX=kv_key_rev} WHERE key = @key ORDER BY rev DESC LIMIT 1`,
		Params: map[string]interface{}{"key": key},
	}
	iter := txn.Query(ctx, stmt)
	defer iter.Stop()
	row, err := iter.Next()
	if errors.Is(err, iterator.Done) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	kv, err := scanKV(row)
	if err != nil {
		return nil, err
	}
	if kv.Deleted {
		return nil, nil
	}
	return kv, nil
}

// compactRevision reads kv_rev row 2 (compact revision). Returns 0 if absent.
func (s *Store) compactRevision(ctx context.Context) (int64, error) {
	row, err := s.client.Single().ReadRow(ctx, "kv_rev",
		spanner.Key{schema.RevCounterRow + 1}, []string{"rev"})
	if err != nil {
		if spanner.ErrCode(err) == codes.NotFound {
			return 0, nil
		}
		return 0, err
	}
	var rev int64
	_ = row.Column(0, &rev)
	return rev, nil
}

// compactRows physically deletes rows that are superseded or deleted and older than targetRev.
func (s *Store) compactRows(ctx context.Context, targetRev int64) {
	stmt := spanner.Statement{
		SQL: `DELETE FROM kv WHERE id IN (
		        SELECT id FROM kv
		        WHERE rev <= @target
		          AND (deleted = true OR prev_revision != 0)
		      )`,
		Params: map[string]interface{}{"target": targetRev},
	}
	_, err := s.client.PartitionedUpdate(ctx, stmt)
	if err != nil {
		s.log.Warn("compact rows failed", zap.Error(err))
	}
}

// scanKV reads one row from a Spanner iterator into a KV struct.
func scanKV(row *spanner.Row) (*KV, error) {
	var kv KV
	var value, oldValue []byte
	if err := row.Columns(
		&kv.Rev, &kv.Key, &value, &oldValue,
		&kv.LeaseID, &kv.Deleted, &kv.Created,
		&kv.CreateRevision, &kv.PrevRevision,
	); err != nil {
		return nil, fmt.Errorf("scan kv row: %w", err)
	}
	kv.Value = value
	kv.OldValue = oldValue
	return &kv, nil
}

// likePrefix converts an exact prefix to a SQL LIKE pattern.
func likePrefix(prefix string) string {
	return prefix + "%"
}

// limitClause returns a LIMIT clause or empty string.
func limitClause(limit int64) string {
	if limit <= 0 {
		return ""
	}
	return fmt.Sprintf(" LIMIT %d", limit)
}

// revCap returns the effective revision cap: revision if set, else currentRev.
func revCap(revision, currentRev int64) int64 {
	if revision > 0 {
		return revision
	}
	return currentRev
}

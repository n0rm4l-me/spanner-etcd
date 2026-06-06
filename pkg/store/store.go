// Package store implements the core Spanner-backed key-value store.
//
// # Revision strategy: PENDING_COMMIT_TIMESTAMP
//
// Every write uses Spanner's PENDING_COMMIT_TIMESTAMP() as the revision.
// This eliminates the kv_rev serialization bottleneck: each write transaction
// is fully independent — no lock on a shared counter row.
//
// Revisions are stored as TIMESTAMP in Spanner and exposed as int64 (UnixNano)
// to etcd clients. Spanner guarantees TrueTime-based commit timestamps are
// globally unique and monotonically increasing across all transactions.
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

	"github.com/n0rm4l-me/spanner-etcd/pkg/metrics"
	"github.com/n0rm4l-me/spanner-etcd/pkg/schema"
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
	Rev            int64 // ModRevision (UnixNano of commit timestamp)
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

const (
	// compactBatchSize is the number of rows deleted per Spanner transaction.
	// Spanner has a 20k mutation limit per transaction; 1000 rows = safe headroom.
	compactBatchSize = 1000

	DefaultAutoCompactInterval = 5 * time.Minute
	DefaultAutoCompactAge      = 5 * time.Minute
)

// StoreConfig holds optional tuning parameters for the Store.
type StoreConfig struct {
	// AutoCompactInterval controls how often the background compaction loop runs.
	// 0 (unset) uses DefaultAutoCompactInterval.
	// -1 disables auto-compaction entirely (rely on explicit Compact calls).
	AutoCompactInterval time.Duration
	// AutoCompactAge controls how far behind current revision to compact.
	// Keeps this much history for Watch replay.
	// 0 (unset) uses DefaultAutoCompactAge.
	AutoCompactAge time.Duration
}

// Store is the central Spanner-backed store. It is safe for concurrent use.
type Store struct {
	client    *spanner.Client
	log       *zap.Logger
	bgCtx     context.Context
	bgCancel  context.CancelFunc
	cfg       StoreConfig
	watcher   *Watcher
	leasesMgr *LeaseManager
}

// New creates a Store with default configuration. The caller is responsible for calling Close.
func New(ctx context.Context, client *spanner.Client, log *zap.Logger) (*Store, error) {
	return NewWithConfig(ctx, client, log, StoreConfig{})
}

// NewWithConfig creates a Store with explicit tuning parameters.
// Pass AutoCompactInterval = -1 to disable background auto-compaction.
func NewWithConfig(ctx context.Context, client *spanner.Client, log *zap.Logger, cfg StoreConfig) (*Store, error) {
	// Negative age would compact future revisions — reject it.
	if cfg.AutoCompactAge < 0 {
		return nil, fmt.Errorf("AutoCompactAge must be >= 0, got %v", cfg.AutoCompactAge)
	}
	// Any negative interval disables auto-compaction (normalized to -1).
	// This avoids surprising startup failures for callers that pass
	// unit-suffixed negative values such as -1*time.Second.
	// Note: the CLI maps "0/off/disable" to -1 before calling NewWithConfig;
	// programmatic callers must pass -1 explicitly to disable.
	if cfg.AutoCompactInterval < 0 {
		cfg.AutoCompactInterval = -1
	}

	// -1 = disabled; 0 = unset, use default.
	if cfg.AutoCompactInterval == 0 {
		cfg.AutoCompactInterval = DefaultAutoCompactInterval
	}
	if cfg.AutoCompactAge == 0 {
		cfg.AutoCompactAge = DefaultAutoCompactAge
	}

	// Derive bgCtx from the caller's ctx so that cancelling the server lifetime
	// context also stops background goroutines even without an explicit Close().
	// bgCancel is called from Close() for the normal shutdown path.
	bgCtx, bgCancel := context.WithCancel(ctx)
	s := &Store{
		client:   client,
		log:      log,
		bgCtx:    bgCtx,
		bgCancel: bgCancel,
		cfg:      cfg,
	}
	s.watcher = newWatcher(bgCtx, s, log)
	s.leasesMgr = newLeaseManager(bgCtx, s, log)
	if cfg.AutoCompactInterval > 0 {
		go s.autoCompactLoop(bgCtx)
	}
	return s, nil
}

// Close shuts down background goroutines.
func (s *Store) Close() {
	s.bgCancel()
	s.watcher.close()
	s.leasesMgr.close()
}

// Leases returns the lease manager.
func (s *Store) Leases() *LeaseManager {
	return s.leasesMgr
}

// CurrentRevision returns the latest global revision as int64 (UnixNano).
// With PCT, current revision = MAX(rev) FROM kv — no lock, no contention.
func (s *Store) CurrentRevision(ctx context.Context) (int64, error) {
	iter := s.client.Single().Query(ctx, spanner.Statement{SQL: `SELECT MAX(rev) FROM kv`})
	defer iter.Stop()
	row, err := iter.Next()
	if errors.Is(err, iterator.Done) {
		return 1, nil
	}
	if err != nil {
		return 0, fmt.Errorf("current revision: %w", err)
	}
	var ts spanner.NullTime
	if err := row.Column(0, &ts); err != nil {
		return 0, err
	}
	if !ts.Valid {
		return 1, nil // empty table
	}
	rev := tsToRev(ts.Time)
	if rev <= 1 {
		return 1, nil
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
	if revision > 0 {
		compactRev, cerr := s.compactRevision(ctx)
		if cerr != nil {
			return 0, nil, fmt.Errorf("get compact revision: %w", cerr)
		}
		if compactRev > 1 && revision <= compactRev {
			return currentRev, nil, ErrCompacted
		}
	}

	capTS := revToTS(revCap(revision, currentRev))
	stmt := spanner.Statement{
		SQL: `SELECT rev, key, value, old_value, lease_id, deleted, created, create_revision, prev_revision
		      FROM kv
		      WHERE key = @key
		        AND rev = (
		          SELECT MAX(rev) FROM kv
		          WHERE key = @key AND rev <= @cap
		        )`,
		Params: map[string]interface{}{
			"key": key,
			"cap": capTS,
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
func (s *Store) List(ctx context.Context, prefix, startKey string, limit, revision int64) (int64, int64, []*KV, error) {
	currentRev, err := s.CurrentRevision(ctx)
	if err != nil {
		return 0, 0, nil, err
	}
	capTS := revToTS(revCap(revision, currentRev))

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
			"cap":       capTS,
		},
	}

	compactRev, err := s.compactRevision(ctx)
	if err != nil {
		return 0, 0, nil, err
	}
	if compactRev > 1 && revision > 0 && revision <= compactRev {
		return currentRev, compactRev, nil, ErrCompacted
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
	if revision > 0 {
		compactRev, cerr := s.compactRevision(ctx)
		if cerr != nil {
			return 0, 0, fmt.Errorf("get compact revision: %w", cerr)
		}
		if compactRev > 1 && revision <= compactRev {
			return currentRev, 0, ErrCompacted
		}
	}
	capTS := revToTS(revCap(revision, currentRev))

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
			"cap":       capTS,
		},
	}

	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()
	row, err := iter.Next()
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
// Uses PENDING_COMMIT_TIMESTAMP() — no kv_rev lock.
func (s *Store) Create(ctx context.Context, key string, value []byte, leaseID int64) (int64, error) {
	var commitTS time.Time

	resp, err := s.client.ReadWriteTransactionWithOptions(ctx,
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			exists, err := s.keyExistsTxn(ctx, txn, key)
			if err != nil {
				return err
			}
			if exists {
				return ErrKeyExists
			}
			return txn.BufferWrite([]*spanner.Mutation{
				spanner.Insert("kv",
					[]string{"rev", "key", "value", "old_value", "lease_id",
						"deleted", "created", "create_revision", "prev_revision"},
					[]interface{}{
						spanner.CommitTimestamp, // rev = PENDING_COMMIT_TIMESTAMP()
						key, value, []byte(nil), leaseID,
						false, true,
						spanner.CommitTimestamp, // create_revision = same commit
						(*time.Time)(nil),       // prev_revision = NULL
					},
				),
			})
		}, spanner.TransactionOptions{CommitOptions: spanner.CommitOptions{ReturnCommitStats: false}})

	status := "ok"
	if err != nil {
		status = "error"
	}
	metrics.KVOperationsTotal.WithLabelValues("create", status).Inc()
	metrics.SpannerTransactions.WithLabelValues(status).Inc()

	if err != nil {
		return 0, err
	}
	commitTS = resp.CommitTs
	rev := tsToRev(commitTS)
	metrics.CurrentRevision.Set(float64(rev))
	s.watcher.notify(rev)
	return rev, nil
}

// Update replaces key at the given revision (CAS).
func (s *Store) Update(ctx context.Context, key string, value []byte, revision, leaseID int64) (int64, *KV, bool, error) {
	var commitTS time.Time
	var prev *KV

	resp, err := s.client.ReadWriteTransactionWithOptions(ctx,
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
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
			return txn.BufferWrite([]*spanner.Mutation{
				spanner.Insert("kv",
					[]string{"rev", "key", "value", "old_value", "lease_id",
						"deleted", "created", "create_revision", "prev_revision"},
					[]interface{}{
						spanner.CommitTimestamp,
						key, value, prev.Value, leaseID,
						false, false,
						revToTS(prev.CreateRevision),
						revToTS(prev.Rev),
					},
				),
			})
		}, spanner.TransactionOptions{})

	status := "ok"
	if err != nil {
		status = "error"
	}
	metrics.KVOperationsTotal.WithLabelValues("update", status).Inc()
	metrics.SpannerTransactions.WithLabelValues(status).Inc()

	if errors.Is(err, ErrRevisionMismatch) || errors.Is(err, ErrKeyNotFound) {
		curRev, _ := s.CurrentRevision(ctx)
		return curRev, prev, false, nil
	}
	if err != nil {
		return 0, nil, false, err
	}
	commitTS = resp.CommitTs
	rev := tsToRev(commitTS)
	metrics.CurrentRevision.Set(float64(rev))
	s.watcher.notify(rev)
	return rev, prev, true, nil
}

// DeleteIfLease removes key only if its current row's lease_id matches leaseID.
// Returns (true, nil) if the tombstone was written, (false, nil) if the key
// was missing or belonged to a different lease, and (false, err) on failure.
// The lease_id check and the tombstone write are atomic in one Spanner RW txn.
func (s *Store) DeleteIfLease(ctx context.Context, key string, leaseID int64) (deleted bool, err error) {
	_, err = s.client.ReadWriteTransactionWithOptions(ctx,
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			prev, err := s.getLatestTxn(ctx, txn, key)
			if err != nil {
				return err
			}
			if prev == nil || prev.LeaseID != leaseID {
				// Key gone or reassigned to a different lease — nothing to do.
				deleted = false
				return nil
			}
			deleted = true
			return txn.BufferWrite([]*spanner.Mutation{
				spanner.Insert("kv",
					[]string{"rev", "key", "value", "old_value", "lease_id",
						"deleted", "created", "create_revision", "prev_revision"},
					[]interface{}{
						spanner.CommitTimestamp,
						key, []byte(nil), prev.Value, int64(0),
						true, false,
						revToTS(prev.CreateRevision),
						revToTS(prev.Rev),
					},
				),
			})
		}, spanner.TransactionOptions{})
	if err != nil {
		return false, err
	}
	return deleted, nil
}

// Delete removes key at the given revision (CAS). revision=0 = unconditional.
func (s *Store) Delete(ctx context.Context, key string, revision int64) (int64, *KV, bool, error) {
	var commitTS time.Time
	var prev *KV

	resp, err := s.client.ReadWriteTransactionWithOptions(ctx,
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
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
			return txn.BufferWrite([]*spanner.Mutation{
				spanner.Insert("kv",
					[]string{"rev", "key", "value", "old_value", "lease_id",
						"deleted", "created", "create_revision", "prev_revision"},
					[]interface{}{
						spanner.CommitTimestamp,
						key, []byte(nil), prev.Value, int64(0),
						true, false,
						revToTS(prev.CreateRevision),
						revToTS(prev.Rev),
					},
				),
			})
		}, spanner.TransactionOptions{})

	status := "ok"
	if err != nil {
		status = "error"
	}
	metrics.KVOperationsTotal.WithLabelValues("delete", status).Inc()
	metrics.SpannerTransactions.WithLabelValues(status).Inc()

	if errors.Is(err, ErrRevisionMismatch) || errors.Is(err, ErrKeyNotFound) {
		curRev, _ := s.CurrentRevision(ctx)
		return curRev, prev, false, nil
	}
	if err != nil {
		return 0, nil, false, err
	}
	commitTS = resp.CommitTs
	rev := tsToRev(commitTS)
	metrics.CurrentRevision.Set(float64(rev))
	s.watcher.notify(rev)
	return rev, prev, true, nil
}

// After returns all events with rev > afterRev matching prefix.
func (s *Store) After(ctx context.Context, prefix string, afterRev, limit int64) (int64, []*Event, error) {
	currentRev, err := s.CurrentRevision(ctx)
	if err != nil {
		return 0, nil, err
	}
	// afterRev=0 means "from beginning" — used by the poll loop, not for client replay.
	// Only check compaction for explicit client-supplied revisions.
	if afterRev > 0 {
		compactRev, cerr := s.compactRevision(ctx)
		if cerr != nil {
			return 0, nil, fmt.Errorf("get compact revision: %w", cerr)
		}
		// afterRev is exclusive (returns rev > afterRev). etcd compaction is
		// inclusive: reads at compactRev itself are disallowed. So we error when
		// afterRev < compactRev (meaning the first possible event is at or below
		// the compact horizon).
		if afterRev < compactRev {
			return currentRev, nil, ErrCompacted
		}
	}

	afterTS := revToTS(afterRev)
	stmt := spanner.Statement{
		SQL: `SELECT rev, key, value, old_value, lease_id, deleted, created, create_revision, prev_revision
		      FROM kv
		      WHERE key LIKE @prefix
		        AND rev > @after_rev
		      ORDER BY rev ASC` + limitClause(limit),
		Params: map[string]interface{}{
			"prefix":    likePrefix(prefix),
			"after_rev": afterTS,
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

// TxnOp represents a single operation within an atomic Txn.
type TxnOp struct {
	Type    TxnOpType
	Key     string
	Value   []byte
	LeaseID int64
}

type TxnOpType int

const (
	TxnOpPut    TxnOpType = 0
	TxnOpDelete TxnOpType = 1
	TxnOpGet    TxnOpType = 2
)

// TxnResult is the result of a single TxnOp.
type TxnResult struct {
	KV  *KV
	Rev int64
	Ok  bool
}

// AtomicTxn executes compare+ops in a single Spanner ReadWriteTransaction.
// All compare reads and op writes are atomic — no TOCTOU window.
// Returns (succeeded, results, commitRev, error).
func (s *Store) AtomicTxn(
	ctx context.Context,
	compares []TxnCompare,
	successOps []TxnOp,
	failureOps []TxnOp,
) (succeeded bool, results []TxnResult, commitRev int64, err error) {
	// Validate compares before opening a Spanner transaction.
	for _, c := range compares {
		if c.Err != nil {
			return false, nil, 0, c.Err
		}
	}

	var hasMutations bool

	resp, txnErr := s.client.ReadWriteTransactionWithOptions(ctx,
		func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
			succeeded = true
			results = nil
			hasMutations = false

			// Evaluate all compare conditions inside the transaction.
			for _, c := range compares {
				kv, err := s.getLatestTxn(ctx, txn, c.Key)
				if err != nil {
					return err
				}
				if !c.Evaluate(kv) {
					succeeded = false
					break
				}
			}

			ops := successOps
			if !succeeded {
				ops = failureOps
			}

			// Execute ops and buffer mutations.
			for _, op := range ops {
				switch op.Type {
				case TxnOpGet:
					kv, err := s.getLatestTxn(ctx, txn, op.Key)
					if err != nil {
						return err
					}
					results = append(results, TxnResult{KV: kv, Ok: kv != nil})

				case TxnOpPut:
					prev, err := s.getLatestTxn(ctx, txn, op.Key)
					if err != nil {
						return err
					}
					var createRevTS interface{} = spanner.CommitTimestamp
					var prevRevTS interface{} = (*time.Time)(nil)
					if prev != nil {
						createRevTS = revToTS(prev.CreateRevision)
						prevRevTS = revToTS(prev.Rev)
					}
					if err := txn.BufferWrite([]*spanner.Mutation{
						spanner.Insert("kv",
							[]string{"rev", "key", "value", "old_value", "lease_id",
								"deleted", "created", "create_revision", "prev_revision"},
							[]interface{}{
								spanner.CommitTimestamp,
								op.Key, op.Value,
								func() []byte {
									if prev != nil {
										return prev.Value
									}
									return nil
								}(),
								op.LeaseID,
								false,
								prev == nil, // created=true only on first write
								createRevTS,
								prevRevTS,
							},
						),
					}); err != nil {
						return err
					}
					hasMutations = true
					results = append(results, TxnResult{KV: prev, Ok: true})

				case TxnOpDelete:
					prev, err := s.getLatestTxn(ctx, txn, op.Key)
					if err != nil {
						return err
					}
					if prev == nil {
						results = append(results, TxnResult{Ok: false})
						continue
					}
					if err := txn.BufferWrite([]*spanner.Mutation{
						spanner.Insert("kv",
							[]string{"rev", "key", "value", "old_value", "lease_id",
								"deleted", "created", "create_revision", "prev_revision"},
							[]interface{}{
								spanner.CommitTimestamp,
								op.Key, []byte(nil), prev.Value, int64(0),
								true, false,
								revToTS(prev.CreateRevision),
								revToTS(prev.Rev),
							},
						),
					}); err != nil {
						return err
					}
					hasMutations = true
					results = append(results, TxnResult{KV: prev, Ok: true})

				default:
					return fmt.Errorf("unknown TxnOpType: %d", op.Type)
				}
			}
			return nil
		}, spanner.TransactionOptions{})

	status := "ok"
	if txnErr != nil {
		status = "error"
	}
	metrics.KVOperationsTotal.WithLabelValues("txn", status).Inc()
	metrics.SpannerTransactions.WithLabelValues(status).Inc()

	if txnErr != nil {
		return false, nil, 0, txnErr
	}
	if hasMutations {
		commitRev = tsToRev(resp.CommitTs)
		if commitRev > 0 {
			metrics.CurrentRevision.Set(float64(commitRev))
			s.watcher.notify(commitRev)
		}
	} else {
		// No mutations — return the revision observed inside the transaction
		// snapshot so callers get a consistent read revision, not a later one.
		commitRev = tsToRev(resp.CommitTs)
		if commitRev <= 1 {
			// Fallback: empty store or no prior writes.
			commitRev, err = s.CurrentRevision(ctx)
			if err != nil {
				return false, nil, 0, err
			}
		}
	}
	return succeeded, results, commitRev, nil
}

// TxnCompare is a single compare predicate for AtomicTxn.
// If Err is non-nil, AtomicTxn returns it immediately without executing.
type TxnCompare struct {
	Key      string
	Evaluate func(kv *KV) bool
	Err      error // pre-validation error (e.g. unsupported compare target)
}

// Compact records the compaction revision.
func (s *Store) Compact(ctx context.Context, targetRev int64) (int64, error) {
	targetTS := revToTS(targetRev)
	_, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		// CAS: only advance compact revision if targetTS is larger than existing.
		// This prevents a race where auto-compact overwrites a larger manual target.
		row, err := txn.ReadRow(ctx, "kv_rev", spanner.Key{schema.CompactRevRow}, []string{"rev"})
		if err != nil && spanner.ErrCode(err) != codes.NotFound {
			return err
		}
		if err == nil {
			var existing time.Time
			if scanErr := row.Column(0, &existing); scanErr != nil {
				return fmt.Errorf("decode existing compact rev: %w", scanErr)
			}
			if !existing.Before(targetTS) {
				return nil // already at or beyond targetRev
			}
		}
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.InsertOrUpdateMap("kv_rev", map[string]interface{}{
				"id":  schema.CompactRevRow,
				"rev": targetTS,
			}),
		})
	})
	if err != nil {
		return 0, err
	}

	// Use the effective compact revision for deletion: if kv_rev was already
	// at a larger value (prior incomplete compaction), delete up to that larger
	// revision so stale rows above targetRev are not left behind.
	effectiveRev := targetRev
	if stored, err := s.compactRevision(ctx); err == nil && stored > effectiveRev {
		effectiveRev = stored
	}

	go func() {
		ctx, cancel := context.WithTimeout(s.bgCtx, 30*time.Minute)
		defer cancel()
		t0 := time.Now()
		total := s.compactRows(ctx, effectiveRev)
		dur := time.Since(t0)
		metrics.CompactedRowsTotal.WithLabelValues("manual").Add(float64(total))
		metrics.CompactionDuration.WithLabelValues("manual").Observe(dur.Seconds())
		if total > 0 {
			s.log.Info("compacted old revisions",
				zap.Int("deleted", total),
				zap.Int64("requested_rev", targetRev),
				zap.Int64("effective_rev", effectiveRev),
				zap.Duration("duration", dur))
		}
	}()

	return targetRev, nil
}

// CompactRevision returns the last compacted revision (1 if never compacted).
func (s *Store) CompactRevision(ctx context.Context) (int64, error) {
	return s.compactRevision(ctx)
}

// Watch returns a channel of events for keys matching prefix, starting at afterRev.
func (s *Store) Watch(ctx context.Context, prefix string, afterRev int64) <-chan []*Event {
	return s.watcher.subscribe(ctx, prefix, afterRev)
}

// ─── helpers ────────────────────────────────────────────────────────────────

// tsToRev converts a Spanner TIMESTAMP to int64 revision (UnixNano).
func tsToRev(ts time.Time) int64 {
	n := ts.UnixNano()
	if n <= 0 {
		return 1
	}
	return n
}

// revToTS converts an int64 revision (UnixNano) to time.Time.
func revToTS(rev int64) time.Time {
	if rev <= 1 {
		return time.Unix(0, 1)
	}
	return time.Unix(0, rev)
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

// getLatestTxn returns the most recent non-deleted KV row for key within a txn.
func (s *Store) getLatestTxn(ctx context.Context, txn *spanner.ReadWriteTransaction, key string) (*KV, error) {
	stmt := spanner.Statement{
		SQL: `SELECT rev, key, value, old_value, lease_id, deleted, created, create_revision, prev_revision
		      FROM kv WHERE key = @key ORDER BY rev DESC LIMIT 1`,
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

// compactRevision reads the stored compact revision (1 if absent).
func (s *Store) compactRevision(ctx context.Context) (int64, error) {
	row, err := s.client.Single().ReadRow(ctx, "kv_rev",
		spanner.Key{schema.CompactRevRow}, []string{"rev"})
	if err != nil {
		if spanner.ErrCode(err) == codes.NotFound {
			return 1, nil
		}
		return 0, err
	}
	var ts time.Time
	_ = row.Column(0, &ts)
	return tsToRev(ts), nil
}

// autoCompactLoop runs in the background and compacts old revisions periodically.
// It targets currentRevision - autoCompactAge, keeping recent history for Watch replay.
func (s *Store) autoCompactLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.AutoCompactInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			curRev, err := s.CurrentRevision(ctx)
			if err != nil {
				s.log.Warn("auto-compact: failed to read current revision", zap.Error(err))
				continue
			}
			if curRev <= 1 {
				continue
			}
			targetRev := tsToRev(revToTS(curRev).Add(-s.cfg.AutoCompactAge))
			if targetRev <= 1 {
				continue
			}
			// If a manual Compact() has already set a larger compact revision,
			// use that as the deletion target so stale rows above our age-based
			// target are not left behind indefinitely.
			if existingRev, err := s.compactRevision(ctx); err == nil && existingRev > targetRev {
				targetRev = existingRev
			}
			// Record the compact revision in kv_rev before deleting rows so that
			// CompactRevision() reflects what background compaction has done.
			// Use CAS inside the transaction: only advance kv_rev if the new target
			// is larger than what is already stored — prevents auto-compact from
			// overwriting a larger manual compact revision with a smaller value.
			targetTS := revToTS(targetRev)
			if _, err := s.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
				row, err := txn.ReadRow(ctx, "kv_rev", spanner.Key{schema.CompactRevRow}, []string{"rev"})
				if err != nil && spanner.ErrCode(err) != codes.NotFound {
					return err
				}
				if err == nil {
					var existing time.Time
					if scanErr := row.Column(0, &existing); scanErr != nil {
						return fmt.Errorf("decode existing compact rev: %w", scanErr)
					}
					if !existing.Before(targetTS) {
						return nil // existing compact rev is already >= target, skip
					}
				}
				return txn.BufferWrite([]*spanner.Mutation{
					spanner.InsertOrUpdateMap("kv_rev", map[string]interface{}{
						"id":  schema.CompactRevRow,
						"rev": targetTS,
					}),
				})
			}); err != nil {
				s.log.Warn("auto-compact: failed to record compact revision", zap.Error(err))
				continue
			}

			runCtx, runCancel := context.WithTimeout(ctx, 30*time.Minute)
			t0 := time.Now()
			total := s.compactRows(runCtx, targetRev)
			dur := time.Since(t0)
			runCancel()
			metrics.CompactedRowsTotal.WithLabelValues("auto").Add(float64(total))
			metrics.CompactionDuration.WithLabelValues("auto").Observe(dur.Seconds())
			if total > 0 {
				s.log.Info("auto-compacted old revisions",
					zap.Int("deleted", total),
					zap.Int64("target_rev", targetRev),
					zap.Duration("duration", dur))
			}
		}
	}
}

// compactRows physically deletes old revisions in batches until none remain.
// Returns the total number of rows deleted.
func (s *Store) compactRows(ctx context.Context, targetRev int64) int {
	targetTS := revToTS(targetRev)
	total := 0
	for {
		ids, err := s.scanCompactBatch(ctx, targetTS)
		if err != nil {
			s.log.Warn("compact rows scan failed",
				zap.Int64("target_rev", targetRev), zap.Error(err))
			return total
		}
		if len(ids) == 0 {
			return total
		}

		mutations := make([]*spanner.Mutation, len(ids))
		for i, id := range ids {
			mutations[i] = spanner.Delete("kv", id)
		}
		if _, err := s.client.Apply(ctx, mutations); err != nil {
			s.log.Warn("compact rows delete failed",
				zap.Int64("target_rev", targetRev), zap.Int("count", len(ids)), zap.Error(err))
			return total
		}
		total += len(ids)

		if len(ids) < compactBatchSize {
			return total
		}
	}
}

// scanCompactBatch fetches one batch of row IDs eligible for compaction.
// A row is eligible only if there exists a strictly newer row for the same key.
// This covers both stale historical revisions AND tombstones — but never deletes
// the latest row for a key (whether it is a live value or a tombstone).
func (s *Store) scanCompactBatch(ctx context.Context, targetTS time.Time) ([]spanner.Key, error) {
	stmt := spanner.Statement{
		SQL: `SELECT kv.id FROM kv
		      WHERE kv.rev <= @target
		        AND EXISTS (
		          SELECT 1 FROM kv AS newer
		          WHERE newer.key = kv.key AND newer.rev > kv.rev
		        )
		      LIMIT @batch`,
		Params: map[string]interface{}{
			"target": targetTS,
			"batch":  int64(compactBatchSize),
		},
	}

	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var ids []spanner.Key
	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			return ids, nil
		}
		if err != nil {
			return nil, err
		}
		var id int64
		if err := row.Column(0, &id); err != nil {
			return nil, fmt.Errorf("decode compact row id: %w", err)
		}
		ids = append(ids, spanner.Key{id})
	}
}

// scanKV reads a Spanner row into a KV struct.
// rev, create_revision, prev_revision are TIMESTAMP → int64 (UnixNano).
func scanKV(row *spanner.Row) (*KV, error) {
	var kv KV
	var revTS time.Time
	var createRevTS spanner.NullTime
	var prevRevTS spanner.NullTime
	var value, oldValue []byte

	if err := row.Columns(
		&revTS, &kv.Key, &value, &oldValue,
		&kv.LeaseID, &kv.Deleted, &kv.Created,
		&createRevTS, &prevRevTS,
	); err != nil {
		return nil, fmt.Errorf("scan kv row: %w", err)
	}
	kv.Rev = tsToRev(revTS)
	kv.Value = value
	kv.OldValue = oldValue
	if createRevTS.Valid {
		kv.CreateRevision = tsToRev(createRevTS.Time)
	}
	if prevRevTS.Valid {
		kv.PrevRevision = tsToRev(prevRevTS.Time)
	}
	return &kv, nil
}

func likePrefix(prefix string) string { return prefix + "%" }

func limitClause(limit int64) string {
	if limit <= 0 {
		return ""
	}
	return fmt.Sprintf(" LIMIT %d", limit)
}

// revCap returns revision if set, otherwise currentRev.
func revCap(revision, currentRev int64) int64 {
	if revision > 0 {
		return revision
	}
	return currentRev
}

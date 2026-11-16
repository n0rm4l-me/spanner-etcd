package store

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"

	"github.com/n0rm4l-me/spanner-etcd/pkg/metrics"
)

const (
	// leaseReapInterval is how often every replica looks for expired leases.
	leaseReapInterval = time.Second
	// leaseCountInterval is how often the active-lease gauge is refreshed.
	leaseCountInterval = 15 * time.Second

	// leaseExpiryExpr is the SQL expression for a lease's expiry time.
	// Rows written before expires_at existed (NULL) fall back to granted_at + ttl.
	leaseExpiryExpr = `COALESCE(expires_at, TIMESTAMP_ADD(granted_at, INTERVAL ttl_sec SECOND))`
)

// Lease represents a lease as returned by Grant.
type Lease struct {
	ID        int64
	TTL       int64
	GrantedAt time.Time
	ExpiresAt time.Time
}

// LeaseManager implements etcd leases on top of the kv_lease table.
//
// A lease is cluster state, not replica state: the row in kv_lease (with its
// expires_at) is the single source of truth. Grant, Keepalive and TimeToLive
// work on any replica regardless of which replica granted the lease, and every
// replica runs a reaper that expires leases whose expires_at has passed.
//
// All expiry arithmetic is done by Spanner (CURRENT_TIMESTAMP), never by the
// replica's own clock, so clock skew between pods cannot expire a lease early
// or keep it alive too long. Expiry is monotonic: Keepalive only extends a
// lease that has not expired yet, so once expires_at has passed the lease is
// dead and any number of replicas may race to reap it, which is idempotent.
type LeaseManager struct {
	store  *Store
	log    *zap.Logger
	nextID atomic.Int64
	stopCh chan struct{}
}

// newLeaseManager starts the reaper on ctx, which must be the server-lifetime
// context, not a request context.
func newLeaseManager(ctx context.Context, s *Store, log *zap.Logger) *LeaseManager {
	lm := &LeaseManager{
		store:  s,
		log:    log,
		stopCh: make(chan struct{}),
	}
	lm.nextID.Store(time.Now().UnixNano())
	go lm.reapLoop(ctx)
	return lm
}

func (lm *LeaseManager) close() {
	close(lm.stopCh)
}

// Grant creates a new lease with the given TTL (seconds).
func (lm *LeaseManager) Grant(ctx context.Context, ttl int64) (*Lease, error) {
	id := lm.nextID.Add(1)
	now := time.Now()

	_, err := lm.store.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, spanner.Statement{
			SQL: `INSERT INTO kv_lease (lease_id, ttl_sec, granted_at, expires_at)
			      VALUES (@id, @ttl, PENDING_COMMIT_TIMESTAMP(),
			              TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL @ttl SECOND))`,
			Params: map[string]interface{}{"id": id, "ttl": ttl},
		})
		return err
	})
	if err != nil {
		return nil, err
	}

	return &Lease{
		ID:        id,
		TTL:       ttl,
		GrantedAt: now,
		ExpiresAt: now.Add(time.Duration(ttl) * time.Second),
	}, nil
}

// Revoke removes a lease and deletes all keys associated with it.
// The lease row is removed only after its keys were deleted, so if deletion
// fails transiently the lease stays in kv_lease and is retried (by the caller,
// or by the reaper once it has expired).
func (lm *LeaseManager) Revoke(ctx context.Context, id int64) error {
	_, err := lm.expireLeaseKeys(ctx, id)
	return err
}

// LeaseSnapshot is an immutable point-in-time copy of a lease's state.
type LeaseSnapshot struct {
	ID        int64
	TTL       int64 // original grant TTL
	Remaining int64 // seconds until expiry (0 if already expired)
}

// GetTTL returns the lease and its remaining TTL as computed by Spanner.
// Returns (zero, false, nil) if the lease does not exist.
func (lm *LeaseManager) GetTTL(ctx context.Context, id int64) (LeaseSnapshot, bool, error) {
	iter := lm.store.client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT ttl_sec, TIMESTAMP_DIFF(` + leaseExpiryExpr + `, CURRENT_TIMESTAMP(), SECOND)
		      FROM kv_lease WHERE lease_id = @id`,
		Params: map[string]interface{}{"id": id},
	})
	defer iter.Stop()

	row, err := iter.Next()
	if errors.Is(err, iterator.Done) {
		return LeaseSnapshot{}, false, nil
	}
	if err != nil {
		return LeaseSnapshot{}, false, fmt.Errorf("lease ttl %d: %w", id, err)
	}
	var ttl, remaining int64
	if err := row.Columns(&ttl, &remaining); err != nil {
		return LeaseSnapshot{}, false, err
	}
	if remaining < 0 {
		remaining = 0
	}
	return LeaseSnapshot{ID: id, TTL: ttl, Remaining: remaining}, true, nil
}

// Keepalive extends the lease to now + TTL and returns the TTL.
// It returns 0 (and no error) if the lease does not exist or has already
// expired; etcd clients read TTL 0 as "lease lost".
//
// The extension is persisted on every call, not coalesced: the lease lifetime
// after the last keepalive is exactly one TTL, as in etcd. Skipping writes
// would shorten that margin, which matters for clients that send keepalives
// only a few times per TTL.
func (lm *LeaseManager) Keepalive(ctx context.Context, id int64) (int64, error) {
	var ttl int64
	_, err := lm.store.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		ttl = 0 // reset: Spanner may retry this callback on ABORTED

		iter := txn.Query(ctx, spanner.Statement{
			SQL: `SELECT ttl_sec FROM kv_lease
			      WHERE lease_id = @id AND ` + leaseExpiryExpr + ` > CURRENT_TIMESTAMP()`,
			Params: map[string]interface{}{"id": id},
		})
		row, err := iter.Next()
		iter.Stop()
		if errors.Is(err, iterator.Done) {
			return nil // unknown or expired
		}
		if err != nil {
			return err
		}
		var granted int64
		if err := row.Columns(&granted); err != nil {
			return err
		}

		if _, err := txn.Update(ctx, spanner.Statement{
			SQL: `UPDATE kv_lease
			      SET expires_at = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL ttl_sec SECOND)
			      WHERE lease_id = @id`,
			Params: map[string]interface{}{"id": id},
		}); err != nil {
			return err
		}
		ttl = granted
		return nil
	})
	if err != nil {
		return 0, err
	}
	return ttl, nil
}

// reapLoop expires leases on this replica until the store is closed.
// Every replica runs it; they may reap the same lease concurrently, which is
// safe because each step is idempotent. The interval is jittered so replicas
// started together do not stay in lockstep.
func (lm *LeaseManager) reapLoop(ctx context.Context) {
	var lastCount time.Time
	for {
		wait := leaseReapInterval + time.Duration(rand.Int63n(int64(leaseReapInterval/2)))
		select {
		case <-lm.stopCh:
			return
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}

		lm.reapExpired(ctx)
		if time.Since(lastCount) >= leaseCountInterval {
			lm.refreshActiveGauge(ctx)
			lastCount = time.Now()
		}
	}
}

// reapExpired revokes every lease whose expiry has passed. A failure leaves the
// lease in kv_lease, so the next tick retries it.
func (lm *LeaseManager) reapExpired(ctx context.Context) {
	ids, err := lm.expiredLeaseIDs(ctx)
	if err != nil {
		if ctx.Err() == nil {
			lm.log.Warn("lease reaper: failed to list expired leases", zap.Error(err))
		}
		return
	}
	for _, id := range ids {
		if ctx.Err() != nil {
			return
		}
		removed, err := lm.expireLeaseKeys(ctx, id)
		// Only the replica that actually removed the lease row counts it, so
		// concurrent reapers do not double-count.
		if removed {
			lm.log.Info("lease expired", zap.Int64("lease_id", id))
			metrics.LeaseExpirations.Inc()
		}
		if err != nil && ctx.Err() == nil {
			lm.log.Warn("lease expiry error",
				zap.Int64("lease_id", id), zap.Bool("lease_row_removed", removed), zap.Error(err))
		}
	}
}

// expiredLeaseIDs lists leases whose expiry is in the past (Spanner time).
func (lm *LeaseManager) expiredLeaseIDs(ctx context.Context) ([]int64, error) {
	iter := lm.store.client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT lease_id FROM kv_lease WHERE ` + leaseExpiryExpr + ` <= CURRENT_TIMESTAMP()`,
	})
	defer iter.Stop()

	var ids []int64
	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			return ids, nil
		}
		if err != nil {
			return nil, err
		}
		var id int64
		if err := row.Columns(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
}

// refreshActiveGauge sets the active-lease gauge from the cluster-wide count.
func (lm *LeaseManager) refreshActiveGauge(ctx context.Context) {
	iter := lm.store.client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT COUNT(*) FROM kv_lease WHERE ` + leaseExpiryExpr + ` > CURRENT_TIMESTAMP()`,
	})
	defer iter.Stop()
	row, err := iter.Next()
	if err != nil {
		return
	}
	var n int64
	if err := row.Columns(&n); err != nil {
		return
	}
	metrics.ActiveLeases.Set(float64(n))
}

// requireLiveLease returns ErrLeaseNotFound unless leaseID is 0 (no lease) or
// names a lease that exists and has not expired.
//
// It must run inside the same read-write transaction as the write that attaches
// a key to the lease. Reading the lease row takes a lock that conflicts with the
// reaper deleting that row, so a Put cannot slip a key under a lease that is
// being torn down without either failing here or being seen by the reaper's
// second pass (see expireLeaseKeys).
func requireLiveLease(ctx context.Context, txn *spanner.ReadWriteTransaction, leaseID int64) error {
	if leaseID == 0 {
		return nil
	}
	iter := txn.Query(ctx, spanner.Statement{
		SQL: `SELECT 1 FROM kv_lease
		      WHERE lease_id = @id AND ` + leaseExpiryExpr + ` > CURRENT_TIMESTAMP()`,
		Params: map[string]interface{}{"id": leaseID},
	})
	defer iter.Stop()
	_, err := iter.Next()
	if errors.Is(err, iterator.Done) {
		return ErrLeaseNotFound
	}
	return err
}

// expireLeaseKeys deletes all keys whose latest revision still belongs to leaseID
// and then removes the lease row. It reports whether this call removed the row.
//
// The key scan runs twice: before and after the lease row is removed. A Put that
// validated the lease just before the row was deleted commits after the first
// scan; once the row is gone no new Put can validate, so the second scan sees
// everything that slipped in and deletes it. If it fails the row is already
// gone, so the error is returned to the caller: Revoke can simply be retried
// (the scans do not depend on the row), the reaper cannot.
func (lm *LeaseManager) expireLeaseKeys(ctx context.Context, leaseID int64) (removed bool, err error) {
	if err := lm.deleteLeaseKeys(ctx, leaseID); err != nil {
		return false, err
	}

	// Remove the lease row. The affected-row count tells us whether a
	// concurrent reaper or Revoke got there first.
	_, err = lm.store.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		n, err := txn.Update(ctx, spanner.Statement{
			SQL:    `DELETE FROM kv_lease WHERE lease_id = @id`,
			Params: map[string]interface{}{"id": leaseID},
		})
		removed = n > 0
		return err
	})
	if err != nil {
		lm.log.Warn("expireLeaseKeys: failed to delete lease record",
			zap.Int64("lease_id", leaseID), zap.Error(err))
		return false, err
	}

	if err := lm.deleteLeaseKeys(ctx, leaseID); err != nil {
		lm.log.Error("expireLeaseKeys: lease row removed but cleanup pass failed; retry Revoke to finish",
			zap.Int64("lease_id", leaseID), zap.Error(err))
		return removed, err
	}
	return removed, nil
}

// deleteLeaseKeys deletes all keys whose latest revision still belongs to leaseID.
//
// Each deletion is performed by DeleteIfLease, a Spanner RW transaction that
// re-reads the key and only writes a tombstone when lease_id still matches, so
// the check and the delete are atomic. It returns an error if the scan or any
// delete failed, so the caller keeps the lease row and retries.
func (lm *LeaseManager) deleteLeaseKeys(ctx context.Context, leaseID int64) error {
	// Find all keys where the MAX(rev) row has lease_id=@lid and is not deleted.
	// Note: the subquery filters MAX(rev) only among rows with lease_id=@lid,
	// so a key reassigned to a different lease may still appear here.
	// DeleteIfLease() re-checks the lease_id atomically and no-ops if mismatched.
	stmt := spanner.Statement{
		SQL: `SELECT key FROM kv
		      INNER JOIN (
		        SELECT key AS k2, MAX(rev) AS max_rev FROM kv WHERE lease_id = @lid GROUP BY key
		      ) AS latest ON kv.key = latest.k2 AND kv.rev = latest.max_rev
		      WHERE kv.deleted = false AND kv.lease_id = @lid`,
		Params: map[string]interface{}{"lid": leaseID},
	}

	iter := lm.store.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var (
		scanErr    error
		deleteErrs int
	)
	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			// Transient Spanner error mid-scan: stop without deleting the lease
			// record so the caller can retry. Deleting kv_lease on a partial scan
			// would orphan keys that were not yet processed.
			scanErr = err
			break
		}
		var key string
		if err := row.Columns(&key); err != nil {
			lm.log.Warn("failed to decode lease key row", zap.Error(err))
			deleteErrs++
			continue
		}
		if _, err := lm.store.DeleteIfLease(ctx, key, leaseID); err != nil {
			lm.log.Warn("failed to delete lease key", zap.String("key", key), zap.Error(err))
			deleteErrs++
		}
	}

	if scanErr != nil {
		lm.log.Warn("deleteLeaseKeys: scan failed",
			zap.Int64("lease_id", leaseID), zap.Error(scanErr))
		return scanErr
	}
	if deleteErrs > 0 {
		lm.log.Warn("deleteLeaseKeys: some key deletes failed",
			zap.Int64("lease_id", leaseID), zap.Int("failed", deleteErrs))
		return fmt.Errorf("expireLeaseKeys: %d key delete(s) failed for lease %d", deleteErrs, leaseID)
	}
	return nil
}

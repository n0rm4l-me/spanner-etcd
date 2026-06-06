package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"

	"github.com/n0rm4l-me/spanner-etcd/pkg/metrics"
)

// Lease represents an active lease.
type Lease struct {
	ID         int64
	TTL        int64
	GrantedAt  time.Time
	ExpiresAt  time.Time
}

// LeaseManager tracks active leases and expires keys when TTL runs out.
// It is horizontally safe: each replica runs its own expiry goroutines, but
// deletes are idempotent (CAS on rev=0 means "delete if exists").
type LeaseManager struct {
	store  *Store
	log    *zap.Logger
	nextID atomic.Int64
	bgCtx  context.Context // long-lived context for expiry goroutines

	mu     sync.Mutex
	leases map[int64]*Lease
	stopCh chan struct{}
}

func newLeaseManager(ctx context.Context, s *Store, log *zap.Logger) *LeaseManager {
	lm := &LeaseManager{
		store:  s,
		log:    log,
		leases: make(map[int64]*Lease),
		stopCh: make(chan struct{}),
		bgCtx:  ctx, // server-lifetime context, not request context
	}
	lm.nextID.Store(time.Now().UnixNano())
	go lm.gcLoop(ctx)
	return lm
}

func (lm *LeaseManager) close() {
	close(lm.stopCh)
}

// Grant creates a new lease with the given TTL.
func (lm *LeaseManager) Grant(ctx context.Context, ttl int64) (*Lease, error) {
	id := lm.nextID.Add(1)
	now := time.Now()
	lease := &Lease{
		ID:        id,
		TTL:       ttl,
		GrantedAt: now,
		ExpiresAt: now.Add(time.Duration(ttl) * time.Second),
	}

	_, err := lm.store.client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertMap("kv_lease", map[string]interface{}{
			"lease_id":   id,
			"ttl_sec":    ttl,
			"granted_at": spanner.CommitTimestamp,
		}),
	})
	if err != nil {
		return nil, err
	}

	lm.mu.Lock()
	lm.leases[id] = lease
	lm.mu.Unlock()
	metrics.ActiveLeases.Inc()

	// Schedule expiry using the server-lifetime context, not the request context.
	// The request context is cancelled as soon as LeaseGrant returns to the client.
	go lm.scheduleExpiry(lm.bgCtx, lease)
	return lease, nil
}

// Revoke removes a lease and deletes all keys associated with it.
// The lease is removed from the in-memory map only after expireLeaseKeys
// succeeds — if the key deletion fails transiently, the lease remains in
// memory so scheduleExpiry can retry and the gcLoop can reload it on restart.
func (lm *LeaseManager) Revoke(ctx context.Context, id int64) error {
	// Use bgCtx so expiry works even when called from a short-lived request ctx.
	if err := lm.expireLeaseKeys(lm.bgCtx, id); err != nil {
		return err
	}

	lm.mu.Lock()
	_, existed := lm.leases[id]
	delete(lm.leases, id)
	lm.mu.Unlock()
	if existed {
		metrics.ActiveLeases.Dec()
	}
	return nil
}

// Get returns a lease by ID, or nil.
func (lm *LeaseManager) Get(id int64) *Lease {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.leases[id]
}

// LeaseSnapshot is an immutable point-in-time copy of a Lease's fields.
// Safe to read without holding lm.mu.
type LeaseSnapshot struct {
	ID         int64
	TTL        int64 // original grant TTL
	Remaining  int64 // seconds until expiry (0 if already expired)
}

// GetTTL returns a snapshot of the lease and its remaining TTL.
// All fields are read under the mutex — no pointer to the live Lease is exposed.
// Returns (zero, false) if the lease is not found.
func (lm *LeaseManager) GetTTL(id int64) (LeaseSnapshot, bool) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lease, ok := lm.leases[id]
	if !ok {
		return LeaseSnapshot{}, false
	}
	remaining := int64(time.Until(lease.ExpiresAt).Seconds())
	if remaining < 0 {
		remaining = 0
	}
	return LeaseSnapshot{ID: lease.ID, TTL: lease.TTL, Remaining: remaining}, true
}

// Keepalive resets the expiry time for the lease.
func (lm *LeaseManager) Keepalive(ctx context.Context, id int64) (int64, error) {
	lm.mu.Lock()
	lease, ok := lm.leases[id]
	if !ok {
		lm.mu.Unlock()
		return 0, nil
	}
	lease.ExpiresAt = time.Now().Add(time.Duration(lease.TTL) * time.Second)
	lm.mu.Unlock()
	return lease.TTL, nil
}

// scheduleExpiry waits for the lease TTL then expires keys.
func (lm *LeaseManager) scheduleExpiry(ctx context.Context, lease *Lease) {
	lm.mu.Lock()
	expiresAt := lease.ExpiresAt
	lm.mu.Unlock()
	timer := time.NewTimer(time.Until(expiresAt))
	defer timer.Stop()

	select {
	case <-lm.stopCh:
		return
	case <-ctx.Done():
		return
	case <-timer.C:
	}

	// Check if lease was already revoked.
	lm.mu.Lock()
	current, ok := lm.leases[lease.ID]
	lm.mu.Unlock()
	if !ok {
		return
	}

	// If keepalive extended it, reschedule.
	// Read ExpiresAt under the mutex to avoid a data race with Keepalive.
	lm.mu.Lock()
	timeUntilExpiry := time.Until(current.ExpiresAt)
	lm.mu.Unlock()
	if timeUntilExpiry > time.Second {
		go lm.scheduleExpiry(ctx, current)
		return
	}

	lm.log.Info("lease expired", zap.Int64("lease_id", lease.ID))
	metrics.LeaseExpirations.Inc()

	// Retry expiry with backoff on transient Spanner errors so a temporary
	// failure does not leave the lease record and its keys stuck indefinitely.
	for attempt := 1; attempt <= 5; attempt++ {
		if err := lm.Revoke(ctx, lease.ID); err == nil {
			return
		} else {
			lm.log.Warn("lease expiry error, retrying",
				zap.Int64("lease_id", lease.ID),
				zap.Int("attempt", attempt),
				zap.Error(err))
			select {
			case <-time.After(time.Duration(attempt) * time.Second):
			case <-lm.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}
	lm.log.Error("lease expiry failed after retries — lease may be stuck",
		zap.Int64("lease_id", lease.ID))
}

// expireLeaseKeys deletes all keys whose latest revision still belongs to leaseID.
// Each deletion is performed by DeleteIfLease — a Spanner RW transaction that
// re-reads the key and only writes a tombstone when lease_id still matches.
// This closes the TOCTOU window between the initial scan and the delete.
func (lm *LeaseManager) expireLeaseKeys(ctx context.Context, leaseID int64) error {
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
		// Atomic lease_id-conditioned delete: only writes the tombstone if the
		// key's current row still belongs to leaseID. This closes the TOCTOU
		// window — the check and the delete are a single Spanner RW transaction.
		if _, err := lm.store.DeleteIfLease(ctx, key, leaseID); err != nil {
			lm.log.Warn("failed to delete lease key", zap.String("key", key), zap.Error(err))
			deleteErrs++
		}
	}

	if scanErr != nil {
		lm.log.Warn("expireLeaseKeys: scan failed, lease record preserved for retry",
			zap.Int64("lease_id", leaseID), zap.Error(scanErr))
		return scanErr
	}
	if deleteErrs > 0 {
		// Some key deletes failed — preserve the lease record so retries can
		// attempt to delete the remaining keys rather than orphaning them.
		lm.log.Warn("expireLeaseKeys: some key deletes failed, lease record preserved",
			zap.Int64("lease_id", leaseID), zap.Int("failed", deleteErrs))
		return fmt.Errorf("expireLeaseKeys: %d key delete(s) failed for lease %d", deleteErrs, leaseID)
	}

	// All keys processed successfully — remove the lease record.
	if _, err := lm.store.client.Apply(ctx, []*spanner.Mutation{
		spanner.Delete("kv_lease", spanner.Key{leaseID}),
	}); err != nil {
		lm.log.Warn("expireLeaseKeys: failed to delete lease record",
			zap.Int64("lease_id", leaseID), zap.Error(err))
		return err
	}
	return nil
}

// gcLoop reloads active leases on startup in case this replica restarted.
// Retries on transient Spanner errors so a mid-scan failure does not silently
// orphan the remaining leases.
func (lm *LeaseManager) gcLoop(ctx context.Context) {
	stmt := spanner.Statement{
		SQL: `SELECT lease_id, ttl_sec, granted_at FROM kv_lease`,
	}

	for attempt := 1; ; attempt++ {
		done, err := lm.loadLeases(ctx, stmt)
		if done || ctx.Err() != nil {
			return
		}
		if err != nil {
			lm.log.Warn("gcLoop: failed to reload leases, retrying",
				zap.Int("attempt", attempt), zap.Error(err))
			select {
			case <-time.After(time.Duration(attempt) * time.Second):
			case <-lm.stopCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}
}

// loadLeases performs one scan of kv_lease and schedules expiry for each entry.
// Returns (true, nil) when the scan completes successfully.
// Returns (false, err) on a Spanner error so gcLoop can retry.
func (lm *LeaseManager) loadLeases(ctx context.Context, stmt spanner.Statement) (bool, error) {
	iter := lm.store.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	for {
		row, err := iter.Next()
		if errors.Is(err, iterator.Done) {
			return true, nil
		}
		if err != nil {
			return false, err
		}
		var id, ttl int64
		var grantedAt time.Time
		if err := row.Columns(&id, &ttl, &grantedAt); err != nil {
			lm.log.Warn("gcLoop: failed to decode lease row", zap.Error(err))
			continue
		}
		lease := &Lease{
			ID:        id,
			TTL:       ttl,
			GrantedAt: grantedAt,
			ExpiresAt: grantedAt.Add(time.Duration(ttl) * time.Second),
		}
		lm.mu.Lock()
		if _, exists := lm.leases[id]; !exists {
			lm.leases[id] = lease
			metrics.ActiveLeases.Inc()
			go lm.scheduleExpiry(ctx, lease)
		}
		lm.mu.Unlock()
	}
}

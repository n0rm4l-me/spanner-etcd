package store

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/spanner"
	"go.uber.org/zap"

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
func (lm *LeaseManager) Revoke(ctx context.Context, id int64) error {
	lm.mu.Lock()
	_, existed := lm.leases[id]
	delete(lm.leases, id)
	lm.mu.Unlock()
	if existed {
		metrics.ActiveLeases.Dec()
	}

	// Use bgCtx so expiry works even when called from a short-lived request ctx.
	return lm.expireLeaseKeys(lm.bgCtx, id)
}

// Get returns a lease by ID, or nil.
func (lm *LeaseManager) Get(id int64) *Lease {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.leases[id]
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
	timer := time.NewTimer(time.Until(lease.ExpiresAt))
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
	if time.Until(current.ExpiresAt) > time.Second {
		go lm.scheduleExpiry(ctx, current)
		return
	}

	lm.log.Info("lease expired", zap.Int64("lease_id", lease.ID))
	metrics.LeaseExpirations.Inc()
	if err := lm.Revoke(ctx, lease.ID); err != nil {
		lm.log.Warn("lease expiry error", zap.Int64("lease_id", lease.ID), zap.Error(err))
	}
}

// expireLeaseKeys deletes all keys whose lease_id matches.
func (lm *LeaseManager) expireLeaseKeys(ctx context.Context, leaseID int64) error {
	// Find all keys with this lease.
	stmt := spanner.Statement{
		SQL: `SELECT key, rev FROM kv
		      INNER JOIN (
		        SELECT key AS k2, MAX(rev) AS max_rev FROM kv WHERE lease_id = @lid GROUP BY key
		      ) AS latest ON kv.key = latest.k2 AND kv.rev = latest.max_rev
		      WHERE kv.deleted = false AND kv.lease_id = @lid`,
		Params: map[string]interface{}{"lid": leaseID},
	}

	iter := lm.store.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	for {
		row, err := iter.Next()
		if err != nil {
			break
		}
		var key string
		var rev int64
		if err := row.Columns(&key, &rev); err != nil {
			continue
		}
		if _, _, _, err := lm.store.Delete(ctx, key, rev); err != nil {
			lm.log.Warn("failed to delete lease key", zap.String("key", key), zap.Error(err))
		}
	}

	// Remove lease record.
	lm.store.client.Apply(ctx, []*spanner.Mutation{ //nolint:errcheck
		spanner.Delete("kv_lease", spanner.Key{leaseID}),
	})
	return nil
}

// gcLoop reloads active leases on startup in case this replica restarted.
func (lm *LeaseManager) gcLoop(ctx context.Context) {
	// On startup, reload persisted leases and schedule their expiry.
	stmt := spanner.Statement{
		SQL: `SELECT lease_id, ttl_sec, granted_at FROM kv_lease`,
	}
	iter := lm.store.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	for {
		row, err := iter.Next()
		if err != nil {
			break
		}
		var id, ttl int64
		var grantedAt time.Time
		if err := row.Columns(&id, &ttl, &grantedAt); err != nil {
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
			go lm.scheduleExpiry(ctx, lease)
		}
		lm.mu.Unlock()
	}
}

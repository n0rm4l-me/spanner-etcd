package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/n0rm4l-me/spanner-etcd/pkg/store"
)

// The tests in this file model several spanner-etcd replicas (and restarts)
// sharing one database. A lease is cluster state: whichever replica a client
// talks to must see it, extend it and expire it the same way.

// waitKeyGone polls until key is deleted or the timeout elapses.
func waitKeyGone(t *testing.T, s *store.Store, key string, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, kv, err := s.Get(context.Background(), key, 0)
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		if kv == nil {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// A lease granted on replica A and kept alive through replica B (a different
// connection behind the load balancer) must not expire on schedule.
func TestLease_KeepaliveServedByOtherReplica(t *testing.T) {
	db := newTestDB(t)
	a := openStore(t, db, store.StoreConfig{})
	b := openStore(t, db, store.StoreConfig{}) // opened before the grant: has never seen the lease
	ctx := context.Background()

	lease, err := a.Leases().Grant(ctx, 3)
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	if _, err := a.Create(ctx, "/ml/k", []byte("v"), lease.ID); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Keep it alive for twice its TTL, always via B.
	for i := 0; i < 6; i++ {
		time.Sleep(time.Second)
		ttl, err := b.Leases().Keepalive(ctx, lease.ID)
		if err != nil {
			t.Fatalf("keepalive %d via B: %v", i, err)
		}
		if ttl != 3 {
			t.Fatalf("keepalive %d via B: want TTL=3, got %d (lease unknown to B?)", i, ttl)
		}
	}

	if _, kv, _ := a.Get(ctx, "/ml/k", 0); kv == nil {
		t.Fatal("key was deleted although its lease was kept alive via another replica")
	}
}

// A restarted replica must honour keepalives that were persisted before the
// restart instead of recomputing expiry from the original grant time.
func TestLease_KeepaliveSurvivesReplicaRestart(t *testing.T) {
	db := newTestDB(t)
	a := openStore(t, db, store.StoreConfig{})
	ctx := context.Background()

	lease, err := a.Leases().Grant(ctx, 3)
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	if _, err := a.Create(ctx, "/restart/k", []byte("v"), lease.ID); err != nil {
		t.Fatalf("create: %v", err)
	}

	// Keep alive past the original grant time + TTL (4s > 3s).
	for i := 0; i < 4; i++ {
		time.Sleep(time.Second)
		if ttl, err := a.Leases().Keepalive(ctx, lease.ID); err != nil || ttl != 3 {
			t.Fatalf("keepalive %d: ttl=%d err=%v", i, ttl, err)
		}
	}

	a.Close() // the replica goes away ...
	c := openStore(t, db, store.StoreConfig{})
	time.Sleep(time.Second) // ... and its replacement runs its startup scan

	if _, kv, _ := c.Get(ctx, "/restart/k", 0); kv == nil {
		t.Fatal("restarted replica expired a lease that had been kept alive")
	}
}

// If the replica that granted a lease dies, another replica must still expire
// the lease and delete its keys. Previously the keys stayed forever.
func TestLease_ExpiresWhenGrantingReplicaDies(t *testing.T) {
	db := newTestDB(t)
	a := openStore(t, db, store.StoreConfig{})
	b := openStore(t, db, store.StoreConfig{}) // opened before the grant
	ctx := context.Background()

	lease, err := a.Leases().Grant(ctx, 2)
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	if _, err := a.Create(ctx, "/orphan/k", []byte("v"), lease.ID); err != nil {
		t.Fatalf("create: %v", err)
	}
	a.Close()

	if !waitKeyGone(t, b, "/orphan/k", 10*time.Second) {
		t.Fatal("key still present 10s after a 2s lease expired: nobody expired it")
	}
}

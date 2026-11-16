package store_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/n0rm4l-me/spanner-etcd/pkg/store"
)

// A key must never be attached to a lease that does not exist or has expired:
// nothing would ever expire it, so it would live forever (a "zombie" key, for
// example a service-registry entry that never goes away). etcd rejects such a
// write with "requested lease not found"; so must we.

func TestPut_RejectsUnknownLease(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	_, _, err := s.Put(ctx, "/lw/unknown", []byte("v"), 424242)
	if !errors.Is(err, store.ErrLeaseNotFound) {
		t.Fatalf("want ErrLeaseNotFound, got %v", err)
	}
	if _, kv, _ := s.Get(ctx, "/lw/unknown", 0); kv != nil {
		t.Fatalf("a rejected Put must not write anything, found %+v", kv)
	}
}

func TestPut_RejectsExpiredLease(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	lease, err := s.Leases().Grant(ctx, 1)
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	time.Sleep(2500 * time.Millisecond) // past expiry; the reaper may or may not have run yet

	if _, _, err := s.Put(ctx, "/lw/expired", []byte("v"), lease.ID); !errors.Is(err, store.ErrLeaseNotFound) {
		t.Fatalf("want ErrLeaseNotFound for an expired lease, got %v", err)
	}
}

func TestPut_RejectsRevokedLease(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	lease, _ := s.Leases().Grant(ctx, 60)
	if _, _, err := s.Put(ctx, "/lw/revoked", []byte("v"), lease.ID); err != nil {
		t.Fatalf("put with live lease: %v", err)
	}
	if err := s.Leases().Revoke(ctx, lease.ID); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if _, _, err := s.Put(ctx, "/lw/revoked2", []byte("v"), lease.ID); !errors.Is(err, store.ErrLeaseNotFound) {
		t.Fatalf("want ErrLeaseNotFound after revoke, got %v", err)
	}
}

func TestPut_AcceptsLiveLeaseAndNoLease(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	lease, _ := s.Leases().Grant(ctx, 60)
	if _, _, err := s.Put(ctx, "/lw/live", []byte("v"), lease.ID); err != nil {
		t.Fatalf("put with live lease: %v", err)
	}
	if _, _, err := s.Put(ctx, "/lw/none", []byte("v"), 0); err != nil {
		t.Fatalf("put without lease: %v", err)
	}
}

func TestCreateUpdate_RejectUnknownLease(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if _, err := s.Create(ctx, "/lw/create", []byte("v"), 424242); !errors.Is(err, store.ErrLeaseNotFound) {
		t.Fatalf("create: want ErrLeaseNotFound, got %v", err)
	}

	rev, err := s.Create(ctx, "/lw/update", []byte("v"), 0)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, _, _, err := s.Update(ctx, "/lw/update", []byte("v2"), rev, 424242); !errors.Is(err, store.ErrLeaseNotFound) {
		t.Fatalf("update: want ErrLeaseNotFound, got %v", err)
	}
}

// A Txn that attaches one key to a dead lease fails as a whole: the other
// mutations in the same branch must not be applied.
func TestAtomicTxn_RejectsUnknownLeaseAtomically(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	_, _, _, err := s.AtomicTxn(ctx, nil,
		[]store.TxnOp{
			{Type: store.TxnOpPut, Key: "/lw/txn/ok", Value: []byte("a")},
			{Type: store.TxnOpPut, Key: "/lw/txn/bad", Value: []byte("b"), LeaseID: 424242},
		}, nil)
	if !errors.Is(err, store.ErrLeaseNotFound) {
		t.Fatalf("want ErrLeaseNotFound, got %v", err)
	}
	for _, k := range []string{"/lw/txn/ok", "/lw/txn/bad"} {
		if _, kv, _ := s.Get(ctx, k, 0); kv != nil {
			t.Errorf("%s was written although the Txn failed: %+v", k, kv)
		}
	}
}

// Puts racing a Revoke of the same lease must not leave a live key behind: a
// Put that lands after the reaper scanned the lease's keys but before the lease
// row is gone has to be cleaned up too.
func TestLease_PutRacingRevoke_LeavesNoOrphanKeys(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const writers = 6
	for round := 0; round < 3; round++ {
		lease, err := s.Leases().Grant(ctx, 120)
		if err != nil {
			t.Fatalf("grant: %v", err)
		}
		prefix := fmt.Sprintf("/lw/race/%d/", round)

		var wg sync.WaitGroup
		started := make(chan struct{})
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				<-started
				for i := 0; i < 20; i++ {
					key := fmt.Sprintf("%sw%d-%d", prefix, w, i)
					if _, _, err := s.Put(ctx, key, []byte("v"), lease.ID); err != nil {
						if errors.Is(err, store.ErrLeaseNotFound) {
							return // lease is gone: exactly what we want
						}
						t.Errorf("put %s: %v", key, err)
						return
					}
				}
			}(w)
		}

		close(started)
		time.Sleep(30 * time.Millisecond)
		if err := s.Leases().Revoke(ctx, lease.ID); err != nil {
			t.Fatalf("revoke: %v", err)
		}
		wg.Wait()

		_, _, kvs, err := s.List(ctx, prefix, prefix, 0, 0)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		for _, kv := range kvs {
			t.Errorf("round %d: orphan key %s still bound to revoked lease %d", round, kv.Key, kv.LeaseID)
		}
	}
}

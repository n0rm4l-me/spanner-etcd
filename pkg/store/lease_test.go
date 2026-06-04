package store_test

import (
	"context"
	"testing"
	"time"
)

func TestLease_GrantRevoke(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	lease, err := s.Leases().Grant(ctx, 60)
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	if lease.ID == 0 {
		t.Fatal("want non-zero lease ID")
	}
	if lease.TTL != 60 {
		t.Fatalf("want TTL=60, got %d", lease.TTL)
	}

	// Put key with lease.
	s.Create(ctx, "/lease/k", []byte("v"), lease.ID)

	// Revoke removes the key.
	if err := s.Leases().Revoke(ctx, lease.ID); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	_, kv, _ := s.Get(ctx, "/lease/k", 0)
	if kv != nil {
		t.Fatalf("want nil after lease revoke, got %+v", kv)
	}
}

func TestLease_NaturalExpiry(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	lease, _ := s.Leases().Grant(ctx, 2) // 2 second TTL
	s.Create(ctx, "/ttl/key", []byte("expires"), lease.ID)

	_, kv, _ := s.Get(ctx, "/ttl/key", 0)
	if kv == nil {
		t.Fatal("key should exist before expiry")
	}

	t.Log("waiting 3s for natural TTL expiry...")
	time.Sleep(3 * time.Second)

	_, kv, _ = s.Get(ctx, "/ttl/key", 0)
	if kv != nil {
		t.Fatalf("want nil after TTL expiry, got %+v", kv)
	}
}

func TestLease_Keepalive(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	lease, _ := s.Leases().Grant(ctx, 2)
	s.Create(ctx, "/ka/key", []byte("alive"), lease.ID)

	// Keepalive three times over 3 seconds.
	for i := 0; i < 3; i++ {
		time.Sleep(time.Second)
		ttl, err := s.Leases().Keepalive(ctx, lease.ID)
		if err != nil {
			t.Fatalf("keepalive %d: %v", i, err)
		}
		if ttl != 2 {
			t.Fatalf("want TTL=2 after keepalive, got %d", ttl)
		}
	}

	// Key should still be alive.
	_, kv, _ := s.Get(ctx, "/ka/key", 0)
	if kv == nil {
		t.Fatal("key should be alive after keepalive")
	}
}

func TestLease_GetUnknown(t *testing.T) {
	s := newTestStore(t)
	got := s.Leases().Get(99999)
	if got != nil {
		t.Fatalf("want nil for unknown lease, got %+v", got)
	}
}

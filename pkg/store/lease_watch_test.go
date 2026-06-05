package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/n0rm4l-me/spanner-etcd/pkg/store"
)

// TestLeaseWatch_RevokeGeneratesDeleteEvent verifies that revoking a lease
// delivers DELETE Watch events for all keys attached to that lease.
// This is the Kubernetes Service endpoints pattern.
func TestLeaseWatch_RevokeGeneratesDeleteEvent(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	lease, err := s.Leases().Grant(ctx, 60)
	if err != nil {
		t.Fatalf("grant: %v", err)
	}

	s.Create(ctx, "/svc/ep1", []byte("10.0.0.1:8080"), lease.ID)
	s.Create(ctx, "/svc/ep2", []byte("10.0.0.2:8080"), lease.ID)
	time.Sleep(200 * time.Millisecond)

	curRev, _ := s.CurrentRevision(ctx)
	ch := s.Watch(ctx, "/svc/", curRev)
	time.Sleep(300 * time.Millisecond)

	if err := s.Leases().Revoke(ctx, lease.ID); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	got := collectEvents(t, ch, 2, 10*time.Second)
	var deleteCount int
	for _, ev := range got {
		if ev.Type == store.EventDelete {
			deleteCount++
		}
	}
	if deleteCount < 2 {
		t.Fatalf("want 2 DELETE events after lease revoke, got %d (events=%+v)", deleteCount, got)
	}
}

// TestLeaseWatch_ExpiryGeneratesDeleteEvent verifies that natural TTL expiry
// also delivers DELETE Watch events — not just explicit Revoke.
func TestLeaseWatch_ExpiryGeneratesDeleteEvent(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	lease, err := s.Leases().Grant(ctx, 2)
	if err != nil {
		t.Fatalf("grant: %v", err)
	}

	s.Create(ctx, "/ttlwatch/k", []byte("expires"), lease.ID)
	time.Sleep(200 * time.Millisecond)

	curRev, _ := s.CurrentRevision(ctx)
	ch := s.Watch(ctx, "/ttlwatch/", curRev)
	time.Sleep(300 * time.Millisecond)

	t.Log("waiting 4s for natural TTL expiry + poll cycle...")
	time.Sleep(4 * time.Second)

	// Collect up to 3 events (may include the CREATE from replay) and find DELETE.
	got := collectEvents(t, ch, 3, 5*time.Second)
	var found bool
	for _, ev := range got {
		if ev.Type == store.EventDelete && ev.KV.Key == "/ttlwatch/k" {
			found = true
		}
	}
	if !found {
		t.Fatalf("want DELETE event after TTL expiry, got %+v", got)
	}
}

package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/n0rm4l-me/spanner-etcd/pkg/store"
)

// TestGet_ErrCompacted verifies that Get returns ErrCompacted for a revision
// that has been physically deleted by compaction.
func TestGet_ErrCompacted(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev1, err := s.Create(ctx, "/compact/key", []byte("v1"), 0)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	rev2, _, _, err := s.Update(ctx, "/compact/key", []byte("v2"), rev1, 0)
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	// Compact up to rev2 — rev1 should become inaccessible.
	if _, err := s.Compact(ctx, rev2); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// Wait for physical deletion.
	waitCompacted(t, s, ctx, "/compact/key", rev1)

	_, _, err = s.Get(ctx, "/compact/key", rev1)
	if err != store.ErrCompacted {
		t.Fatalf("Get at compacted revision: want ErrCompacted, got %v", err)
	}

	// Current value still readable.
	_, kv, err := s.Get(ctx, "/compact/key", 0)
	if err != nil || kv == nil || string(kv.Value) != "v2" {
		t.Fatalf("Get current: err=%v kv=%v", err, kv)
	}
}

// TestList_ErrCompacted verifies that List returns ErrCompacted for a compacted revision.
func TestList_ErrCompacted(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev1, err := s.Create(ctx, "/clist/a", []byte("v1"), 0)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	rev2, _, _, err := s.Update(ctx, "/clist/a", []byte("v2"), rev1, 0)
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	if _, err := s.Compact(ctx, rev2); err != nil {
		t.Fatalf("compact: %v", err)
	}
	waitCompacted(t, s, ctx, "/clist/a", rev1)

	_, _, _, err = s.List(ctx, "/clist/", "/clist/", 0, rev1)
	if err != store.ErrCompacted {
		t.Fatalf("List at compacted revision: want ErrCompacted, got %v", err)
	}
}

// TestCount_ErrCompacted verifies that Count returns ErrCompacted for a compacted revision.
func TestCount_ErrCompacted(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev1, err := s.Create(ctx, "/ccount/a", []byte("v1"), 0)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	rev2, _, _, err := s.Update(ctx, "/ccount/a", []byte("v2"), rev1, 0)
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	if _, err := s.Compact(ctx, rev2); err != nil {
		t.Fatalf("compact: %v", err)
	}
	waitCompacted(t, s, ctx, "/ccount/a", rev1)

	_, _, err = s.Count(ctx, "/ccount/", "/ccount/", rev1)
	if err != store.ErrCompacted {
		t.Fatalf("Count at compacted revision: want ErrCompacted, got %v", err)
	}
}

// TestAfter_ErrCompacted verifies that After returns ErrCompacted for a compacted afterRev.
func TestAfter_ErrCompacted(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev1, err := s.Create(ctx, "/cafter/a", []byte("v1"), 0)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	rev2, _, _, err := s.Update(ctx, "/cafter/a", []byte("v2"), rev1, 0)
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	if _, err := s.Compact(ctx, rev2); err != nil {
		t.Fatalf("compact: %v", err)
	}
	waitCompacted(t, s, ctx, "/cafter/a", rev1)

	_, _, err = s.After(ctx, "/cafter/", rev1-1, 100)
	if err != store.ErrCompacted {
		t.Fatalf("After at compacted revision: want ErrCompacted, got %v", err)
	}
}

// waitCompacted verifies that Get at the given revision immediately returns
// ErrCompacted — compaction is recorded synchronously so no polling is needed.
func waitCompacted(t *testing.T, s *store.Store, ctx context.Context, key string, rev int64) {
	t.Helper()
	// Give the async compaction goroutine a moment to record the compact revision.
	// The ErrCompacted guard is based on kv_rev, not physical row deletion.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_, _, err := s.Get(ctx, key, rev)
		if err == store.ErrCompacted {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("Get(%q, rev=%d) did not return ErrCompacted within deadline", key, rev)
}

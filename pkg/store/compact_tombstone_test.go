package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/n0rm4l-me/spanner-etcd/pkg/store"
)

// A deleted key leaves a tombstone row. Compaction used to keep it forever, so a
// store with a steady churn of short-lived keys (Kubernetes events, jetcd
// registrations that expire) grew without bound. etcd drops such a key entirely
// once the compaction revision passes its deletion.
//
// Two rules keep that safe, and the tests below pin both:
//   - a tombstone is removed only when it is the key's only remaining row, so
//     removing it can never expose an older value (resurrection);
//   - the newest row of the whole store is never removed, because the current
//     revision is the maximum rev in the table and must never go backwards.

// rowCount is the number of physical rows (values and tombstones) held for key.
func rowCount(t *testing.T, s *store.Store, key string) int {
	t.Helper()
	_, evs, err := s.After(context.Background(), key, 0, 0)
	if err != nil {
		t.Fatalf("after %s: %v", key, err)
	}
	n := 0
	for _, ev := range evs {
		if ev.KV.Key == key {
			n++
		}
	}
	return n
}

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for: %s", d, what)
}

func TestCompact_RemovesDeletedKeyCompletely(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	for _, v := range []string{"v1", "v2"} {
		if _, _, err := s.Put(ctx, "/ct/gone", []byte(v), 0); err != nil {
			t.Fatalf("put: %v", err)
		}
	}
	if _, _, ok, err := s.Delete(ctx, "/ct/gone", 0); err != nil || !ok {
		t.Fatalf("delete: ok=%v err=%v", ok, err)
	}
	zrev, _, err := s.Put(ctx, "/ct/z", []byte("z"), 0) // newer than the tombstone
	if err != nil {
		t.Fatalf("put z: %v", err)
	}
	if n := rowCount(t, s, "/ct/gone"); n != 3 {
		t.Fatalf("before compaction want 3 rows (2 values + tombstone), got %d", n)
	}

	if _, err := s.Compact(ctx, zrev); err != nil {
		t.Fatalf("compact: %v", err)
	}
	waitFor(t, 20*time.Second, "every row of the deleted key to be removed", func() bool {
		return rowCount(t, s, "/ct/gone") == 0
	})

	// It stays deleted (no resurrection), the rest is untouched, and the key can
	// be created again as a brand new generation.
	if _, kv, _ := s.Get(ctx, "/ct/gone", 0); kv != nil {
		t.Fatalf("deleted key came back: %+v", kv)
	}
	if _, kv, _ := s.Get(ctx, "/ct/z", 0); kv == nil || string(kv.Value) != "z" {
		t.Fatalf("unrelated key damaged: %+v", kv)
	}
	rev, prev, err := s.Put(ctx, "/ct/gone", []byte("again"), 0)
	if err != nil || prev != nil {
		t.Fatalf("recreate: prev=%+v err=%v", prev, err)
	}
	_, kv, _ := s.Get(ctx, "/ct/gone", 0)
	if kv == nil || !kv.Created || kv.CreateRevision != rev {
		t.Fatalf("recreated key should start a new generation, got %+v", kv)
	}
}

// If the newest row in the store is a tombstone, removing it would lower
// CurrentRevision (the maximum rev), and clients would see the revision go
// backwards. It has to stay until something newer exists.
func TestCompact_NeverLowersTheCurrentRevision(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if _, _, err := s.Put(ctx, "/cr/k", []byte("v"), 0); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, _, ok, err := s.Delete(ctx, "/cr/k", 0); err != nil || !ok {
		t.Fatalf("delete: ok=%v err=%v", ok, err)
	}
	cur, err := s.CurrentRevision(ctx)
	if err != nil {
		t.Fatalf("current revision: %v", err)
	}

	if _, err := s.Compact(ctx, cur); err != nil {
		t.Fatalf("compact: %v", err)
	}
	// The superseded value goes; the tombstone, being the newest row, stays.
	waitFor(t, 20*time.Second, "the superseded value to be removed", func() bool {
		return rowCount(t, s, "/cr/k") == 1
	})
	time.Sleep(2 * time.Second) // give the tombstone pass the chance to (wrongly) run
	if n := rowCount(t, s, "/cr/k"); n != 1 {
		t.Fatalf("the newest tombstone must stay, rows left: %d", n)
	}
	if got, _ := s.CurrentRevision(ctx); got != cur {
		t.Fatalf("current revision moved from %d to %d", cur, got)
	}

	// Once a newer write exists the old tombstone can go, and the revision only grows.
	zrev, _, err := s.Put(ctx, "/cr/z", []byte("z"), 0)
	if err != nil {
		t.Fatalf("put z: %v", err)
	}
	if _, err := s.Compact(ctx, zrev); err != nil {
		t.Fatalf("compact again: %v", err)
	}
	waitFor(t, 20*time.Second, "the old tombstone to be removed", func() bool {
		return rowCount(t, s, "/cr/k") == 0
	})
	if got, _ := s.CurrentRevision(ctx); got != zrev {
		t.Fatalf("current revision: want %d, got %d", zrev, got)
	}
}

// A tombstone newer than the compaction target is still history a watcher may
// need to replay, so it stays.
func TestCompact_TombstoneNewerThanTargetStays(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if _, _, err := s.Put(ctx, "/nt/k", []byte("v"), 0); err != nil {
		t.Fatalf("put: %v", err)
	}
	target, _, err := s.Put(ctx, "/nt/other", []byte("o"), 0)
	if err != nil {
		t.Fatalf("put other: %v", err)
	}
	if _, _, ok, err := s.Delete(ctx, "/nt/k", 0); err != nil || !ok {
		t.Fatalf("delete: ok=%v err=%v", ok, err)
	}
	if _, _, err := s.Put(ctx, "/nt/newer", []byte("n"), 0); err != nil { // so the tombstone is not the newest row
		t.Fatalf("put newer: %v", err)
	}

	if _, err := s.Compact(ctx, target); err != nil {
		t.Fatalf("compact: %v", err)
	}
	// The value (older than target and superseded) goes; the tombstone is newer
	// than target and stays.
	waitFor(t, 20*time.Second, "the superseded value to be removed", func() bool {
		return rowCount(t, s, "/nt/k") == 1
	})
	time.Sleep(2 * time.Second)
	if n := rowCount(t, s, "/nt/k"); n != 1 {
		t.Fatalf("a tombstone newer than the target must stay, rows left: %d", n)
	}
}

package store_test

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// TestCompact_LargeVolume verifies that compaction correctly handles more rows
// than a single batch (> compactBatchSize), looping until all are deleted.
func TestCompact_LargeVolume(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const n = 2500 // 2.5× the batch size of 1000

	// Write n versions of the same key — each update leaves a stale row.
	rev, err := s.Create(ctx, "/compact/large", []byte("v0"), 0)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	for i := 1; i < n; i++ {
		newRev, _, _, err := s.Update(ctx, "/compact/large", []byte(fmt.Sprintf("v%d", i)), rev, 0)
		if err != nil {
			t.Fatalf("update %d: %v", i, err)
		}
		rev = newRev
	}

	curRev, err := s.CurrentRevision(ctx)
	if err != nil {
		t.Fatalf("current revision: %v", err)
	}
	// targetRev is the revision just before the last write — all prior rows are stale.
	targetRev := rev - 1

	t.Logf("compacting %d stale rows at targetRev=%d (curRev=%d)", n-1, targetRev, curRev)
	start := time.Now()
	if _, err := s.Compact(ctx, targetRev); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// Compact runs async — poll until the historical revision is no longer readable.
	deadline := time.Now().Add(30 * time.Second)
	var compacted bool
	for time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		_, kv, _ := s.Get(ctx, "/compact/large", targetRev)
		if kv == nil {
			compacted = true
			t.Logf("historical revision compacted in %s", time.Since(start).Round(time.Millisecond))
			break
		}
	}
	if !compacted {
		t.Fatal("historical revision still readable after compaction deadline")
	}

	// Current value must still be readable.
	_, kv, err := s.Get(ctx, "/compact/large", 0)
	if err != nil || kv == nil {
		t.Fatalf("current value missing after compaction: err=%v kv=%v", err, kv)
	}
	expected := fmt.Sprintf("v%d", n-1)
	if string(kv.Value) != expected {
		t.Fatalf("want current value=%q, got %q", expected, kv.Value)
	}
	t.Logf("current value intact: %q", kv.Value)
}

// TestCompact_DoesNotDeleteCurrentRevision verifies that compaction never
// deletes the latest revision of a key — only stale historical rows.
func TestCompact_DoesNotDeleteCurrentRevision(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev1, err := s.Create(ctx, "/compact/cur/a", []byte("v1"), 0)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	rev2, _, _, err := s.Update(ctx, "/compact/cur/a", []byte("v2"), rev1, 0)
	if err != nil {
		t.Fatalf("update to v2: %v", err)
	}
	rev3, _, _, err := s.Update(ctx, "/compact/cur/a", []byte("v3"), rev2, 0)
	if err != nil {
		t.Fatalf("update to v3: %v", err)
	}

	// Compact up to rev2 — rev3 (current) must survive.
	if _, err := s.Compact(ctx, rev2); err != nil {
		t.Fatalf("compact: %v", err)
	}

	// Poll until rev1 is gone — this confirms compaction actually ran.
	// Fail explicitly if the deadline passes without compaction completing.
	deadline := time.Now().Add(10 * time.Second)
	var compacted bool
	for time.Now().Before(deadline) {
		time.Sleep(300 * time.Millisecond)
		_, kv, _ := s.Get(ctx, "/compact/cur/a", rev1)
		if kv == nil {
			compacted = true
			break
		}
	}
	if !compacted {
		t.Fatal("rev1 still readable after compaction deadline — compaction did not run")
	}

	_, kv, err := s.Get(ctx, "/compact/cur/a", 0)
	if err != nil || kv == nil {
		t.Fatalf("current value missing after compaction: %v", err)
	}
	if string(kv.Value) != "v3" || kv.Rev != rev3 {
		t.Fatalf("want v3 at rev=%d, got %q rev=%d", rev3, kv.Value, kv.Rev)
	}
}

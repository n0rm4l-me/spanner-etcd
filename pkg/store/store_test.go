package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/n0rm4l-me/spanner-etcd/pkg/store"
)

// ── Create ────────────────────────────────────────────────────────────────────

func TestCreate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev, err := s.Create(ctx, "/a", []byte("hello"), 0)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if rev <= 0 {
		t.Fatalf("want rev > 0, got %d", rev)
	}
}

func TestCreate_KeyExists(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	s.Create(ctx, "/dup", []byte("v1"), 0)
	_, err := s.Create(ctx, "/dup", []byte("v2"), 0)
	if err != store.ErrKeyExists {
		t.Fatalf("want ErrKeyExists, got %v", err)
	}
}

func TestCreate_AfterDelete(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev1, _ := s.Create(ctx, "/recreate", []byte("v1"), 0)
	s.Delete(ctx, "/recreate", rev1)

	time.Sleep(100 * time.Millisecond)
	_, err := s.Create(ctx, "/recreate", []byte("v2"), 0)
	if err != nil {
		t.Fatalf("re-create after delete: %v", err)
	}
}

// ── Get ───────────────────────────────────────────────────────────────────────

func TestGet(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	s.Create(ctx, "/get/key", []byte("value"), 0)
	curRev, kv, err := s.Get(ctx, "/get/key", 0)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if kv == nil {
		t.Fatal("want kv, got nil")
	}
	if string(kv.Value) != "value" {
		t.Fatalf("want value=%q, got %q", "value", kv.Value)
	}
	if curRev <= 0 {
		t.Fatalf("want curRev > 0, got %d", curRev)
	}
}

func TestGet_NotFound(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	_, kv, err := s.Get(ctx, "/nonexistent", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if kv != nil {
		t.Fatalf("want nil kv, got %+v", kv)
	}
}

func TestGet_Historical(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev1, _ := s.Create(ctx, "/hist", []byte("v1"), 0)
	s.Update(ctx, "/hist", []byte("v2"), rev1, 0)

	_, kv, err := s.Get(ctx, "/hist", rev1)
	if err != nil {
		t.Fatalf("historical get: %v", err)
	}
	if kv == nil || string(kv.Value) != "v1" {
		t.Fatalf("want v1 at rev=%d, got %v", rev1, kv)
	}

	_, kv2, _ := s.Get(ctx, "/hist", 0)
	if string(kv2.Value) != "v2" {
		t.Fatalf("want current v2, got %q", kv2.Value)
	}
}

// ── Update ────────────────────────────────────────────────────────────────────

func TestUpdate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev1, _ := s.Create(ctx, "/upd", []byte("before"), 0)
	rev2, prev, ok, err := s.Update(ctx, "/upd", []byte("after"), rev1, 0)

	if err != nil {
		t.Fatalf("update: %v", err)
	}
	if !ok {
		t.Fatal("want ok=true")
	}
	if rev2 <= rev1 {
		t.Fatalf("want rev2 > rev1 (%d > %d)", rev2, rev1)
	}
	if prev == nil || string(prev.Value) != "before" {
		t.Fatalf("want prev=before, got %v", prev)
	}
}

func TestUpdate_RevisionMismatch(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev1, _ := s.Create(ctx, "/cas", []byte("v1"), 0)
	s.Update(ctx, "/cas", []byte("v2"), rev1, 0)

	// Try to update with stale rev.
	_, _, ok, _ := s.Update(ctx, "/cas", []byte("v3"), rev1, 0)
	if ok {
		t.Fatal("want ok=false on stale revision, got true")
	}
}

// ── Delete ────────────────────────────────────────────────────────────────────

func TestDelete(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev1, _ := s.Create(ctx, "/del", []byte("bye"), 0)
	rev2, prev, ok, err := s.Delete(ctx, "/del", rev1)

	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if !ok {
		t.Fatal("want ok=true")
	}
	if rev2 <= rev1 {
		t.Fatalf("want rev2 > rev1, got %d <= %d", rev2, rev1)
	}
	if prev == nil || string(prev.Value) != "bye" {
		t.Fatalf("want prev=bye, got %v", prev)
	}

	time.Sleep(100 * time.Millisecond)
	_, kv, _ := s.Get(ctx, "/del", 0)
	if kv != nil {
		t.Fatalf("want nil after delete, got %+v", kv)
	}
}

func TestDelete_Unconditional(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	s.Create(ctx, "/uncond", []byte("x"), 0)
	_, _, ok, err := s.Delete(ctx, "/uncond", 0) // revision=0 means unconditional
	if err != nil || !ok {
		t.Fatalf("unconditional delete failed: ok=%v err=%v", ok, err)
	}
}

// ── List ──────────────────────────────────────────────────────────────────────

func TestList_Prefix(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	s.Create(ctx, "/list/a", []byte("1"), 0)
	s.Create(ctx, "/list/b", []byte("2"), 0)
	s.Create(ctx, "/list/c", []byte("3"), 0)
	s.Create(ctx, "/other/x", []byte("4"), 0)

	_, _, kvs, err := s.List(ctx, "/list/", "/list/", 0, 0)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(kvs) != 3 {
		t.Fatalf("want 3 keys, got %d", len(kvs))
	}
}

func TestList_Historical(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev1, _ := s.Create(ctx, "/hist/a", []byte("v1"), 0)
	s.Create(ctx, "/hist/b", []byte("v1"), 0)
	s.Create(ctx, "/hist/c", []byte("v1"), 0)

	// At rev1 only /hist/a existed.
	_, _, kvs, err := s.List(ctx, "/hist/", "/hist/", 0, rev1)
	if err != nil {
		t.Fatalf("historical list: %v", err)
	}
	if len(kvs) != 1 || kvs[0].Key != "/hist/a" {
		t.Fatalf("want 1 key (/hist/a) at rev1, got %v", kvs)
	}
}

func TestList_Limit(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		s.Create(ctx, "/lim/"+string(rune('a'+i)), []byte("v"), 0)
	}

	_, _, kvs, err := s.List(ctx, "/lim/", "/lim/", 3, 0)
	if err != nil {
		t.Fatalf("list with limit: %v", err)
	}
	if len(kvs) != 3 {
		t.Fatalf("want 3 keys (limit), got %d", len(kvs))
	}
}

// ── Count ─────────────────────────────────────────────────────────────────────

func TestCount(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	s.Create(ctx, "/cnt/a", []byte("1"), 0)
	s.Create(ctx, "/cnt/b", []byte("2"), 0)

	_, count, err := s.Count(ctx, "/cnt/", "/cnt/", 0)
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 2 {
		t.Fatalf("want count=2, got %d", count)
	}
}

// ── After ─────────────────────────────────────────────────────────────────────

func TestAfter(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	startRev, _ := s.CurrentRevision(ctx)

	s.Create(ctx, "/after/a", []byte("1"), 0)
	s.Create(ctx, "/after/b", []byte("2"), 0)

	_, events, err := s.After(ctx, "/after/", startRev, 100)
	if err != nil {
		t.Fatalf("after: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("want 2 events, got %d", len(events))
	}
	for _, ev := range events {
		if ev.Type != store.EventPut {
			t.Fatalf("want EventPut, got %v", ev.Type)
		}
	}
}

func TestAfter_DeleteEvent(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev1, _ := s.Create(ctx, "/after/del", []byte("v"), 0)
	startRev := rev1
	s.Delete(ctx, "/after/del", rev1)

	_, events, err := s.After(ctx, "/after/", startRev, 100)
	if err != nil {
		t.Fatalf("after: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("want at least 1 event")
	}
	var found bool
	for _, ev := range events {
		if ev.Type == store.EventDelete && ev.KV.Key == "/after/del" {
			found = true
		}
	}
	if !found {
		t.Fatalf("want DELETE event for /after/del in %+v", events)
	}
}

// ── Compact ───────────────────────────────────────────────────────────────────

func TestCompact(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev1, _ := s.Create(ctx, "/compact/k", []byte("v1"), 0)
	s.Update(ctx, "/compact/k", []byte("v2"), rev1, 0)
	cur, _ := s.CurrentRevision(ctx)

	_, err := s.Compact(ctx, cur-1)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}

	// Current value still accessible.
	time.Sleep(500 * time.Millisecond) // allow async GC
	_, kv, _ := s.Get(ctx, "/compact/k", 0)
	if kv == nil || string(kv.Value) != "v2" {
		t.Fatalf("want v2 after compact, got %v", kv)
	}
}

// ── Revision monotonicity ─────────────────────────────────────────────────────

func TestRevisionMonotonicity(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	var last int64
	for i := 0; i < 10; i++ {
		rev, _ := s.Create(ctx, "/mono/"+string(rune('a'+i)), []byte("v"), 0)
		if rev <= last {
			t.Fatalf("revision not monotonic: %d <= %d", rev, last)
		}
		last = rev
	}
}

// ── OldValue (PrevKv) ─────────────────────────────────────────────────────────

func TestOldValue_OnUpdate(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev1, _ := s.Create(ctx, "/old/k", []byte("original"), 0)
	_, _, _, err := s.Update(ctx, "/old/k", []byte("updated"), rev1, 0)
	if err != nil {
		t.Fatalf("update: %v", err)
	}

	// After is used by Watch to deliver PrevKv.
	_, events, _ := s.After(ctx, "/old/", rev1, 10)
	var found bool
	for _, ev := range events {
		if ev.KV.Key == "/old/k" && ev.Type == store.EventPut {
			if string(ev.KV.OldValue) == "original" {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("want OldValue=original in update event, events=%+v", events)
	}
}

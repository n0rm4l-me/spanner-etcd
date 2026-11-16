package store_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestPut_CreatesThenOverwrites(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev1, prev, err := s.Put(ctx, "/put/a", []byte("v1"), 0)
	if err != nil {
		t.Fatalf("put v1: %v", err)
	}
	if prev != nil {
		t.Fatalf("first put: want prev=nil, got %+v", prev)
	}

	rev2, prev, err := s.Put(ctx, "/put/a", []byte("v2"), 0)
	if err != nil {
		t.Fatalf("put v2: %v", err)
	}
	if rev2 <= rev1 {
		t.Fatalf("want rev2 > rev1, got rev1=%d rev2=%d", rev1, rev2)
	}
	if prev == nil || string(prev.Value) != "v1" || prev.Rev != rev1 {
		t.Fatalf("second put: want prev={v1, rev=%d}, got %+v", rev1, prev)
	}

	_, kv, err := s.Get(ctx, "/put/a", 0)
	if err != nil || kv == nil {
		t.Fatalf("get: kv=%v err=%v", kv, err)
	}
	if string(kv.Value) != "v2" || kv.Rev != rev2 {
		t.Errorf("current row: want v2@%d, got %q@%d", rev2, kv.Value, kv.Rev)
	}
	if kv.Created {
		t.Error("overwrite row must not have created=true")
	}
	if kv.CreateRevision != rev1 {
		t.Errorf("CreateRevision: want %d (first write), got %d", rev1, kv.CreateRevision)
	}
	if kv.PrevRevision != rev1 {
		t.Errorf("PrevRevision: want %d, got %d", rev1, kv.PrevRevision)
	}
	if string(kv.OldValue) != "v1" {
		t.Errorf("OldValue: want v1, got %q", kv.OldValue)
	}

	// The first row must still be readable at its own revision and be marked created.
	_, first, err := s.Get(ctx, "/put/a", rev1)
	if err != nil || first == nil {
		t.Fatalf("historical get: kv=%v err=%v", first, err)
	}
	if !first.Created || first.CreateRevision != rev1 || string(first.Value) != "v1" {
		t.Errorf("first row: want created v1 with CreateRevision=%d, got %+v", rev1, first)
	}
}

func TestPut_AfterDelete_StartsNewGeneration(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	rev1, _, err := s.Put(ctx, "/put/regen", []byte("v1"), 0)
	if err != nil {
		t.Fatalf("put v1: %v", err)
	}
	if _, _, ok, err := s.Delete(ctx, "/put/regen", rev1); err != nil || !ok {
		t.Fatalf("delete: ok=%v err=%v", ok, err)
	}

	rev3, prev, err := s.Put(ctx, "/put/regen", []byte("v2"), 0)
	if err != nil {
		t.Fatalf("put after delete: %v", err)
	}
	if prev != nil {
		t.Fatalf("put after delete: want prev=nil (key was deleted), got %+v", prev)
	}

	_, kv, err := s.Get(ctx, "/put/regen", 0)
	if err != nil || kv == nil {
		t.Fatalf("get: kv=%v err=%v", kv, err)
	}
	if !kv.Created || kv.CreateRevision != rev3 {
		t.Errorf("want a new generation (created=true, CreateRevision=%d), got created=%v CreateRevision=%d",
			rev3, kv.Created, kv.CreateRevision)
	}
	if kv.PrevRevision != 0 {
		t.Errorf("PrevRevision: want 0 for a new generation, got %d", kv.PrevRevision)
	}
}

func TestPut_StoresLeaseID(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	if _, _, err := s.Put(ctx, "/put/leased", []byte("v"), 4242); err != nil {
		t.Fatalf("put: %v", err)
	}
	_, kv, err := s.Get(ctx, "/put/leased", 0)
	if err != nil || kv == nil {
		t.Fatalf("get: kv=%v err=%v", kv, err)
	}
	if kv.LeaseID != 4242 {
		t.Errorf("LeaseID: want 4242, got %d", kv.LeaseID)
	}
}

// TestPut_ConcurrentSameKey_OneRowPerPut checks that concurrent Puts to one key
// serialize: every call that returns without error leaves exactly one row, and
// each gets a distinct revision.
func TestPut_ConcurrentSameKey_OneRowPerPut(t *testing.T) {
	s := newTestStore(t)
	ctx := context.Background()

	const (
		workers   = 8
		perWorker = 4
		key       = "/put/concurrent"
	)

	var (
		mu   sync.Mutex
		seen = map[int64]int{}
		wg   sync.WaitGroup
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				rev, _, err := s.Put(ctx, key, []byte(fmt.Sprintf("w%d-%d", w, i)), 0)
				if err != nil {
					t.Errorf("put: %v", err)
					continue
				}
				mu.Lock()
				seen[rev]++
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	want := workers * perWorker
	_, events, err := s.After(ctx, key, 0, 0)
	if err != nil {
		t.Fatalf("after: %v", err)
	}
	if len(events) != want {
		t.Errorf("want %d rows, got %d", want, len(events))
	}
	if len(seen) != want {
		t.Errorf("want %d distinct revisions, got %d", want, len(seen))
	}
}

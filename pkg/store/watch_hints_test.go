package store_test

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Events reported by the Change Stream must never be handed to subscribers
// directly. The Change Stream delivers per partition, so it can repeat events
// and reorder them across partitions; the ordered read of the kv table is the
// only delivery path, and the Change Stream merely tells it to look now.
//
// This test replays the committed events through the Change Stream hook, in
// reverse order and twice, and the subscriber must still see each revision
// exactly once and in increasing order.
func TestWatch_ChangeStreamEventsAreOnlyHints(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := s.Watch(ctx, "/wh/", 0)

	var (
		mu        sync.Mutex
		delivered []int64
	)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case evs := <-ch:
				if len(evs) == 0 {
					return
				}
				mu.Lock()
				for _, ev := range evs {
					delivered = append(delivered, ev.KV.Rev)
				}
				mu.Unlock()
			}
		}
	}()

	for _, k := range []string{"/wh/a", "/wh/b", "/wh/c"} {
		if _, _, err := s.Put(ctx, k, []byte("v"), 0); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	time.Sleep(1500 * time.Millisecond) // let the normal path deliver them

	_, evs, err := s.After(ctx, "/wh/", 0, 0)
	if err != nil || len(evs) != 3 {
		t.Fatalf("after: len=%d err=%v", len(evs), err)
	}
	for i, j := 0, len(evs)-1; i < j; i, j = i+1, j-1 {
		evs[i], evs[j] = evs[j], evs[i]
	}
	s.InjectChangeStreamEvents(evs) // out of order
	s.InjectChangeStreamEvents(evs) // and again
	time.Sleep(1500 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(delivered) != 3 {
		t.Errorf("want exactly 3 deliveries, got %d: %v", len(delivered), delivered)
	}
	for i := 1; i < len(delivered); i++ {
		if delivered[i] <= delivered[i-1] {
			t.Errorf("revisions not strictly increasing at %d: %v", i, delivered)
			break
		}
	}
}

// A Change Stream hint for a write made by another replica must wake the
// watcher: the event is then read from the table and delivered promptly,
// without waiting for the next poll tick.
func TestWatch_ChangeStreamHintWakesPoller(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := s.Watch(ctx, "/ww/", 0)

	rev, _, err := s.Put(ctx, "/ww/k", []byte("v"), 0)
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	_, evs, err := s.After(ctx, "/ww/", 0, 0)
	if err != nil || len(evs) != 1 {
		t.Fatalf("after: len=%d err=%v", len(evs), err)
	}

	select {
	case got := <-ch:
		if len(got) != 1 || got[0].KV.Rev != rev {
			t.Fatalf("want one event at rev %d, got %+v", rev, got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("event was not delivered")
	}

	// Nothing more may arrive just because the same event is reported again.
	s.InjectChangeStreamEvents(evs)
	select {
	case got := <-ch:
		t.Fatalf("duplicate delivery after a hint: %+v", got)
	case <-time.After(1500 * time.Millisecond):
	}
}

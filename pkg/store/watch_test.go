package store_test

import (
	"context"
	"testing"
	"time"

	"github.com/n0rm4l-me/spanner-etcd/pkg/store"
)

// TestWatch_LiveEvents verifies that a Write after Watch() is delivered.
func TestWatch_LiveEvents(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	curRev, _ := s.CurrentRevision(ctx)
	ch := s.Watch(ctx, "/watch/", curRev)

	// Give Watch time to register before writing.
	time.Sleep(200 * time.Millisecond)

	s.Create(ctx, "/watch/a", []byte("v1"), 0)
	s.Create(ctx, "/watch/b", []byte("v2"), 0)

	got := collectEvents(t, ch, 2, 5*time.Second)
	if len(got) < 2 {
		t.Fatalf("want 2 events, got %d", len(got))
	}
}

// TestWatch_DeleteEvent verifies that DELETE events are delivered with correct type.
func TestWatch_DeleteEvent(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rev1, _ := s.Create(ctx, "/wdel/k", []byte("val"), 0)

	// Subscribe from rev1 so we get both CREATE and DELETE events.
	ch := s.Watch(ctx, "/wdel/", rev1)
	time.Sleep(200 * time.Millisecond)

	s.Delete(ctx, "/wdel/k", rev1)

	// Collect up to 3 events and find the DELETE one.
	got := collectEvents(t, ch, 3, 5*time.Second)
	var found bool
	for _, ev := range got {
		if ev.Type == store.EventDelete && ev.KV.Key == "/wdel/k" {
			found = true
		}
	}
	if !found {
		t.Fatalf("want DELETE event for /wdel/k in %+v", got)
	}
}

// TestWatch_Replay verifies that Watch with startRev delivers past events.
func TestWatch_Replay(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rev1, _ := s.Create(ctx, "/replay/a", []byte("r1"), 0)
	s.Create(ctx, "/replay/b", []byte("r2"), 0)

	// Subscribe starting from rev1 — should replay both events.
	ch := s.Watch(ctx, "/replay/", rev1)

	got := collectEvents(t, ch, 2, 5*time.Second)
	if len(got) < 2 {
		t.Fatalf("want 2 replay events, got %d", len(got))
	}
}

// TestWatch_PrefixFilter verifies that only matching-prefix events are delivered.
func TestWatch_PrefixFilter(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	curRev, _ := s.CurrentRevision(ctx)
	ch := s.Watch(ctx, "/prefix/", curRev)
	time.Sleep(200 * time.Millisecond)

	s.Create(ctx, "/other/key", []byte("noise"), 0)
	s.Create(ctx, "/prefix/key", []byte("signal"), 0)

	got := collectEvents(t, ch, 1, 5*time.Second)
	for _, ev := range got {
		if ev.KV.Key == "/other/key" {
			t.Fatalf("got event for /other/key — prefix filter broken")
		}
	}
	var found bool
	for _, ev := range got {
		if ev.KV.Key == "/prefix/key" {
			found = true
		}
	}
	if !found {
		t.Fatalf("want event for /prefix/key, not found in %+v", got)
	}
}

// TestWatch_OldValue verifies that update events carry OldValue for PrevKv.
func TestWatch_OldValue(t *testing.T) {
	s := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rev1, _ := s.Create(ctx, "/prevkv/k", []byte("before"), 0)

	// Subscribe from rev1 to get both CREATE and UPDATE events.
	ch := s.Watch(ctx, "/prevkv/", rev1)
	time.Sleep(200 * time.Millisecond)

	s.Update(ctx, "/prevkv/k", []byte("after"), rev1, 0)

	// Collect up to 3 events and find the UPDATE one (value="after").
	got := collectEvents(t, ch, 3, 5*time.Second)
	var found bool
	for _, ev := range got {
		if ev.Type == store.EventPut && string(ev.KV.Value) == "after" {
			if string(ev.KV.OldValue) != "before" {
				t.Fatalf("want OldValue=before for update event, got %q", ev.KV.OldValue)
			}
			found = true
		}
	}
	if !found {
		t.Fatalf("want update event with value=after, events=%+v", got)
	}
}

// collectEvents drains up to n events from ch within timeout.
func collectEvents(t *testing.T, ch <-chan []*store.Event, n int, timeout time.Duration) []*store.Event {
	t.Helper()
	var result []*store.Event
	deadline := time.After(timeout)
	for len(result) < n {
		select {
		case events, ok := <-ch:
			if !ok {
				return result
			}
			result = append(result, events...)
		case <-deadline:
			return result
		}
	}
	return result
}

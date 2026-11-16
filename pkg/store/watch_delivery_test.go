package store_test

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/n0rm4l-me/spanner-etcd/pkg/store"
)

// TestWatch_LiveEventsDeliveredExactlyOnceInOrder checks the core Watch
// contract that Kubernetes and jetcd rely on: every committed revision reaches a
// live subscriber exactly once, and revisions arrive in increasing order.
//
// Writes come from two replicas sharing one database: the subscriber's own
// replica (a "local" write, which also nudges its poll loop) and another one (a
// "remote" write, seen only through the Change Stream or the poll loop). They
// happen in two bursts, one early and one after the Change Stream has had time
// to become the primary event source, because duplicates can come from the
// overlap of the two delivery paths.
//
// The emulator has no Change Streams, so there it only exercises the poll path
// and runs briefly. Against a real instance (SPANNER_ETCD_TEST_INSTANCE) it
// runs long enough to cover the slow poll interval.
func TestWatch_LiveEventsDeliveredExactlyOnceInOrder(t *testing.T) {
	gap, listen := 2*time.Second, 6*time.Second
	if realInstance() != "" {
		gap, listen = 15*time.Second, 50*time.Second
	}

	db := newTestDB(t)
	cfg := store.StoreConfig{AutoCompactInterval: -1}
	a := openStore(t, db, cfg) // the replica the client watches through
	b := openStore(t, db, cfg) // another replica: its writes are "remote" to a

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := a.Watch(ctx, "/wd/", 0)

	var (
		mu        sync.Mutex
		delivered []int64 // revisions in the order they reached the subscriber
		keyOf     = map[int64]string{}
		torndown  bool
	)
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			select {
			case <-ctx.Done():
				return
			case evs := <-ch:
				mu.Lock()
				if len(evs) == 0 {
					torndown = true
					mu.Unlock()
					return
				}
				for _, ev := range evs {
					delivered = append(delivered, ev.KV.Rev)
					keyOf[ev.KV.Rev] = ev.KV.Key
				}
				mu.Unlock()
			}
		}
	}()

	committed := map[int64]string{} // revision -> key, from the Put results
	burst := func(name string) {
		for i := 0; i < 5; i++ {
			for _, w := range []struct {
				s    *store.Store
				side string
			}{{a, "local"}, {b, "remote"}} {
				key := fmt.Sprintf("/wd/%s/%s-%d", name, w.side, i)
				rev, _, err := w.s.Put(ctx, key, []byte("v"), 0)
				if err != nil {
					t.Fatalf("put %s: %v", key, err)
				}
				committed[rev] = key
				time.Sleep(50 * time.Millisecond)
			}
		}
	}

	burst("early")
	time.Sleep(gap)
	burst("late")
	time.Sleep(listen)
	cancel()
	<-readerDone

	mu.Lock()
	defer mu.Unlock()
	if torndown {
		t.Fatal("subscription was torn down by the store during the test")
	}

	// Exactly once.
	count := map[int64]int{}
	for _, r := range delivered {
		count[r]++
	}
	var dups, missing, unknown []int64
	for r, n := range count {
		if _, ok := committed[r]; !ok {
			unknown = append(unknown, r)
		} else if n > 1 {
			dups = append(dups, r)
		}
	}
	for r := range committed {
		if count[r] == 0 {
			missing = append(missing, r)
		}
	}
	sortInt64s(dups)
	sortInt64s(missing)
	sortInt64s(unknown)
	for _, r := range dups {
		t.Errorf("revision %d (%s) delivered %d times", r, committed[r], count[r])
	}
	for _, r := range missing {
		t.Errorf("revision %d (%s) never delivered", r, committed[r])
	}
	for _, r := range unknown {
		t.Errorf("revision %d (%s) delivered but was never committed by this test", r, keyOf[r])
	}

	// In order.
	for i := 1; i < len(delivered); i++ {
		if delivered[i] <= delivered[i-1] && delivered[i] != delivered[i-1] {
			t.Errorf("out of order at position %d: revision %d (%s) after %d (%s)",
				i, delivered[i], keyOf[delivered[i]], delivered[i-1], keyOf[delivered[i-1]])
		}
	}

	t.Logf("committed=%d deliveries=%d duplicates=%d missing=%d", len(committed), len(delivered), len(dups), len(missing))
}

func sortInt64s(s []int64) { sort.Slice(s, func(i, j int) bool { return s[i] < s[j] }) }

// TestWatch_RemoteWriteLatency measures how long an event takes to reach a
// subscriber when the write was made through ANOTHER replica, which is the
// common case behind a load balancer. Such an event can only be learnt through
// the Change Stream (or the poll timer), so this is the number that shows
// whether the Change Stream hint really wakes the reader.
//
// The poll timer (1s) bounds the latency whatever the Change Stream does, so no
// event may take much longer than that. Against a real instance the Change
// Stream often delivers its hint within tens of milliseconds and sometimes only
// after its 2s heartbeat interval; the timer makes the second case no worse than
// about one second. On the emulator it measures the 1s poll tick itself.
func TestWatch_RemoteWriteLatency(t *testing.T) {
	settle := 2 * time.Second
	if realInstance() != "" {
		settle = 10 * time.Second // let the Change Stream readers start
	}

	db := newTestDB(t)
	cfg := store.StoreConfig{AutoCompactInterval: -1}
	a := openStore(t, db, cfg) // watches
	b := openStore(t, db, cfg) // writes

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch := a.Watch(ctx, "/wl/", 0)
	time.Sleep(settle)

	const n = 5
	var lat []time.Duration
	for i := 0; i < n; i++ {
		t0 := time.Now()
		if _, _, err := b.Put(ctx, fmt.Sprintf("/wl/k%d", i), []byte("v"), 0); err != nil {
			t.Fatalf("put: %v", err)
		}
		select {
		case evs := <-ch:
			if len(evs) == 0 {
				t.Fatal("subscription torn down")
			}
			lat = append(lat, time.Since(t0))
		case <-time.After(60 * time.Second):
			t.Fatalf("event %d from another replica not delivered within 60s", i)
		}
		time.Sleep(300 * time.Millisecond)
	}

	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	t.Logf("remote-write latency over %d events: min=%v median=%v p90=%v max=%v",
		n, lat[0], lat[n/2], lat[n*9/10], lat[n-1])
	if lat[n-1] > 3*time.Second {
		t.Errorf("slowest event took %v: delivery must be bounded by the 1s poll timer", lat[n-1])
	}
}

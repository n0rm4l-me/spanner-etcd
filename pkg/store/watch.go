// Package store — Watch fan-out with Change Stream or polling fallback.
//
// # Event delivery strategy
//
// spanner-etcd supports two event delivery modes:
//
//  1. Change Stream mode (preferred, ~10–50ms latency):
//     A ChangeStreamReader reads all partitions of the kv_changes Change Stream
//     and calls dispatchEvents whenever a DataChangeRecord arrives. This is
//     push-based — Spanner notifies us as soon as a write commits.
//
//  2. Poll mode (fallback, ~1s latency):
//     A background goroutine runs SELECT ... WHERE rev > lastRev every second.
//     Used automatically when Change Streams are unavailable (Spanner emulator,
//     older instances) or on first startup before the stream is established.
//
// At startup the Watcher always begins in poll mode. It concurrently tries to
// start the ChangeStreamReader. If the reader starts successfully, the poll loop
// is kept running in parallel for a short transition window to avoid missing
// events, then the poll loop slows its ticker to 30s (heartbeat-only) once the
// Change Stream has proven healthy for >csHealthyDuration.
//
// The reason for running both simultaneously during transition is that Spanner
// Change Streams have a small start lag (~100ms) — using polls during that window
// ensures no events are dropped.
package store

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/n0rm4l-me/spanner-etcd/pkg/metrics"
)

// closedSentinel is sent on sub.ch when the subscription is being torn down.
// It signals watchLoop to emit a Canceled WatchResponse before returning.
// Using a sentinel avoids sending to a closed channel (which panics).
var closedSentinel = []*Event{}

const (
	pollInterval      = time.Second
	pollSlowInterval  = 30 * time.Second // heartbeat poll when CS is healthy
	pollBatchSize     = int64(500)
	subChannelDepth   = 100
	csHealthyDuration = 10 * time.Second // CS must be healthy this long before we slow polls
)

// subscriber is a single Watch consumer.
type subscriber struct {
	prefix string
	ch     chan []*Event
	cancel context.CancelFunc
	closed atomic.Bool // guards against sending to ch after closedSentinel
}

// Watcher orchestrates event delivery to Watch subscribers.
// It runs a poll loop (always) plus an optional Change Stream reader.
// Both paths call dispatchEvents which routes events to matching subscribers.
type Watcher struct {
	store    *Store
	log      *zap.Logger
	notifyCh chan int64 // woken by local writes

	mu          sync.Mutex
	subscribers []*subscriber
	stopCh      chan struct{}

	// csHealthy is set to 1 once the Change Stream reader has been running
	// without error for csHealthyDuration. The poll ticker slows down at that
	// point to avoid unnecessary Spanner reads.
	csHealthy atomic.Int32
}

func newWatcher(ctx context.Context, store *Store, log *zap.Logger) *Watcher {
	w := &Watcher{
		store:    store,
		log:      log,
		notifyCh: make(chan int64, 1024),
		stopCh:   make(chan struct{}),
	}

	// Always start poll loop — acts as fallback / transition safety net.
	go w.pollLoop(ctx)

	// Attempt to start Change Stream reader.
	// If the Spanner backend doesn't support Change Streams (e.g. emulator),
	// startChangeStream logs a warning and returns silently, leaving the poll
	// loop as the sole event source.
	go w.startChangeStream(ctx)

	return w
}

func (w *Watcher) close() {
	close(w.stopCh)
}

// notify wakes the poll loop with a hint about the latest revision.
// Called by Create/Update/Delete after a successful write.
func (w *Watcher) notify(rev int64) {
	select {
	case w.notifyCh <- rev:
	default:
	}
}

// subscribe returns a channel that delivers events for prefix, starting after afterRev.
// The channel is never closed — teardown is signalled by sending closedSentinel
// (an empty non-nil []*Event slice). Callers must select on both the channel and
// their own context/done signal rather than using for-range semantics.
func (w *Watcher) subscribe(ctx context.Context, prefix string, afterRev int64) <-chan []*Event {
	subCtx, cancel := context.WithCancel(ctx)
	sub := &subscriber{
		prefix: prefix,
		ch:     make(chan []*Event, subChannelDepth),
		cancel: cancel,
	}

	w.mu.Lock()
	w.subscribers = append(w.subscribers, sub)
	w.mu.Unlock()
	metrics.ActiveWatches.Inc()

	// Replay existing events from afterRev before switching to the live feed.
	// Use bgCtx so the query isn't cancelled when the gRPC request context ends.
	go func() {
		defer func() {
			// Mark closed BEFORE removeSub so dispatchEvents stops enqueueing
			// new items immediately — no window between removal and closed flag.
			sub.closed.Store(true)
			w.removeSub(sub)
			// Drain ALL buffered event batches first so the sentinel is the very
			// next item watchLoop receives — it must not be queued behind stale
			// events that should no longer be delivered after cancellation.
			for {
				select {
				case <-sub.ch:
				default:
					goto sendSentinel
				}
			}
		sendSentinel:
			sub.ch <- closedSentinel
		}()

		// afterRev=0 means "live watch from now" — no replay needed.
		// afterRev>0 means "replay from that revision" — include startRev itself
		// by querying After(afterRev-1) since After returns rev > N.
		if afterRev == 0 {
			<-subCtx.Done()
			return
		}

		// Check compaction before starting replay. afterRev-1 can be 0 when
		// afterRev=1 (epoch), which bypasses the afterRev>0 guard in After().
		// Explicitly check the compaction horizon here to avoid silent empty replay.
		if compactRev, cerr := w.store.compactRevision(w.store.bgCtx); cerr != nil {
			w.log.Warn("watch replay: failed to read compact revision, cancelling watch",
				zap.String("prefix", prefix), zap.Error(cerr))
			return
		} else if compactRev > 1 && afterRev <= compactRev {
			w.log.Warn("watch replay: startRevision already compacted",
				zap.Int64("start_rev", afterRev), zap.Int64("compact_rev", compactRev))
			return
		}

		// Paginate replay until we have delivered all historical events.
		// A single batch is limited to pollBatchSize — loop until a partial
		// batch signals there are no more pages.
		replayRev := afterRev - 1
		for {
			_, events, err := w.store.After(w.store.bgCtx, prefix, replayRev, pollBatchSize)
			if err != nil {
				w.log.Warn("watch replay error", zap.String("prefix", prefix), zap.Error(err))
				return
			}
			if len(events) > 0 {
				select {
				case sub.ch <- events:
				case <-subCtx.Done():
					return
				}
				replayRev = events[len(events)-1].KV.Rev
			}
			// Fewer than a full batch means we have caught up.
			if int64(len(events)) < pollBatchSize {
				break
			}
		}
		<-subCtx.Done()
	}()

	return sub.ch
}

func (w *Watcher) removeSub(sub *subscriber) {
	sub.cancel()
	w.mu.Lock()
	defer w.mu.Unlock()
	out := w.subscribers[:0]
	for _, s := range w.subscribers {
		if s != sub {
			out = append(out, s)
		}
	}
	w.subscribers = out
	metrics.ActiveWatches.Dec()
}

// dispatchFromCS is the DispatchFunc passed to the Change Stream reader.
func (w *Watcher) dispatchFromCS(events []*Event) {
	metrics.WatchEventsTotal.WithLabelValues("change_stream").Add(float64(len(events)))
	w.dispatchEvents(events)
}

// dispatchEvents routes a slice of events to all matching subscribers.
// Called from both the poll loop and the Change Stream dispatch function.
func (w *Watcher) dispatchEvents(events []*Event) {
	if len(events) == 0 {
		return
	}

	w.mu.Lock()
	subs := make([]*subscriber, len(w.subscribers))
	copy(subs, w.subscribers)
	w.mu.Unlock()

	for _, sub := range subs {
		prefix := strings.TrimSuffix(sub.prefix, "%")
		var matching []*Event
		for _, ev := range events {
			if prefix == "" || strings.HasPrefix(ev.KV.Key, prefix) {
				matching = append(matching, ev)
			}
		}
		if len(matching) == 0 {
			continue
		}
		// Skip already-closed subscribers (closed.Store happens before sentinel send).
		if sub.closed.Load() {
			continue
		}
		select {
		case sub.ch <- matching:
		default:
			// Channel full — cancel subscription (etcd semantics: client must reconnect).
			// Do NOT close sub.ch here — the channel is never closed; the subscriber
			// goroutine sends closedSentinel when it exits, preventing a closed-channel panic.
			w.log.Warn("subscriber channel full, closing watch",
				zap.String("prefix", sub.prefix))
			metrics.WatchSubscriberDropsTotal.Inc()
			sub.cancel()
		}
	}
}

// ── Poll loop ──────────────────────────────────────────────────────────────────

// pollLoop runs until ctx or stopCh. It slows its ticker once the Change Stream
// is healthy to act only as a heartbeat / safety net.
func (w *Watcher) pollLoop(ctx context.Context) {
	lastRev, _ := w.store.CurrentRevision(ctx)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stopCh:
			return
		case <-ctx.Done():
			return
		case hint := <-w.notifyCh:
			// Local write notification — poll immediately regardless of CS status.
			if hint > lastRev {
				lastRev = w.doPoll(ctx, lastRev)
			}
		case <-ticker.C:
			if w.csHealthy.Load() == 1 {
				// Change Stream is healthy — slow the ticker and skip redundant polls.
				ticker.Reset(pollSlowInterval)
			} else {
				ticker.Reset(pollInterval)
			}
			lastRev = w.doPoll(ctx, lastRev)
		}
	}
}

// doPoll fetches events after lastRev and dispatches them.
func (w *Watcher) doPoll(ctx context.Context, lastRev int64) int64 {
	curRev, events, err := w.store.After(ctx, "", lastRev, pollBatchSize)
	if err != nil {
		// ErrCompacted means lastRev was compacted away. After() already returns
		// curRev even on error, so use it directly — no extra Spanner round-trip.
		if err == ErrCompacted {
			w.log.Warn("poll: revision compacted, advancing to current",
				zap.Int64("old_rev", lastRev), zap.Int64("new_rev", curRev))
			return curRev
		}
		w.log.Warn("poll error", zap.Error(err))
		return lastRev
	}
	if len(events) > 0 {
		metrics.WatchEventsTotal.WithLabelValues("poll").Add(float64(len(events)))
		w.dispatchEvents(events)
	}
	return curRev
}

// ── Change Stream ──────────────────────────────────────────────────────────────

// startChangeStream starts the ChangeStreamReader. If the backend does not
// support Change Streams (emulator, missing DDL) it logs and returns silently,
// leaving the poll loop as the only event source.
func (w *Watcher) startChangeStream(ctx context.Context) {
	// Unique replica ID derived from the store pointer — stable within a process.
	replicaID := fmt.Sprintf("replica-%p", w.store)

	reader := NewChangeStreamReader(
		w.store.client,
		replicaID,
		w.dispatchFromCS, // metrics-instrumented dispatch
		w.log,
	)

	// Monitor CS health: if it has been running for csHealthyDuration without
	// error, mark it healthy so the poll loop slows down.
	go func() {
		select {
		case <-time.After(csHealthyDuration):
			w.csHealthy.Store(1)
			metrics.CSMode.Set(1)
			w.log.Info("change stream healthy, slowing poll loop")
		case <-ctx.Done():
		case <-w.stopCh:
		}
	}()

	if err := reader.Start(ctx); err != nil {
		w.log.Warn("change stream unavailable, using poll fallback", zap.Error(err))
		w.csHealthy.Store(0)
		metrics.CSMode.Set(0)
	}
}

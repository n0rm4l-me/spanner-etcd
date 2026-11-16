// Package store: Watch fan-out, one ordered read path woken by Change Streams.
//
// # Event delivery strategy
//
// Every event a subscriber receives comes from a single source: the poll loop,
// which reads the kv table in revision order (SELECT ... WHERE rev > lastRev
// ORDER BY rev) and advances lastRev to the last revision it delivered. A
// strong Spanner read sees every commit up to its read timestamp, so this path
// is gap-free, duplicate-free and ordered by construction.
//
// What changes is only how quickly the loop is told to look:
//
//   - A local write wakes it immediately (notify).
//   - The Change Stream wakes it when any replica's write commits. The reader's
//     events are NOT delivered to subscribers; only the newest revision in them
//     is used as a "something happened" hint. Measured against a real instance
//     the hint arrives either within ~15 ms of the commit or about one heartbeat
//     interval (2 s) later, and which of the two you get is not predictable.
//   - A timer wakes it every second. It is the guaranteed upper bound on
//     delivery latency, the safety net for lost hints, and the only mechanism on
//     the emulator, which has no Change Streams. It never slows down: a slower
//     timer would turn a slow Change Stream into slow watchers.
//
// Delivering Change Stream events directly would add a second path: they arrive
// per partition (so they can be repeated and reordered across partitions) and
// would overlap with the poll path, giving subscribers duplicates and
// out-of-order events.
package store

import (
	"context"
	"fmt"
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
	pollInterval    = time.Second
	pollBatchSize   = int64(500)
	subChannelDepth = 100
	// csStaleAfter is how long the Change Stream may be silent before it is
	// reported as inactive. A healthy stream produces a record at least every
	// heartbeatInterval (2s) per partition.
	csStaleAfter = 10 * time.Second
)

// subscriber is a single Watch consumer.
type subscriber struct {
	start  string // inclusive
	end    string // exclusive; "" = unbounded
	ch     chan []*Event
	cancel context.CancelFunc
	closed atomic.Bool // guards against sending to ch after closedSentinel
}

// wakeup tells the poll loop that revision rev (or a later one) has committed.
type wakeup struct {
	rev    int64
	source string // metrics label of the events this wakeup leads to: "poll" or "change_stream"
}

// Watcher orchestrates event delivery to Watch subscribers.
// The poll loop is the only producer of events; local writes and the optional
// Change Stream reader merely wake it (see the package comment).
type Watcher struct {
	store    *Store
	log      *zap.Logger
	notifyCh chan wakeup // woken by local writes and Change Stream hints

	mu          sync.Mutex
	subscribers []*subscriber
	stopCh      chan struct{}

	// csActivity is when (UnixNano) the Change Stream last delivered a record,
	// 0 if it never did. It only drives the change_stream_active metric; it must
	// not influence delivery, which the poll timer bounds on its own.
	csActivity atomic.Int64
}

// csFresh reports whether the Change Stream has delivered a record recently.
func (w *Watcher) csFresh() bool {
	last := w.csActivity.Load()
	return last != 0 && time.Since(time.Unix(0, last)) < csStaleAfter
}

func newWatcher(ctx context.Context, store *Store, log *zap.Logger) *Watcher {
	w := &Watcher{
		store:    store,
		log:      log,
		notifyCh: make(chan wakeup, 1024),
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
// Called by Create/Put/Update/Delete after a successful local write.
func (w *Watcher) notify(rev int64) {
	w.wake(wakeup{rev: rev, source: "poll"})
}

// wake queues a wakeup without blocking. If the queue is full the loop is
// already busy and its next read covers everything committed before it starts,
// so a dropped wakeup cannot delay delivery.
func (w *Watcher) wake(u wakeup) {
	select {
	case w.notifyCh <- u:
	default:
	}
}

// subscribe returns a channel that delivers events for keys in [start, end),
// starting after afterRev.
// The channel is never closed — teardown is signalled by sending closedSentinel
// (an empty non-nil []*Event slice). Callers must select on both the channel and
// their own context/done signal rather than using for-range semantics.
func (w *Watcher) subscribe(ctx context.Context, start, end string, afterRev int64) <-chan []*Event {
	subCtx, cancel := context.WithCancel(ctx)
	sub := &subscriber{
		start:  start,
		end:    end,
		ch:     make(chan []*Event, subChannelDepth),
		cancel: cancel,
	}

	w.mu.Lock()
	w.subscribers = append(w.subscribers, sub)
	w.mu.Unlock()
	metrics.ActiveWatches.Inc()

	// Replay existing events from afterRev before switching to the live feed.
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
			// The channel was just drained and closed=true prevents new sends,
			// so the sentinel should fit immediately. Use a short timeout as a
			// safety net against any race rather than blocking indefinitely.
			select {
			case sub.ch <- closedSentinel:
			case <-time.After(100 * time.Millisecond):
			}
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
		if compactRev, cerr := w.store.compactRevision(subCtx); cerr != nil {
			w.log.Warn("watch replay: failed to read compact revision, cancelling watch",
				zap.String("start", start), zap.String("end", end), zap.Error(cerr))
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
			_, events, err := w.store.AfterRange(subCtx, start, end, replayRev, pollBatchSize)
			if err != nil {
				w.log.Warn("watch replay error", zap.String("start", start), zap.String("end", end), zap.Error(err))
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
//
// It does not deliver the events. Change Stream records arrive per partition and
// can repeat or be reordered, so handing them to subscribers would duplicate and
// reorder what the poll loop delivers. The newest revision in the batch is only
// a hint that the poll loop should read the table now.
func (w *Watcher) dispatchFromCS(events []*Event) {
	var maxRev int64
	for _, ev := range events {
		if ev.KV != nil && ev.KV.Rev > maxRev {
			maxRev = ev.KV.Rev
		}
	}
	w.log.Debug("change stream hint", zap.Int64("rev", maxRev), zap.Int("events", len(events)))
	if maxRev == 0 {
		return
	}
	w.wake(wakeup{rev: maxRev, source: "change_stream"})
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
		var matching []*Event
		for _, ev := range events {
			if InRange(ev.KV.Key, sub.start, sub.end) {
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
				zap.String("start", sub.start), zap.String("end", sub.end))
			metrics.WatchSubscriberDropsTotal.Inc()
			sub.cancel()
		}
	}
}

// ── Poll loop ──────────────────────────────────────────────────────────────────

// pollLoop runs until ctx or stopCh. Its ticker stays at pollInterval whatever
// the Change Stream does, so delivery latency is bounded by one tick.
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
		case u := <-w.notifyCh:
			// A local write or a Change Stream hint: read right away. Hints that
			// are not ahead of what was already delivered cost nothing.
			if u.rev > lastRev {
				lastRev = w.doPoll(ctx, lastRev, u.source)
			} else {
				w.log.Debug("wakeup ignored: not ahead of last delivered revision",
					zap.String("source", u.source), zap.Int64("hint_rev", u.rev), zap.Int64("last_rev", lastRev))
			}
		case <-ticker.C:
			// Safety net: runs even if no hint arrived, so a lost hint, a slow
			// or an unavailable Change Stream only delays delivery by one tick.
			if w.csFresh() {
				metrics.CSMode.Set(1)
			} else {
				metrics.CSMode.Set(0)
			}
			lastRev = w.doPoll(ctx, lastRev, "poll")
		}
	}
}

// doPoll delivers every event with a revision above lastRev, in revision order,
// and returns the revision of the last one delivered (lastRev if there were
// none). It is the only place events are handed to subscribers.
//
// lastRev advances to the last delivered event, never to a separately read
// "current revision": that read is a different snapshot from the events query,
// and jumping to it would re-deliver any event that committed in between.
func (w *Watcher) doPoll(ctx context.Context, lastRev int64, source string) int64 {
	for {
		curRev, events, err := w.store.After(ctx, "", lastRev, pollBatchSize)
		if err != nil {
			// ErrCompacted means lastRev was compacted away. After() already returns
			// curRev even on error, so use it directly: no extra Spanner round-trip.
			if err == ErrCompacted {
				w.log.Warn("poll: revision compacted, advancing to current",
					zap.Int64("old_rev", lastRev), zap.Int64("new_rev", curRev))
				return curRev
			}
			w.log.Warn("poll error", zap.Error(err))
			return lastRev
		}
		if len(events) == 0 {
			return lastRev
		}

		// A full batch means there may be more. Drop the trailing revision from
		// it: a transaction that mutates several keys gives them all one
		// revision, and the next read starts strictly after lastRev, so ending
		// a batch inside such a group would lose the rest of the group. It is
		// read again, whole, by the next iteration.
		full := int64(len(events)) == pollBatchSize
		if full {
			last := events[len(events)-1].KV.Rev
			cut := len(events)
			for cut > 0 && events[cut-1].KV.Rev == last {
				cut--
			}
			if cut > 0 {
				events = events[:cut]
			}
		}

		metrics.WatchEventsTotal.WithLabelValues(source).Add(float64(len(events)))
		w.dispatchEvents(events)
		lastRev = events[len(events)-1].KV.Rev
		w.log.Debug("poll delivered", zap.String("source", source),
			zap.Int("events", len(events)), zap.Int64("last_rev", lastRev))
		if !full {
			return lastRev
		}
	}
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
		w.dispatchFromCS, // turns events into poll-loop wakeups
		w.log,
	)
	// The reader reports every record it reads, which feeds the
	// change_stream_active metric (see csFresh).
	reader.activity = &w.csActivity

	if err := reader.Start(ctx); err != nil {
		w.log.Warn("change stream unavailable, using poll fallback", zap.Error(err))
	}
}

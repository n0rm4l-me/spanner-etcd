package store

import (
	"context"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

const (
	pollInterval    = time.Second
	pollBatchSize   = int64(500)
	subChannelDepth = 100
)

// subscriber is a single Watch consumer.
type subscriber struct {
	prefix string
	ch     chan []*Event
	cancel context.CancelFunc
}

// Watcher runs a single background poll loop and fans events out to subscribers.
// Multiple spanner-etcd replicas can run concurrently: each has its own Watcher
// polling the same Spanner table, so Watch events are delivered independently
// per replica — which is correct for a stateless horizontally-scaled deployment.
type Watcher struct {
	store  *Store
	log    *zap.Logger
	notifyCh chan int64 // woken by local writes with their new revision

	mu          sync.Mutex
	subscribers []*subscriber
	running     bool
	stopCh      chan struct{}
}

func newWatcher(ctx context.Context, store *Store, log *zap.Logger) *Watcher {
	w := &Watcher{
		store:  store,
		log:    log,
		notifyCh: make(chan int64, 1024),
		stopCh: make(chan struct{}),
	}
	go w.pollLoop(ctx)
	return w
}

func (w *Watcher) close() {
	close(w.stopCh)
}

// notify wakes the poll loop with a hint about the latest revision.
func (w *Watcher) notify(rev int64) {
	select {
	case w.notifyCh <- rev:
	default:
	}
}

// subscribe returns a channel that delivers events for prefix, starting after afterRev.
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

	// Replay existing events from afterRev before switching to live feed.
	// Use bgCtx (not subCtx) for the replay query so it isn't cancelled
	// when the gRPC request context times out between subscribe and first poll.
	go func() {
		defer func() {
			w.removeSub(sub)
			close(sub.ch)
		}()

		cur, events, err := w.store.After(w.store.bgCtx, prefix, afterRev, pollBatchSize)
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
		}
		// From here the poll loop will deliver further events via sub.ch.
		_ = cur
		<-subCtx.Done()
	}()

	return sub.ch
}

func (w *Watcher) removeSub(sub *subscriber) {
	sub.cancel()
	w.mu.Lock()
	defer w.mu.Unlock()
	subs := w.subscribers[:0]
	for _, s := range w.subscribers {
		if s != sub {
			subs = append(subs, s)
		}
	}
	w.subscribers = subs
}

// pollLoop is the single background goroutine that polls for new events.
// Design: one poll per replica, scales horizontally because Spanner handles
// concurrent readers without coordination.
func (w *Watcher) pollLoop(ctx context.Context) {
	// Start from current revision to avoid replaying history at startup.
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
			if hint > lastRev {
				lastRev = w.poll(ctx, lastRev)
			}
		case <-ticker.C:
			lastRev = w.poll(ctx, lastRev)
		}
	}
}

// poll fetches all events after lastRev and delivers them to matching subscribers.
// Returns the new lastRev.
func (w *Watcher) poll(ctx context.Context, lastRev int64) int64 {
	curRev, events, err := w.store.After(ctx, "", lastRev, pollBatchSize)
	if err != nil {
		w.log.Warn("poll error", zap.Error(err))
		return lastRev
	}
	if len(events) == 0 {
		return curRev
	}

	w.mu.Lock()
	subs := make([]*subscriber, len(w.subscribers))
	copy(subs, w.subscribers)
	w.mu.Unlock()

	for _, sub := range subs {
		var matching []*Event
		for _, ev := range events {
			if strings.HasPrefix(ev.KV.Key, strings.TrimSuffix(sub.prefix, "%")) {
				matching = append(matching, ev)
			}
		}
		if len(matching) == 0 {
			continue
		}
		select {
		case sub.ch <- matching:
		default:
			// Subscriber channel full — drop and close (etcd semantics: client must reconnect).
			w.log.Warn("subscriber channel full, dropping", zap.String("prefix", sub.prefix))
			sub.cancel()
		}
	}

	return curRev
}

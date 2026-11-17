package store

import (
	"context"
	"time"
)

// InjectChangeStreamEvents feeds events to the Watcher exactly as the Change
// Stream reader does, so tests can exercise that path deterministically
// instead of waiting for Spanner to produce the records.
func (s *Store) InjectChangeStreamEvents(events []*Event) {
	s.watcher.dispatchFromCS(events)
}

// TryLockCompaction exposes the compaction lease to tests.
func (s *Store) TryLockCompaction(ctx context.Context, ttl time.Duration) (bool, error) {
	return s.tryLockCompaction(ctx, ttl)
}

// CleanupCursors exposes the stale-cursor cleanup to tests.
func (s *Store) CleanupCursors(ctx context.Context) (int64, error) {
	return s.cleanupCursors(ctx)
}

// ChangeStreamFresh reports whether the Change Stream has delivered a record
// (a heartbeat counts) recently, which is what the change_stream_active metric
// shows.
func (s *Store) ChangeStreamFresh() bool {
	return s.watcher.csFresh()
}

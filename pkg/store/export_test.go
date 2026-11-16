package store

import (
	"context"
	"time"
)

// InjectChangeStreamEvents feeds events to the Watcher exactly as the Change
// Stream reader does. The emulator has no Change Streams, so tests use this to
// exercise that path deterministically.
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

package store

// InjectChangeStreamEvents feeds events to the Watcher exactly as the Change
// Stream reader does. The emulator has no Change Streams, so tests use this to
// exercise that path deterministically.
func (s *Store) InjectChangeStreamEvents(events []*Event) {
	s.watcher.dispatchFromCS(events)
}

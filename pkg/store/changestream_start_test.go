package store_test

import (
	"testing"
	"time"
)

// A replica that starts right after the schema was created used to give up on
// the Change Stream for good. The reader asked for records from one second
// before "now", which on a fresh database is before the stream existed, and
// Spanner answers that with OutOfRange ("start_timestamp is too far in the
// past"). The failure was logged once and never retried, so the replica served
// Watch from the 1 s poll loop until its pod was restarted.
//
// newTestDB creates the stream a few milliseconds before openStore starts the
// watcher, which is exactly the first start on an empty database.
func TestChangeStream_AttachesWhenStartedOnAFreshDatabase(t *testing.T) {
	s := newTestStore(t)

	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if s.ChangeStreamFresh() {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatal("the Change Stream never started delivering: the reader gave up after its first failure and does not retry")
}

package store_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/spanner"

	"github.com/n0rm4l-me/spanner-etcd/pkg/store"
)

// Every replica runs the background compactor, and each round scans the whole
// table. The compaction lease makes it one round per interval for the cluster.
func TestCompactionLease_OneHolderAtATime(t *testing.T) {
	db := newTestDB(t)
	off := store.StoreConfig{AutoCompactInterval: -1}
	a := openStore(t, db, off)
	b := openStore(t, db, off)
	ctx := context.Background()

	if ok, err := a.TryLockCompaction(ctx, 2*time.Second); err != nil || !ok {
		t.Fatalf("first replica: want the lease, got ok=%v err=%v", ok, err)
	}
	if ok, err := b.TryLockCompaction(ctx, 2*time.Second); err != nil || ok {
		t.Fatalf("second replica must not get a held lease: ok=%v err=%v", ok, err)
	}

	time.Sleep(2500 * time.Millisecond) // the lease runs out on its own
	if ok, err := b.TryLockCompaction(ctx, 2*time.Second); err != nil || !ok {
		t.Fatalf("after expiry the other replica should get it: ok=%v err=%v", ok, err)
	}
	if ok, _ := a.TryLockCompaction(ctx, 2*time.Second); ok {
		t.Fatal("the first replica must now be locked out")
	}
}

// Replicas that tick at the same moment: exactly one wins.
func TestCompactionLease_ConcurrentAttemptsHaveOneWinner(t *testing.T) {
	db := newTestDB(t)
	off := store.StoreConfig{AutoCompactInterval: -1}
	const replicas = 4
	stores := make([]*store.Store, replicas)
	for i := range stores {
		stores[i] = openStore(t, db, off)
	}

	var wins atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for _, s := range stores {
		wg.Add(1)
		go func(s *store.Store) {
			defer wg.Done()
			<-start
			if ok, err := s.TryLockCompaction(context.Background(), time.Minute); err != nil {
				t.Errorf("lock: %v", err)
			} else if ok {
				wins.Add(1)
			}
		}(s)
	}
	close(start)
	wg.Wait()
	if got := wins.Load(); got != 1 {
		t.Fatalf("want exactly one winner among %d replicas, got %d", replicas, got)
	}
}

// Cursors written by replicas that no longer exist must not pile up.
func TestCleanupCursors_RemovesOnlyAbandonedOnes(t *testing.T) {
	db := newTestDB(t)
	s := openStore(t, db, store.StoreConfig{AutoCompactInterval: -1})
	ctx := context.Background()

	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		t.Fatalf("spanner client: %v", err)
	}
	defer client.Close()
	cols := []string{"replica_id", "partition_token", "resume_timestamp", "updated_at"}
	if _, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.InsertOrUpdate("kv_cs_cursors", cols, []interface{}{"dead-replica", "tok", time.Now().Add(-72 * time.Hour), time.Now().Add(-72 * time.Hour)}),
		spanner.InsertOrUpdate("kv_cs_cursors", cols, []interface{}{"live-replica", "tok", time.Now(), spanner.CommitTimestamp}),
	}); err != nil {
		t.Fatalf("insert cursors: %v", err)
	}

	n, err := s.CleanupCursors(ctx)
	if err != nil || n != 1 {
		t.Fatalf("want exactly 1 abandoned cursor removed, got n=%d err=%v", n, err)
	}
	if _, err := client.Single().ReadRow(ctx, "kv_cs_cursors", spanner.Key{"live-replica", "tok"}, []string{"replica_id"}); err != nil {
		t.Fatalf("the live cursor must stay: %v", err)
	}
	if _, err := client.Single().ReadRow(ctx, "kv_cs_cursors", spanner.Key{"dead-replica", "tok"}, []string{"replica_id"}); err == nil {
		t.Fatal("the abandoned cursor should be gone")
	}
}

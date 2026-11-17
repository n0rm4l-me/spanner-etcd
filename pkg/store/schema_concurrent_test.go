package store_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/n0rm4l-me/spanner-etcd/pkg/schema"
	"github.com/n0rm4l-me/spanner-etcd/pkg/store"
)

// Replicas of a new deployment start together. Each one applies the schema, and
// Spanner rejects a schema change while another is in progress
// (FailedPrecondition "a concurrent schema change operation ... is already in
// progress"). The loser used to exit with a fatal error and relied on its pod
// being restarted. Every replica must come up, and the schema must be complete.
func TestSchemaEnsure_ReplicasStartingTogetherOnAnEmptyDatabase(t *testing.T) {
	dbPath, admin := newEmptyTestDB(t)

	const replicas = 4
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	errs := make([]error, replicas)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < replicas; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = schema.Ensure(ctx, admin, dbPath, zap.NewNop())
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("replica %d: %v", i, err)
		}
	}
	if t.Failed() {
		return
	}

	// The schema is complete and usable.
	s := openStore(t, dbPath, store.StoreConfig{})
	if _, _, err := s.Put(ctx, "/schema/k", []byte("v"), 0); err != nil {
		t.Fatalf("put on the new schema: %v", err)
	}
}

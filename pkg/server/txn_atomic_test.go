package server_test

import (
	"context"
	"sync"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestTxn_Atomic_ConcurrentCreate verifies that two concurrent
// Txn{Compare(version=0), Then[Put]} calls on the same key cannot both
// succeed — the loser must get Succeeded=false.
// This is the Kubernetes leader election pattern.
func TestTxn_Atomic_ConcurrentCreate(t *testing.T) {
	cli := testServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const key = "/atomic/leader"
	const iterations = 20
	var wins int
	var mu sync.Mutex

	for i := 0; i < iterations; i++ {
		// Reset: delete the key.
		if _, err := cli.Delete(ctx, key); err != nil {
			t.Fatalf("iteration %d: delete: %v", i, err)
		}
		time.Sleep(50 * time.Millisecond)

		var wg sync.WaitGroup
		results := make([]bool, 2)
		errs := make([]error, 2)

		for j := 0; j < 2; j++ {
			j := j
			wg.Add(1)
			go func() {
				defer wg.Done()
				txn, err := cli.Txn(ctx).
					If(clientv3.Compare(clientv3.Version(key), "=", 0)).
					Then(clientv3.OpPut(key, "value")).
					Commit()
				errs[j] = err
				if err == nil {
					results[j] = txn.Succeeded
				}
			}()
		}
		wg.Wait()

		for j, e := range errs {
			if e != nil {
				t.Fatalf("iteration %d goroutine %d: txn error: %v", i, j, e)
			}
		}

		successCount := 0
		for _, r := range results {
			if r {
				successCount++
			}
		}

		mu.Lock()
		if successCount == 1 {
			wins++
		} else if successCount == 2 {
			t.Errorf("iteration %d: both concurrent Txn calls succeeded — atomicity broken", i)
		}
		mu.Unlock()
	}
	t.Logf("ran %d iterations, %d clean races detected", iterations, wins)
}

// TestTxn_Atomic_CompareAndSwap verifies that a CAS Txn is atomic:
// if the compare sees modRev=N, the write must commit only if modRev is still N.
func TestTxn_Atomic_CompareAndSwap(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	// Create key.
	if _, err := cli.Put(ctx, "/atomic/cas", "v1"); err != nil {
		t.Fatalf("put: %v", err)
	}
	gr, _ := cli.Get(ctx, "/atomic/cas")
	modRev := gr.Kvs[0].ModRevision

	// Successful CAS.
	txn, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision("/atomic/cas"), "=", modRev)).
		Then(clientv3.OpPut("/atomic/cas", "v2")).
		Commit()
	if err != nil || !txn.Succeeded {
		t.Fatalf("CAS should succeed: err=%v succeeded=%v", err, txn.Succeeded)
	}

	// Stale CAS must fail — modRev is now stale.
	txn2, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision("/atomic/cas"), "=", modRev)).
		Then(clientv3.OpPut("/atomic/cas", "should-not-write")).
		Commit()
	if err != nil {
		t.Fatalf("stale CAS err: %v", err)
	}
	if txn2.Succeeded {
		t.Fatal("stale CAS should not succeed — atomicity broken")
	}

	// Value must be v2, not "should-not-write".
	time.Sleep(200 * time.Millisecond)
	resp, _ := cli.Get(ctx, "/atomic/cas")
	if string(resp.Kvs[0].Value) != "v2" {
		t.Fatalf("want v2, got %q", resp.Kvs[0].Value)
	}
}

// TestTxn_Atomic_MultiKey verifies that a multi-key atomic Txn either
// commits all ops or none — no partial writes.
func TestTxn_Atomic_MultiKey(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	cli.Put(ctx, "/atomic/mk/a", "a1")
	cli.Put(ctx, "/atomic/mk/b", "b1")
	grA, _ := cli.Get(ctx, "/atomic/mk/a")
	grB, _ := cli.Get(ctx, "/atomic/mk/b")
	revA := grA.Kvs[0].ModRevision
	revB := grB.Kvs[0].ModRevision

	// Atomic update of both keys.
	txn, err := cli.Txn(ctx).
		If(
			clientv3.Compare(clientv3.ModRevision("/atomic/mk/a"), "=", revA),
			clientv3.Compare(clientv3.ModRevision("/atomic/mk/b"), "=", revB),
		).
		Then(
			clientv3.OpPut("/atomic/mk/a", "a2"),
			clientv3.OpPut("/atomic/mk/b", "b2"),
		).
		Commit()
	if err != nil || !txn.Succeeded {
		t.Fatalf("multi-key txn: err=%v succeeded=%v", err, txn.Succeeded)
	}

	time.Sleep(200 * time.Millisecond)
	rA, _ := cli.Get(ctx, "/atomic/mk/a")
	rB, _ := cli.Get(ctx, "/atomic/mk/b")
	if string(rA.Kvs[0].Value) != "a2" || string(rB.Kvs[0].Value) != "b2" {
		t.Fatalf("want a2/b2, got %q/%q", rA.Kvs[0].Value, rB.Kvs[0].Value)
	}
	// Both keys must have the same ModRevision (committed in one txn).
	if rA.Kvs[0].ModRevision != rB.Kvs[0].ModRevision {
		t.Fatalf("keys have different ModRevision: a=%d b=%d — not atomic",
			rA.Kvs[0].ModRevision, rB.Kvs[0].ModRevision)
	}
}

// TestTxn_Atomic_ElseOp verifies the Else branch executes when compare fails,
// and the whole txn is atomic.
func TestTxn_Atomic_ElseOp(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	cli.Put(ctx, "/atomic/else/k", "exists")

	// Compare will fail (version != 0 — key exists).
	txn, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version("/atomic/else/k"), "=", 0)).
		Then(clientv3.OpPut("/atomic/else/k", "from-then")).
		Else(clientv3.OpPut("/atomic/else/k", "from-else")).
		Commit()
	if err != nil {
		t.Fatalf("txn: %v", err)
	}
	if txn.Succeeded {
		t.Fatal("compare should have failed")
	}

	time.Sleep(200 * time.Millisecond)
	resp, _ := cli.Get(ctx, "/atomic/else/k")
	if string(resp.Kvs[0].Value) != "from-else" {
		t.Fatalf("want from-else, got %q", resp.Kvs[0].Value)
	}
}

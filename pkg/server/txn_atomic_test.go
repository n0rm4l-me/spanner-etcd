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

// TestTxn_Atomic_CompareAndSwap verifies that a CAS Txn is atomic.
func TestTxn_Atomic_CompareAndSwap(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	if _, err := cli.Put(ctx, "/atomic/cas", "v1"); err != nil {
		t.Fatalf("put: %v", err)
	}
	gr, err := cli.Get(ctx, "/atomic/cas")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(gr.Kvs) == 0 {
		t.Fatal("key not found after put")
	}
	modRev := gr.Kvs[0].ModRevision

	txn, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision("/atomic/cas"), "=", modRev)).
		Then(clientv3.OpPut("/atomic/cas", "v2")).
		Commit()
	if err != nil || !txn.Succeeded {
		t.Fatalf("CAS should succeed: err=%v succeeded=%v", err, txn.Succeeded)
	}

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

	time.Sleep(200 * time.Millisecond)
	resp, err := cli.Get(ctx, "/atomic/cas")
	if err != nil {
		t.Fatalf("get after CAS: %v", err)
	}
	if len(resp.Kvs) == 0 {
		t.Fatal("key missing after CAS")
	}
	if string(resp.Kvs[0].Value) != "v2" {
		t.Fatalf("want v2, got %q", resp.Kvs[0].Value)
	}
}

// TestTxn_Atomic_MultiKey verifies that a multi-key atomic Txn either
// commits all ops or none.
func TestTxn_Atomic_MultiKey(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	if _, err := cli.Put(ctx, "/atomic/mk/a", "a1"); err != nil {
		t.Fatalf("put a: %v", err)
	}
	if _, err := cli.Put(ctx, "/atomic/mk/b", "b1"); err != nil {
		t.Fatalf("put b: %v", err)
	}

	grA, err := cli.Get(ctx, "/atomic/mk/a")
	if err != nil || len(grA.Kvs) == 0 {
		t.Fatalf("get a: err=%v kvs=%d", err, len(grA.Kvs))
	}
	grB, err := cli.Get(ctx, "/atomic/mk/b")
	if err != nil || len(grB.Kvs) == 0 {
		t.Fatalf("get b: err=%v kvs=%d", err, len(grB.Kvs))
	}
	revA := grA.Kvs[0].ModRevision
	revB := grB.Kvs[0].ModRevision

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
	rA, err := cli.Get(ctx, "/atomic/mk/a")
	if err != nil || len(rA.Kvs) == 0 {
		t.Fatalf("get a after txn: err=%v", err)
	}
	rB, err := cli.Get(ctx, "/atomic/mk/b")
	if err != nil || len(rB.Kvs) == 0 {
		t.Fatalf("get b after txn: err=%v", err)
	}
	if string(rA.Kvs[0].Value) != "a2" || string(rB.Kvs[0].Value) != "b2" {
		t.Fatalf("want a2/b2, got %q/%q", rA.Kvs[0].Value, rB.Kvs[0].Value)
	}
	if rA.Kvs[0].ModRevision != rB.Kvs[0].ModRevision {
		t.Fatalf("keys have different ModRevision: a=%d b=%d — not atomic",
			rA.Kvs[0].ModRevision, rB.Kvs[0].ModRevision)
	}
}

// TestTxn_Atomic_ElseOp verifies the Else branch executes when compare fails.
func TestTxn_Atomic_ElseOp(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	if _, err := cli.Put(ctx, "/atomic/else/k", "exists"); err != nil {
		t.Fatalf("put: %v", err)
	}

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
	resp, err := cli.Get(ctx, "/atomic/else/k")
	if err != nil || len(resp.Kvs) == 0 {
		t.Fatalf("get: err=%v", err)
	}
	if string(resp.Kvs[0].Value) != "from-else" {
		t.Fatalf("want from-else, got %q", resp.Kvs[0].Value)
	}
}

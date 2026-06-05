package server_test

import (
	"context"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestTxn_MultiOp verifies a transaction with multiple Then operations —
// the pattern Kubernetes uses for atomic multi-key updates.
func TestTxn_MultiOp(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	cli.Put(ctx, "/txn/a", "v1")
	cli.Put(ctx, "/txn/b", "v1")

	grA, _ := cli.Get(ctx, "/txn/a")
	grB, _ := cli.Get(ctx, "/txn/b")
	revA := grA.Kvs[0].ModRevision
	revB := grB.Kvs[0].ModRevision

	// Atomic update of two keys in one transaction.
	txn, err := cli.Txn(ctx).
		If(
			clientv3.Compare(clientv3.ModRevision("/txn/a"), "=", revA),
			clientv3.Compare(clientv3.ModRevision("/txn/b"), "=", revB),
		).
		Then(
			clientv3.OpPut("/txn/a", "v2"),
			clientv3.OpPut("/txn/b", "v2"),
		).
		Commit()
	if err != nil || !txn.Succeeded {
		t.Fatalf("multi-op txn: err=%v succeeded=%v", err, txn.Succeeded)
	}

	time.Sleep(200 * time.Millisecond)
	rA, _ := cli.Get(ctx, "/txn/a")
	rB, _ := cli.Get(ctx, "/txn/b")
	if string(rA.Kvs[0].Value) != "v2" || string(rB.Kvs[0].Value) != "v2" {
		t.Fatalf("want v2/v2, got %q/%q", rA.Kvs[0].Value, rB.Kvs[0].Value)
	}
}

// TestTxn_ElseOp verifies that the Else branch executes when the condition fails.
func TestTxn_ElseOp(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	cli.Put(ctx, "/txnelse/k", "original")

	// Condition uses wrong revision — should fall into Else.
	txn, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision("/txnelse/k"), "=", 0)).
		Then(clientv3.OpPut("/txnelse/k", "from-then")).
		Else(clientv3.OpPut("/txnelse/k", "from-else")).
		Commit()
	if err != nil {
		t.Fatalf("txn: %v", err)
	}
	if txn.Succeeded {
		t.Fatal("condition should have failed, Succeeded should be false")
	}

	time.Sleep(200 * time.Millisecond)
	resp, _ := cli.Get(ctx, "/txnelse/k")
	if string(resp.Kvs[0].Value) != "from-else" {
		t.Fatalf("want from-else, got %q", resp.Kvs[0].Value)
	}
}

// TestTxn_CreateIfNotExists verifies the Kubernetes "create if not exists" pattern:
// VERSION=0 means key does not exist.
func TestTxn_CreateIfNotExists(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	// First call: key doesn't exist → Then branch creates it.
	txn1, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version("/txncreate/k"), "=", 0)).
		Then(clientv3.OpPut("/txncreate/k", "created")).
		Commit()
	if err != nil || !txn1.Succeeded {
		t.Fatalf("create-if-not-exists: err=%v succeeded=%v", err, txn1.Succeeded)
	}

	// Second call: key exists now → condition fails, not created again.
	txn2, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version("/txncreate/k"), "=", 0)).
		Then(clientv3.OpPut("/txncreate/k", "overwritten")).
		Commit()
	if err != nil {
		t.Fatalf("txn2: %v", err)
	}
	if txn2.Succeeded {
		t.Fatal("second create-if-not-exists should fail — key already exists")
	}

	time.Sleep(200 * time.Millisecond)
	resp, _ := cli.Get(ctx, "/txncreate/k")
	if string(resp.Kvs[0].Value) != "created" {
		t.Fatalf("want created, got %q", resp.Kvs[0].Value)
	}
}

// TestTxn_GetInThen verifies that OpGet inside a Then branch returns the value.
func TestTxn_GetInThen(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	cli.Put(ctx, "/txnget/k", "readable")

	resp, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version("/txnget/k"), ">", 0)).
		Then(clientv3.OpGet("/txnget/k")).
		Commit()
	if err != nil || !resp.Succeeded {
		t.Fatalf("txn get-in-then: err=%v succeeded=%v", err, resp.Succeeded)
	}
	if len(resp.Responses) == 0 {
		t.Fatal("want responses in Then")
	}
	kvs := resp.Responses[0].GetResponseRange().Kvs
	if len(kvs) == 0 || string(kvs[0].Value) != "readable" {
		t.Fatalf("want readable in Then response, got %v", kvs)
	}
}

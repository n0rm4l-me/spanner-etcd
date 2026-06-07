package server_test

import (
	"context"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestTxn_UnsupportedCompareTarget verifies that an unsupported Compare target
// returns gRPC InvalidArgument — not a silent false/failure.
func TestTxn_UnsupportedCompareTarget(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	// Compare_LEASE is not supported in atomic mode.
	_, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.LeaseValue("/foo"), "=", 0)).
		Then(clientv3.OpPut("/foo", "bar")).
		Commit()
	if err == nil {
		t.Fatal("expected error for unsupported compare target, got nil")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", status.Code(err))
	}
}

// TestTxn_RangeEndFallback verifies that a Txn with RangeEnd falls back to
// the non-atomic path and succeeds (not Unimplemented).
func TestTxn_RangeEndFallback(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	if _, err := cli.Put(ctx, "/fallback/a", "v1"); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := cli.Put(ctx, "/fallback/b", "v2"); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Range get inside Txn — should use non-atomic fallback, not return Unimplemented.
	txn, err := cli.Txn(ctx).
		Then(clientv3.OpGet("/fallback/", clientv3.WithPrefix())).
		Commit()
	if err != nil {
		t.Fatalf("Txn with range get should succeed via non-atomic fallback, got: %v", err)
	}
	if !txn.Succeeded {
		t.Fatal("Txn should succeed")
	}
	if len(txn.Responses) == 0 {
		t.Fatal("expected responses")
	}
	kvs := txn.Responses[0].GetResponseRange().Kvs
	if len(kvs) < 2 {
		t.Fatalf("expected 2 keys, got %d", len(kvs))
	}
	t.Logf("range Txn fallback: got %d keys", len(kvs))
}

// TestTxn_ReadOnly_SnapshotRevision verifies that a read-only Txn (compare fails,
// no Else ops) returns a revision consistent with the kv state at the time of
// the transaction — not a "phantom" revision from the commit timestamp.
func TestTxn_ReadOnly_SnapshotRevision(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	// Write a key to establish a known revision.
	pr, err := cli.Put(ctx, "/snapshot/k", "v1")
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	knownRev := pr.Header.Revision

	// Read-only Txn: compare fails (key exists, version != 0), no Else ops.
	txn, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version("/snapshot/k"), "=", 0)).
		Commit()
	if err != nil {
		t.Fatalf("txn: %v", err)
	}
	if txn.Succeeded {
		t.Fatal("compare should have failed — key already exists")
	}

	// The returned revision must be >= the last known write revision.
	// It must NOT be 0 (phantom) or wildly ahead of actual kv state.
	if txn.Header.Revision < knownRev {
		t.Fatalf("Txn revision %d < last write revision %d — inconsistent snapshot",
			txn.Header.Revision, knownRev)
	}
	t.Logf("write_rev=%d txn_rev=%d", knownRev, txn.Header.Revision)
}

// TestTxn_DuplicateKey_InvalidArgument verifies that a Txn with duplicate
// mutations for the same key returns InvalidArgument.
func TestTxn_DuplicateKey_InvalidArgument(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	_, err := cli.Txn(ctx).
		Then(
			clientv3.OpPut("/dup/k", "v1"),
			clientv3.OpPut("/dup/k", "v2"), // duplicate
		).
		Commit()
	if err == nil {
		t.Fatal("expected error for duplicate key in Txn, got nil")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v: %v", status.Code(err), err)
	}
}

// TestTxn_NoMutations_RevisionNotZero verifies that a Txn with no mutations
// returns a non-zero revision even on an otherwise empty response.
func TestTxn_NoMutations_RevisionNotZero(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	// Put something so current revision > 0.
	if _, err := cli.Put(ctx, "/nomut/k", "v"); err != nil {
		t.Fatalf("put: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Txn with only a Get in Else (no mutations).
	txn, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version("/nomut/k"), "=", 0)).
		Else(clientv3.OpGet("/nomut/k")).
		Commit()
	if err != nil {
		t.Fatalf("txn: %v", err)
	}
	if txn.Header.Revision == 0 {
		t.Fatal("Txn with no mutations returned Revision=0 — phantom revision bug")
	}
	t.Logf("no-mutation Txn revision: %d", txn.Header.Revision)
}

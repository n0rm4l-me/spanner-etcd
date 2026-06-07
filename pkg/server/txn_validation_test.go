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
	// Boundary key at RangeEnd — WithPrefix("/fallback/") → RangeEnd = "/fallback0"
	if _, err := cli.Put(ctx, "/fallback0", "boundary"); err != nil {
		t.Fatalf("put boundary: %v", err)
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
	// Boundary key /fallback0 must NOT appear in prefix scan results (exclusive RangeEnd)
	for _, kv := range kvs {
		if string(kv.Key) == "/fallback0" {
			t.Fatal("boundary key /fallback0 at RangeEnd must not be included in prefix scan")
		}
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

// TestTxn_IgnoreValue verifies that IgnoreValue=true preserves the existing
// value when updating a key via the non-atomic Txn fallback path.
func TestTxn_IgnoreValue(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	if _, err := cli.Put(ctx, "/ignore/k", "original"); err != nil {
		t.Fatalf("put: %v", err)
	}

	// IgnoreValue=true triggers non-atomic fallback (IgnoreValue is in the fallback list).
	// The value should remain "original" regardless of what is passed.
	_, err := cli.Txn(ctx).
		Then(clientv3.OpPut("/ignore/k", "ignored-value",
			clientv3.WithIgnoreValue(),
			clientv3.WithPrevKV())).
		Commit()
	if err != nil {
		t.Fatalf("IgnoreValue Txn: %v", err)
	}

	resp, err := cli.Get(ctx, "/ignore/k")
	if err != nil || len(resp.Kvs) == 0 {
		t.Fatalf("get: err=%v", err)
	}
	if string(resp.Kvs[0].Value) != "original" {
		t.Fatalf("IgnoreValue should preserve original value, got %q", resp.Kvs[0].Value)
	}
}

// TestTxn_IgnoreValue_KeyNotFound verifies that IgnoreValue on a non-existent
// key returns NotFound.
func TestTxn_IgnoreValue_KeyNotFound(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	_, err := cli.Txn(ctx).
		Then(clientv3.OpPut("/ignore/nonexistent", "val",
			clientv3.WithIgnoreValue())).
		Commit()
	if err == nil {
		t.Fatal("expected NotFound for IgnoreValue on missing key, got nil")
	}
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound, got %v", status.Code(err))
	}
}

// TestTxn_RangeDeleteFallback verifies that a Txn with a range delete (RangeEnd)
// uses the non-atomic fallback, deletes all matching keys, and returns correct count.
func TestTxn_RangeDeleteFallback(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	// Create several keys
	for _, k := range []string{"/rdel/a", "/rdel/b", "/rdel/c"} {
		if _, err := cli.Put(ctx, k, "v"); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}
	// Create boundary key at RangeEnd (should NOT be deleted — exclusive upper bound)
	// WithPrefix("/rdel/") computes RangeEnd = "/rdel0" (last byte incremented)
	if _, err := cli.Put(ctx, "/rdel0", "boundary"); err != nil {
		t.Fatalf("put boundary: %v", err)
	}
	// Create a key outside the range to verify it's not deleted
	if _, err := cli.Put(ctx, "/other/x", "v"); err != nil {
		t.Fatalf("put other: %v", err)
	}

	// Range delete inside Txn — triggers non-atomic fallback
	txn, err := cli.Txn(ctx).
		Then(clientv3.OpDelete("/rdel/", clientv3.WithPrefix())).
		Commit()
	if err != nil {
		t.Fatalf("Txn range delete: %v", err)
	}
	if !txn.Succeeded {
		t.Fatal("Txn should succeed")
	}
	if txn.Header.Revision == 0 {
		t.Fatal("Txn header revision must not be 0")
	}
	// Verify DeleteRange response contains correct deleted count.
	if len(txn.Responses) == 0 {
		t.Fatal("expected responses")
	}
	dresp := txn.Responses[0].GetResponseDeleteRange()
	if dresp == nil {
		t.Fatal("expected DeleteRange response")
	}
	if dresp.Deleted != 3 {
		t.Fatalf("expected 3 deleted, got %d", dresp.Deleted)
	}

	// Verify all /rdel/ keys deleted (check individually, not prefix scan
	// which would use the same rangeToPrefix approximation)
	for _, key := range []string{"/rdel/a", "/rdel/b", "/rdel/c"} {
		r, err := cli.Get(ctx, key)
		if err != nil {
			t.Fatalf("get %s: %v", key, err)
		}
		if len(r.Kvs) != 0 {
			t.Fatalf("expected key %s to be deleted", key)
		}
	}

	// Verify boundary key /rdel0 was NOT deleted (RangeEnd is exclusive)
	resp3, err := cli.Get(ctx, "/rdel0")
	if err != nil || len(resp3.Kvs) == 0 {
		t.Fatal("boundary key /rdel0 at RangeEnd must survive (exclusive upper bound)")
	}

	// Verify /other/x not deleted
	resp2, err := cli.Get(ctx, "/other/x")
	if err != nil || len(resp2.Kvs) == 0 {
		t.Fatalf("other key should survive range delete")
	}
	t.Logf("range delete Txn fallback: revision=%d deleted=%d", txn.Header.Revision, dresp.Deleted)
}

// TestTxn_IgnoreLease verifies that IgnoreLease=true preserves the existing lease.
func TestTxn_IgnoreLease(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	// Grant a lease
	lease, err := cli.Grant(ctx, 30)
	if err != nil {
		t.Fatalf("grant: %v", err)
	}

	// Put key with lease
	if _, err := cli.Put(ctx, "/ignorelease/k", "v1", clientv3.WithLease(lease.ID)); err != nil {
		t.Fatalf("put: %v", err)
	}

	// IgnoreLease=true — should preserve original lease
	_, err = cli.Txn(ctx).
		Then(clientv3.OpPut("/ignorelease/k", "v2", clientv3.WithIgnoreLease())).
		Commit()
	if err != nil {
		t.Fatalf("IgnoreLease Txn: %v", err)
	}

	resp, err := cli.Get(ctx, "/ignorelease/k")
	if err != nil || len(resp.Kvs) == 0 {
		t.Fatalf("get: %v", err)
	}
	if resp.Kvs[0].Lease != int64(lease.ID) {
		t.Fatalf("IgnoreLease should preserve lease %d, got %d", lease.ID, resp.Kvs[0].Lease)
	}
	if string(resp.Kvs[0].Value) != "v2" {
		t.Fatalf("value should be updated to v2, got %q", resp.Kvs[0].Value)
	}
}

// TestTxn_NonAtomic_ElseBranch verifies that the non-atomic fallback
// correctly executes the Else branch when compare fails.
func TestTxn_NonAtomic_ElseBranch(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	if _, err := cli.Put(ctx, "/nonatom/k", "exists"); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Compare fails (key exists) → Else branch with range get (triggers fallback)
	txn, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.Version("/nonatom/k"), "=", 0)).
		Then(clientv3.OpPut("/nonatom/k", "from-then")).
		Else(clientv3.OpGet("/nonatom/", clientv3.WithPrefix())).
		Commit()
	if err != nil {
		t.Fatalf("txn: %v", err)
	}
	if txn.Succeeded {
		t.Fatal("compare should fail — key exists")
	}
	if len(txn.Responses) == 0 {
		t.Fatal("Else branch should return responses")
	}
	kvs := txn.Responses[0].GetResponseRange().Kvs
	if len(kvs) == 0 {
		t.Fatal("Else range get should return keys")
	}
}

// TestTxn_NonAtomic_DuplicateKey verifies duplicate key validation in non-atomic path.
func TestTxn_NonAtomic_DuplicateKey(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	if _, err := cli.Put(ctx, "/dupnonatomic/k", "v"); err != nil {
		t.Fatalf("put: %v", err)
	}

	// IgnoreValue triggers non-atomic path; duplicate key should still fail.
	_, err := cli.Txn(ctx).
		Then(
			clientv3.OpPut("/dupnonatomic/k", "v1", clientv3.WithIgnoreValue()),
			clientv3.OpPut("/dupnonatomic/k", "v2", clientv3.WithIgnoreValue()),
		).
		Commit()
	if err == nil {
		t.Fatal("expected InvalidArgument for duplicate key in non-atomic Txn")
	}
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", status.Code(err))
	}
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

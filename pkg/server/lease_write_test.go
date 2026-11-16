package server_test

import (
	"context"
	"errors"
	"testing"

	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Clients (etcdctl, jetcd) recognise a dead lease by the etcd error and react
// by granting a new one. The error must be the real etcd one, not a generic
// Internal error, and the failed write must leave no trace.

func TestPut_UnknownLease_IsEtcdLeaseNotFound(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	_, err := cli.Put(ctx, "/lw/k", "v", clientv3.WithLease(clientv3.LeaseID(424242)))
	if !errors.Is(err, rpctypes.ErrLeaseNotFound) {
		t.Fatalf("want rpctypes.ErrLeaseNotFound, got %v", err)
	}
	if resp, _ := cli.Get(ctx, "/lw/k"); len(resp.Kvs) != 0 {
		t.Fatalf("rejected Put left a key behind: %v", resp.Kvs)
	}
}

// The client-side recovery flow: the lease is revoked, the next Put with it
// fails with lease-not-found, a fresh Grant makes the same Put succeed, and the
// key then follows the new lease.
func TestPut_AfterRevoke_RegrantRecovers(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	old, err := cli.Grant(ctx, 60)
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	if _, err := cli.Put(ctx, "/lw/svc", "node-1", clientv3.WithLease(old.ID)); err != nil {
		t.Fatalf("put with live lease: %v", err)
	}
	if _, err := cli.Revoke(ctx, old.ID); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	if _, err := cli.Put(ctx, "/lw/svc", "node-1", clientv3.WithLease(old.ID)); !errors.Is(err, rpctypes.ErrLeaseNotFound) {
		t.Fatalf("put with revoked lease: want ErrLeaseNotFound, got %v", err)
	}

	fresh, err := cli.Grant(ctx, 60)
	if err != nil {
		t.Fatalf("regrant: %v", err)
	}
	if _, err := cli.Put(ctx, "/lw/svc", "node-1", clientv3.WithLease(fresh.ID)); err != nil {
		t.Fatalf("put with fresh lease: %v", err)
	}
	resp, err := cli.Get(ctx, "/lw/svc")
	if err != nil || len(resp.Kvs) != 1 || resp.Kvs[0].Lease != int64(fresh.ID) {
		t.Fatalf("want key bound to the fresh lease %d, got %v (err=%v)", fresh.ID, resp, err)
	}
}

func TestTxn_UnknownLease_FailsWholeTxn(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	_, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision("/lw/txn/guard"), "=", 0)).
		Then(
			clientv3.OpPut("/lw/txn/plain", "a"),
			clientv3.OpPut("/lw/txn/leased", "b", clientv3.WithLease(clientv3.LeaseID(424242))),
		).
		Commit()
	if !errors.Is(err, rpctypes.ErrLeaseNotFound) {
		t.Fatalf("want rpctypes.ErrLeaseNotFound, got %v", err)
	}
	for _, k := range []string{"/lw/txn/plain", "/lw/txn/leased"} {
		if resp, _ := cli.Get(ctx, k); len(resp.Kvs) != 0 {
			t.Errorf("%s was written although the Txn failed", k)
		}
	}
}

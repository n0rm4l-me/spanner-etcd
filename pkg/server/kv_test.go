package server_test

import (
	"context"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/n0rm4l-me/spanner-etcd/pkg/server"
)

// testServer starts a real spanner-etcd gRPC server against the Spanner
// emulator and returns an etcd client connected to it.
func testServer(t *testing.T) *clientv3.Client {
	t.Helper()

	s := newTestStore(t)
	log := zap.NewNop()

	srv, err := server.New(context.Background(), s, server.Config{
		ListenAddr:  "127.0.0.1:0", // random port
		MetricsAddr: "",
	}, log)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// Run Serve in background; it sets listener before accepting connections.
	ready := make(chan struct{})
	go func() {
		// Signal readiness after a short delay so listener is bound.
		go func() {
			time.Sleep(200 * time.Millisecond)
			close(ready)
		}()
		srv.Serve(ctx) //nolint:errcheck
	}()
	<-ready

	addr := srv.Addr()
	if addr == "" || addr == "127.0.0.1:0" {
		t.Fatal("server did not bind a port")
	}

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{addr},
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if err != nil {
		t.Fatalf("etcd client: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

func TestKV_PutGet(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	_, err := cli.Put(ctx, "/kv/hello", "world")
	if err != nil {
		t.Fatalf("put: %v", err)
	}

	resp, err := cli.Get(ctx, "/kv/hello")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(resp.Kvs) != 1 || string(resp.Kvs[0].Value) != "world" {
		t.Fatalf("want value=world, got %v", resp.Kvs)
	}
}

func TestKV_Update(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	cli.Put(ctx, "/kv/upd", "v1")
	cli.Put(ctx, "/kv/upd", "v2")
	time.Sleep(200 * time.Millisecond)

	resp, _ := cli.Get(ctx, "/kv/upd")
	if len(resp.Kvs) == 0 || string(resp.Kvs[0].Value) != "v2" {
		t.Fatalf("want v2, got %v", resp.Kvs)
	}
}

func TestKV_Delete(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	cli.Put(ctx, "/kv/del", "bye")
	gr, _ := cli.Get(ctx, "/kv/del")
	modRev := gr.Kvs[0].ModRevision

	// Kubernetes-style delete via Txn.
	txn, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision("/kv/del"), "=", modRev)).
		Then(clientv3.OpDelete("/kv/del")).
		Commit()
	if err != nil || !txn.Succeeded {
		t.Fatalf("delete txn: err=%v succeeded=%v", err, txn.Succeeded)
	}

	time.Sleep(200 * time.Millisecond)
	resp, _ := cli.Get(ctx, "/kv/del")
	if len(resp.Kvs) != 0 {
		t.Fatalf("want 0 keys after delete, got %d", len(resp.Kvs))
	}
}

func TestKV_DeleteRange(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	cli.Put(ctx, "/dr/a", "1")
	cli.Put(ctx, "/dr/b", "2")
	cli.Put(ctx, "/dr/c", "3")

	dresp, err := cli.Delete(ctx, "/dr/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("delete range: %v", err)
	}
	if dresp.Deleted != 3 {
		t.Fatalf("want deleted=3, got %d", dresp.Deleted)
	}

	resp, _ := cli.Get(ctx, "/dr/", clientv3.WithPrefix())
	if len(resp.Kvs) != 0 {
		t.Fatalf("want 0 keys, got %d", len(resp.Kvs))
	}
}

func TestKV_Prefix(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	cli.Put(ctx, "/pfx/a", "1")
	cli.Put(ctx, "/pfx/b", "2")
	cli.Put(ctx, "/other/x", "3")

	resp, err := cli.Get(ctx, "/pfx/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("prefix get: %v", err)
	}
	if len(resp.Kvs) != 2 {
		t.Fatalf("want 2 keys, got %d", len(resp.Kvs))
	}
}

func TestKV_HistoricalGet(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	pr1, _ := cli.Put(ctx, "/hist/k", "v1")
	rev1 := pr1.Header.Revision
	cli.Put(ctx, "/hist/k", "v2")
	cli.Put(ctx, "/hist/k", "v3")

	resp, err := cli.Get(ctx, "/hist/k", clientv3.WithRev(rev1))
	if err != nil {
		t.Fatalf("historical get: %v", err)
	}
	if len(resp.Kvs) == 0 || string(resp.Kvs[0].Value) != "v1" {
		t.Fatalf("want v1 at rev=%d, got %v", rev1, resp.Kvs)
	}
}

func TestKV_Txn_CAS(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	cli.Put(ctx, "/cas/k", "initial")
	gr, _ := cli.Get(ctx, "/cas/k")
	modRev := gr.Kvs[0].ModRevision

	// Successful CAS.
	txn, err := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision("/cas/k"), "=", modRev)).
		Then(clientv3.OpPut("/cas/k", "updated")).
		Commit()
	if err != nil || !txn.Succeeded {
		t.Fatalf("CAS txn failed: err=%v succeeded=%v", err, txn.Succeeded)
	}

	// Stale CAS should fail.
	txn2, _ := cli.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision("/cas/k"), "=", modRev)).
		Then(clientv3.OpPut("/cas/k", "should-not-set")).
		Commit()
	if txn2.Succeeded {
		t.Fatal("stale CAS should not succeed")
	}

	time.Sleep(200 * time.Millisecond)
	resp, _ := cli.Get(ctx, "/cas/k")
	if string(resp.Kvs[0].Value) != "updated" {
		t.Fatalf("want updated, got %q", resp.Kvs[0].Value)
	}
}

func TestKV_Compact(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	cli.Put(ctx, "/cmp/k", "v1")
	cli.Put(ctx, "/cmp/k", "v2")
	cur, _ := cli.Get(ctx, "/cmp/k")
	curRev := cur.Header.Revision

	_, err := cli.Compact(ctx, curRev-1)
	if err != nil {
		t.Fatalf("compact: %v", err)
	}

	resp, _ := cli.Get(ctx, "/cmp/k")
	if len(resp.Kvs) == 0 || string(resp.Kvs[0].Value) != "v2" {
		t.Fatalf("current value should be v2 after compact, got %v", resp.Kvs)
	}
}

func TestWatch_LiveViaGRPC(t *testing.T) {
	cli := testServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wCh := cli.Watch(ctx, "/wgrpc/", clientv3.WithPrefix())
	time.Sleep(200 * time.Millisecond)

	cli.Put(ctx, "/wgrpc/a", "ev1")
	cli.Put(ctx, "/wgrpc/b", "ev2")

	var got []string
	deadline := time.After(5 * time.Second)
	for len(got) < 2 {
		select {
		case resp := <-wCh:
			for _, ev := range resp.Events {
				got = append(got, string(ev.Kv.Value))
			}
		case <-deadline:
			t.Fatalf("timeout: got only %v", got)
		}
	}
	if got[0] != "ev1" || got[1] != "ev2" {
		t.Fatalf("want [ev1 ev2], got %v", got)
	}
}

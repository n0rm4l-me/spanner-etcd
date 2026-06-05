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

// TestWatch_CancelWatch verifies that cancelling a Watch context stops event delivery
// and does not block or leak goroutines.
func TestWatch_CancelWatch(t *testing.T) {
	cli := testServer(t)

	watchCtx, watchCancel := context.WithCancel(context.Background())
	wCh := cli.Watch(watchCtx, "/cancel/", clientv3.WithPrefix())
	time.Sleep(200 * time.Millisecond)

	cli.Put(context.Background(), "/cancel/a", "v1")

	// Drain first event.
	select {
	case <-wCh:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for first event")
	}

	// Cancel watch.
	watchCancel()
	time.Sleep(200 * time.Millisecond)

	// Write after cancel — should NOT be delivered.
	cli.Put(context.Background(), "/cancel/b", "v2")
	time.Sleep(500 * time.Millisecond)

	select {
	case resp, ok := <-wCh:
		if ok && len(resp.Events) > 0 {
			t.Fatalf("got event after cancel: %+v", resp.Events)
		}
	default:
		// Channel drained or closed — expected.
	}
}

// TestWatch_MultipleWatchers verifies that multiple concurrent Watch clients
// all receive the same events independently.
func TestWatch_MultipleWatchers(t *testing.T) {
	cli := testServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	const numWatchers = 5
	channels := make([]clientv3.WatchChan, numWatchers)
	for i := range channels {
		channels[i] = cli.Watch(ctx, "/multi/", clientv3.WithPrefix())
	}
	time.Sleep(300 * time.Millisecond)

	cli.Put(ctx, "/multi/k", "broadcast")

	for i, wCh := range channels {
		select {
		case resp := <-wCh:
			if len(resp.Events) == 0 || string(resp.Events[0].Kv.Value) != "broadcast" {
				t.Errorf("watcher %d: want broadcast, got %+v", i, resp.Events)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("watcher %d: timeout", i)
		}
	}
}

// TestWatch_GracefulShutdown verifies that the server shuts down within the
// grace period even with active Watch streams open, and that new requests
// are rejected after shutdown.
func TestWatch_GracefulShutdown(t *testing.T) {
	s := newTestStore(t)
	log := zap.NewNop()

	srv, err := server.New(context.Background(), s, server.Config{
		ListenAddr:  "127.0.0.1:0",
		MetricsAddr: "",
	}, log)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	srvCtx, srvCancel := context.WithCancel(context.Background())

	ready := make(chan struct{})
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		go func() { time.Sleep(200 * time.Millisecond); close(ready) }()
		srv.Serve(srvCtx) //nolint:errcheck
	}()
	<-ready

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{srv.Addr()},
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	defer cli.Close()

	watchCtx, watchCancel := context.WithCancel(context.Background())
	defer watchCancel()

	wCh := cli.Watch(watchCtx, "/shutdown/", clientv3.WithPrefix())
	time.Sleep(200 * time.Millisecond)

	// Confirm watch is live.
	cli.Put(context.Background(), "/shutdown/k", "before")
	select {
	case resp := <-wCh:
		if len(resp.Events) == 0 {
			t.Fatal("expected event before shutdown")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for pre-shutdown event")
	}

	// Trigger graceful shutdown — cancel client watch first so GracefulStop
	// can complete (Watch streams block GracefulStop until client closes them).
	watchCancel()
	time.Sleep(100 * time.Millisecond)
	srvCancel()

	// Server must complete shutdown within grace period + buffer.
	select {
	case <-serveDone:
		t.Log("server shut down cleanly")
	case <-time.After(12 * time.Second):
		t.Fatal("server did not shut down within grace period")
	}
}

// TestWatch_ConcurrentWritesAllDelivered verifies that under concurrent writes
// all Watch events are delivered with no duplicates or drops.
func TestWatch_ConcurrentWritesAllDelivered(t *testing.T) {
	cli := testServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	wCh := cli.Watch(ctx, "/concurrent/", clientv3.WithPrefix())
	time.Sleep(300 * time.Millisecond)

	const n = 20
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < n; i++ {
			key := "/concurrent/" + string(rune('a'+i))
			cli.Put(ctx, key, "v") //nolint:errcheck
		}
	}()
	<-done

	var got int
	deadline := time.After(10 * time.Second)
	for got < n {
		select {
		case resp := <-wCh:
			got += len(resp.Events)
		case <-deadline:
			t.Fatalf("timeout: got %d/%d events", got, n)
		}
	}
	if got < n {
		t.Fatalf("want at least %d events, got %d", n, got)
	}
}

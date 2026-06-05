package server_test

import (
	"context"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/n0rm4l-me/spanner-etcd/pkg/server"
)

// TestAuth_TokenExpiry verifies that when a token expires the client
// automatically re-authenticates (jetcd detects "etcdserver: invalid auth token"
// and calls Authenticate again) without returning an error to the caller.
func TestAuth_TokenExpiry(t *testing.T) {
	s := newTestStore(t)
	log := newTestLogger(t)

	srv, err := server.New(context.Background(), s, server.Config{
		ListenAddr:   "127.0.0.1:0",
		MetricsAddr:  "",
		AuthUsers:    "root:secret",
		AuthTokenTTL: 2 * time.Second, // very short TTL for testing
	}, log)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ready := make(chan struct{})
	go func() {
		go func() { time.Sleep(200 * time.Millisecond); close(ready) }()
		srv.Serve(ctx) //nolint:errcheck
	}()
	<-ready

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{srv.Addr()},
		DialTimeout: 5 * time.Second,
		Username:    "root",
		Password:    "secret",
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	t.Cleanup(func() { cli.Close() })

	// Write a key before expiry.
	if _, err := cli.Put(ctx, "/auth/test", "before"); err != nil {
		t.Fatalf("put before expiry: %v", err)
	}

	t.Log("waiting for token to expire (2s TTL + 500ms buffer)...")
	time.Sleep(2500 * time.Millisecond)

	// After expiry: jetcd must re-authenticate automatically.
	// If it receives "etcdserver: invalid auth token" it retries with fresh token.
	if _, err := cli.Put(ctx, "/auth/test", "after-expiry"); err != nil {
		t.Fatalf("put after token expiry: client did not re-authenticate: %v", err)
	}

	// Verify the value was written.
	resp, err := cli.Get(ctx, "/auth/test")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(resp.Kvs) == 0 || string(resp.Kvs[0].Value) != "after-expiry" {
		t.Fatalf("want 'after-expiry', got %v", resp.Kvs)
	}
	t.Log("✓ client re-authenticated transparently after token expiry")
}

// TestAuth_WrongPassword verifies that wrong credentials are rejected.
func TestAuth_WrongPassword(t *testing.T) {
	s := newTestStore(t)
	log := newTestLogger(t)

	srv, err := server.New(context.Background(), s, server.Config{
		ListenAddr:  "127.0.0.1:0",
		AuthUsers:   "root:correct",
		AuthTokenTTL: time.Hour,
	}, log)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ready := make(chan struct{})
	go func() {
		go func() { time.Sleep(200 * time.Millisecond); close(ready) }()
		srv.Serve(ctx) //nolint:errcheck
	}()
	<-ready

	_, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{srv.Addr()},
		DialTimeout: 5 * time.Second,
		Username:    "root",
		Password:    "wrong",
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if err == nil {
		t.Fatal("expected error with wrong password, got nil")
	}
	t.Logf("✓ wrong password rejected: %v", err)
}

// TestAuth_NoAuth verifies that without credentials, requests are rejected
// when auth is enabled.
func TestAuth_NoAuth(t *testing.T) {
	s := newTestStore(t)
	log := newTestLogger(t)

	srv, err := server.New(context.Background(), s, server.Config{
		ListenAddr:   "127.0.0.1:0",
		AuthUsers:    "root:secret",
		AuthTokenTTL: time.Hour,
	}, log)
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	ready := make(chan struct{})
	go func() {
		go func() { time.Sleep(200 * time.Millisecond); close(ready) }()
		srv.Serve(ctx) //nolint:errcheck
	}()
	<-ready

	// Connect without credentials.
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{srv.Addr()},
		DialTimeout: 5 * time.Second,
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	t.Cleanup(func() { cli.Close() })

	_, err = cli.Put(ctx, "/noauth/test", "value")
	if err == nil {
		t.Fatal("expected Unauthenticated error, got nil")
	}
	t.Logf("✓ unauthenticated request rejected: %v", err)
}

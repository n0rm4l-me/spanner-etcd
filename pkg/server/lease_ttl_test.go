package server_test

import (
	"context"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// TestLeaseTimeToLive_RemainingTTL verifies that LeaseTimeToLive returns the
// actual remaining TTL, not the original grant TTL.
func TestLeaseTimeToLive_RemainingTTL(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	// Grant a 5-second lease.
	resp, err := cli.Grant(ctx, 5)
	if err != nil {
		t.Fatalf("grant: %v", err)
	}

	// Wait 2 seconds — remaining TTL should be ~3s, not 5s.
	time.Sleep(2 * time.Second)

	ttlResp, err := cli.TimeToLive(ctx, resp.ID)
	if err != nil {
		t.Fatalf("timetolive: %v", err)
	}

	if ttlResp.GrantedTTL != 5 {
		t.Errorf("GrantedTTL: want 5, got %d", ttlResp.GrantedTTL)
	}
	// Remaining TTL should be ≤ 3 (we waited 2s) and > 0.
	if ttlResp.TTL <= 0 || ttlResp.TTL > 3 {
		t.Errorf("TTL: want 1-3, got %d (should reflect remaining time, not grant TTL)", ttlResp.TTL)
	}
	t.Logf("granted=%ds remaining=%ds", ttlResp.GrantedTTL, ttlResp.TTL)
}

// TestLeaseTimeToLive_GrantedTTLPreserved verifies GrantedTTL is always the original grant value.
func TestLeaseTimeToLive_GrantedTTLPreserved(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()

	resp, err := cli.Grant(ctx, 30)
	if err != nil {
		t.Fatalf("grant: %v", err)
	}
	// Send keepalive.
	_, err = cli.KeepAliveOnce(ctx, resp.ID)
	if err != nil {
		t.Fatalf("keepalive: %v", err)
	}

	ttlResp, err := cli.TimeToLive(ctx, resp.ID)
	if err != nil {
		t.Fatalf("timetolive: %v", err)
	}
	if ttlResp.GrantedTTL != 30 {
		t.Errorf("GrantedTTL: want 30, got %d", ttlResp.GrantedTTL)
	}
	if ttlResp.TTL <= 0 || ttlResp.TTL > 30 {
		t.Errorf("TTL out of range: %d", ttlResp.TTL)
	}
}

// TestWatch_ReplayPagination verifies that Watch replay correctly delivers
// more than pollBatchSize (500) historical events.
func TestWatch_ReplayPagination(t *testing.T) {
	cli := testServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Record start revision before writing.
	statusResp, err := cli.Status(ctx, cli.Endpoints()[0])
	if err != nil {
		t.Fatalf("status: %v", err)
	}
	startRev := statusResp.Header.Revision

	// Write 600 keys — more than pollBatchSize=500.
	const n = 600
	for i := 0; i < n; i++ {
		key := "/replay/pagination/" + string(rune('a'+i%26)) + string(rune('0'+i/26))
		if _, err := cli.Put(ctx, key, "v"); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	// Subscribe from startRev — should replay all 600 events.
	wCh := cli.Watch(ctx, "/replay/pagination/",
		clientv3.WithPrefix(),
		clientv3.WithRev(startRev),
	)

	var got int
	deadline := time.After(30 * time.Second)
	for got < n {
		select {
		case resp := <-wCh:
			if resp.Err() != nil {
				t.Fatalf("watch error: %v", resp.Err())
			}
			got += len(resp.Events)
		case <-deadline:
			t.Fatalf("timeout: got %d/%d events (replay pagination may be broken)", got, n)
		}
	}
	t.Logf("received %d/%d events via paginated replay", got, n)
}

// TestWatch_NoPanicOnContextCancel verifies that cancelling a watch context
// causes the WatchChan to close cleanly without panicking or blocking.
func TestWatch_NoPanicOnContextCancel(t *testing.T) {
	cli := testServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	wCh := cli.Watch(ctx, "/canceled/", clientv3.WithPrefix())
	time.Sleep(100 * time.Millisecond)

	cancel()

	select {
	case _, ok := <-wCh:
		_ = ok // channel closed or drained — either is fine
	case <-time.After(3 * time.Second):
		t.Fatal("watch channel did not close within 3s after context cancellation")
	}
}

// TestWatch_CanceledResponseOnSubscriberDrop verifies that cancelling a watch
// causes the WatchChan to close cleanly. A full channel-overflow test requires
// internal hooks not exposed via the public gRPC API.
func TestWatch_CanceledResponseOnSubscriberDrop(t *testing.T) {
	cli := testServer(t)
	ctx := context.Background()
	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()

	wCh := cli.Watch(watchCtx, "/drop/", clientv3.WithPrefix())
	time.Sleep(100 * time.Millisecond)

	// Write an event so the watch is confirmed live.
	cli.Put(ctx, "/drop/k", "v")
	select {
	case resp := <-wCh:
		if resp.Err() != nil {
			t.Fatalf("unexpected error: %v", resp.Err())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for event")
	}

	// Cancel — the WatchChan must close or drain within a reasonable time.
	watchCancel()
	select {
	case _, ok := <-wCh:
		_ = ok
	case <-time.After(3 * time.Second):
		t.Fatal("watch channel did not close within 3s after watchCancel()")
	}
}

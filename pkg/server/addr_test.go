package server_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/n0rm4l-me/spanner-etcd/pkg/server"
)

// Addr is called from another goroutine while Serve is binding its listener, which
// is how the test helpers (and any embedder) use it. Under -race this fails if the
// two are not ordered. Ready must also report the real, bound address.
func TestServer_AddrIsSafeWhileServeStarts(t *testing.T) {
	s := newTestStore(t)
	srv, err := server.New(context.Background(), s, server.Config{ListenAddr: "127.0.0.1:0"}, zap.NewNop())
	if err != nil {
		t.Fatalf("new server: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = srv.Addr()
				}
			}
		}()
	}

	served := make(chan struct{})
	go func() {
		defer close(served)
		srv.Serve(ctx) //nolint:errcheck
	}()

	select {
	case <-srv.Ready():
	case <-time.After(10 * time.Second):
		t.Fatal("server did not report ready")
	}
	if addr := srv.Addr(); addr == "" || addr == "127.0.0.1:0" {
		t.Errorf("after Ready, Addr must be the bound address, got %q", addr)
	}

	close(stop)
	wg.Wait()
	cancel()
	<-served
}

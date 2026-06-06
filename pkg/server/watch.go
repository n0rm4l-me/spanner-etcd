package server

import (
	"io"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/n0rm4l-me/spanner-etcd/pkg/store"
)

// progressNotifyInterval matches the etcd default (10 minutes).
// This is a keepalive heartbeat only — real events are delivered immediately.
// Clients that expect frequent progress notifications (e.g. for leader election)
// can lower this via the --watch-progress-notify-interval flag on the API server.
const progressNotifyInterval = 10 * time.Minute

// WatchServer implements etcdserverpb.WatchServer.
type WatchServer struct {
	etcdserverpb.UnimplementedWatchServer
	store *store.Store
	log   *zap.Logger
}

func newWatchServer(s *store.Store, log *zap.Logger) *WatchServer {
	return &WatchServer{store: s, log: log}
}

// Watch handles a bidirectional gRPC stream. Each WatchCreateRequest spawns
// a goroutine that polls Spanner for events and streams them back.
func (w *WatchServer) Watch(stream etcdserverpb.Watch_WatchServer) error {
	ctx := stream.Context()

	// watchID → cancel for cleanup
	watches := make(map[int64]func())
	var nextID int64

	// Responses from per-watch goroutines are merged here.
	respCh := make(chan *etcdserverpb.WatchResponse, 64)

	// cancelledCh carries watch IDs cancelled by the store layer (sentinel path).
	// Unbuffered so senders block until the receive loop drains it — this
	// guarantees no IDs are dropped even when stream.Recv() is blocking.
	cancelledCh := make(chan int64)

	// sendErrCh receives the first stream.Send error so the main loop can exit
	// and propagate non-cancel failures to the caller.
	sendErrCh := make(chan error, 1)
	var sendErr error

	// Send loop — runs in its own goroutine.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case resp, ok := <-respCh:
				if !ok {
					return
				}
				if err := stream.Send(resp); err != nil {
					select {
					case sendErrCh <- err:
					default:
					}
					return
				}
			}
		}
	}()

	// reqCh merges stream.Recv() results with store-side cancellations so the
	// receive loop never misses a cancelled watch while blocked on Recv().
	type recvResult struct {
		req *etcdserverpb.WatchRequest
		err error
	}
	reqCh := make(chan recvResult, 1)
	go func() {
		for {
			req, err := stream.Recv()
			select {
			case reqCh <- recvResult{req, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	drainCancelled := func() {
		for {
			select {
			case id := <-cancelledCh:
				if cancel, ok := watches[id]; ok {
					cancel()
					delete(watches, id)
				}
			default:
				return
			}
		}
	}

	// Receive loop — selects across incoming requests AND store cancellations.
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case err := <-sendErrCh:
			sendErr = err
			break loop
		case id := <-cancelledCh:
			if cancel, ok := watches[id]; ok {
				cancel()
				delete(watches, id)
			}
		case rr := <-reqCh:
			req, err := rr.req, rr.err
			if err == io.EOF {
				break loop
			}
			if err != nil {
				if status.Code(err) == codes.Canceled {
					break loop
				}
				return err
			}

			drainCancelled()

			switch v := req.RequestUnion.(type) {
			case *etcdserverpb.WatchRequest_CreateRequest:
				cr := v.CreateRequest
				id := nextID
				nextID++

				key := string(cr.Key)
				prefix := watchPrefix(key, string(cr.RangeEnd))
				startRev := cr.StartRevision

				watchCtx, cancel := contextWithCancel(ctx)
				watches[id] = cancel

				respCh <- &etcdserverpb.WatchResponse{
					Header:  header(0),
					WatchId: id,
					Created: true,
				}

				go w.watchLoop(watchCtx, id, prefix, startRev, respCh, cancelledCh)

			case *etcdserverpb.WatchRequest_CancelRequest:
				id := v.CancelRequest.WatchId
				if cancel, ok := watches[id]; ok {
					cancel()
					delete(watches, id)
				}
				respCh <- &etcdserverpb.WatchResponse{
					Header:   header(0),
					WatchId:  id,
					Canceled: true,
				}
			}
		}
	}

	// Cancel all open watches.
	for _, cancel := range watches {
		cancel()
	}
	if sendErr != nil && status.Code(sendErr) != codes.Canceled {
		return sendErr
	}
	return nil
}

// watchLoop subscribes to store events and sends them to the response channel.
func (w *WatchServer) watchLoop(
	ctx interface{ Done() <-chan struct{} },
	watchID int64,
	prefix string,
	startRev int64,
	respCh chan<- *etcdserverpb.WatchResponse,
	cancelledCh chan<- int64,
) {
	// Use a background-cancelled context.
	goctx, ok := ctx.(interface {
		Done() <-chan struct{}
		Err() error
	})
	if !ok {
		return
	}

	// Build a Go context from the stream context.
	type canceler interface {
		Done() <-chan struct{}
	}

	stdCtx, cancel := streamContextWithCancel(goctx)
	defer cancel()

	eventCh := w.store.Watch(stdCtx, prefix, startRev)
	progressTicker := time.NewTicker(progressNotifyInterval)
	defer progressTicker.Stop()

	for {
		select {
		case <-stdCtx.Done():
			return
		case events := <-eventCh:
			// closedSentinel (empty non-nil slice) signals the subscription was
			// torn down by the store layer (channel overflow, compaction, or
			// other internal error). Notify the client and clean up the watches map.
			if len(events) == 0 {
				// Include CompactRevision so etcd clients know the minimum safe
				// revision they can resume from after this cancellation.
				compactRev, _ := w.store.CompactRevision(stdCtx)
				select {
				case respCh <- &etcdserverpb.WatchResponse{
					Header:          header(0),
					WatchId:         watchID,
					Canceled:        true,
					CompactRevision: compactRev,
				}:
				case <-stdCtx.Done():
				}
				// Signal the receive loop to remove this watch ID from the map.
				// Block until delivered or the stream context is done — never drop.
				select {
				case cancelledCh <- watchID:
				case <-stdCtx.Done():
				}
				return
			}
			var pbEvents []*mvccpb.Event
			var maxRev int64
			for _, ev := range events {
				pbEv := toWatchEvent(ev)
				pbEvents = append(pbEvents, pbEv)
				if ev.KV.Rev > maxRev {
					maxRev = ev.KV.Rev
				}
			}
			select {
			case respCh <- &etcdserverpb.WatchResponse{
				Header:  header(maxRev),
				WatchId: watchID,
				Events:  pbEvents,
			}:
			case <-stdCtx.Done():
				return
			}
		case <-progressTicker.C:
			curRev, _ := w.store.CurrentRevision(stdCtx)
			select {
			case respCh <- &etcdserverpb.WatchResponse{
				Header:  header(curRev),
				WatchId: watchID,
			}:
			case <-stdCtx.Done():
				return
			}
		}
	}
}

func toWatchEvent(ev *store.Event) *mvccpb.Event {
	pbEv := &mvccpb.Event{
		Kv: toProtoKV(ev.KV),
	}
	// Always populate PrevKv when we have the previous value.
	// Kubernetes API server requires PrevKv for all Watch events (PUT and DELETE)
	// to maintain its internal watch cache. Events with PrevKv=nil cause
	// "watch chan error: etcd event received with PrevKv=nil".
	if ev.KV.PrevRevision > 0 && ev.KV.OldValue != nil {
		pbEv.PrevKv = &mvccpb.KeyValue{
			Key:            []byte(ev.KV.Key),
			Value:          ev.KV.OldValue,
			ModRevision:    ev.KV.PrevRevision,
			CreateRevision: ev.KV.CreateRevision,
		}
	}
	if ev.Type == store.EventDelete {
		pbEv.Type = mvccpb.DELETE
	} else {
		pbEv.Type = mvccpb.PUT
	}
	return pbEv
}

// watchPrefix converts etcd [key, rangeEnd) to a prefix string for the store.
func watchPrefix(key, rangeEnd string) string {
	if rangeEnd == "" {
		return key
	}
	if rangeEnd == "\x00" {
		return ""
	}
	// Prefix scan: rangeEnd = key with last byte incremented.
	if len(key) > 0 && len(rangeEnd) == len(key) {
		return key[:len(key)-1]
	}
	return key
}

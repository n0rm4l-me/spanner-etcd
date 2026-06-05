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

	// Send loop — runs in this goroutine.
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
					return
				}
			}
		}
	}()

	// Receive loop.
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if status.Code(err) == codes.Canceled {
				break
			}
			return err
		}

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

			// Send initial confirmation.
			respCh <- &etcdserverpb.WatchResponse{
				Header:  header(0),
				WatchId: id,
				Created: true,
			}

			go w.watchLoop(watchCtx, id, prefix, startRev, respCh)

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

	// Cancel all open watches.
	for _, cancel := range watches {
		cancel()
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
		case events, ok := <-eventCh:
			if !ok {
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
			respCh <- &etcdserverpb.WatchResponse{
				Header:  header(maxRev),
				WatchId: watchID,
				Events:  pbEvents,
			}
		case <-progressTicker.C:
			curRev, _ := w.store.CurrentRevision(stdCtx)
			respCh <- &etcdserverpb.WatchResponse{
				Header:  header(curRev),
				WatchId: watchID,
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

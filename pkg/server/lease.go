package server

import (
	"context"
	"io"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/paas/spanner-etcd/pkg/store"
)

// LeaseServer implements etcdserverpb.LeaseServer.
type LeaseServer struct {
	etcdserverpb.UnimplementedLeaseServer
	leases *store.LeaseManager
	log    *zap.Logger
}

func newLeaseServer(lm *store.LeaseManager, log *zap.Logger) *LeaseServer {
	return &LeaseServer{leases: lm, log: log}
}

// LeaseGrant creates a new lease.
func (l *LeaseServer) LeaseGrant(ctx context.Context, r *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	ttl := r.TTL
	if ttl <= 0 {
		ttl = 15 // etcd default
	}
	lease, err := l.leases.Grant(ctx, ttl)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "grant lease: %v", err)
	}
	return &etcdserverpb.LeaseGrantResponse{
		Header: header(0),
		ID:     lease.ID,
		TTL:    lease.TTL,
	}, nil
}

// LeaseRevoke removes a lease and its associated keys.
func (l *LeaseServer) LeaseRevoke(ctx context.Context, r *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	if err := l.leases.Revoke(ctx, r.ID); err != nil {
		return nil, status.Errorf(codes.Internal, "revoke lease: %v", err)
	}
	return &etcdserverpb.LeaseRevokeResponse{Header: header(0)}, nil
}

// LeaseKeepAlive handles the bidirectional keepalive stream.
func (l *LeaseServer) LeaseKeepAlive(stream etcdserverpb.Lease_LeaseKeepAliveServer) error {
	ctx := stream.Context()
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		ttl, err := l.leases.Keepalive(ctx, req.ID)
		if err != nil {
			return status.Errorf(codes.Internal, "keepalive: %v", err)
		}
		if err := stream.Send(&etcdserverpb.LeaseKeepAliveResponse{
			Header: header(0),
			ID:     req.ID,
			TTL:    ttl,
		}); err != nil {
			return err
		}
	}
}

// LeaseTimeToLive returns TTL info for a lease.
func (l *LeaseServer) LeaseTimeToLive(ctx context.Context, r *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	lease := l.leases.Get(r.ID)
	if lease == nil {
		return nil, status.Errorf(codes.NotFound, "lease not found: %d", r.ID)
	}
	return &etcdserverpb.LeaseTimeToLiveResponse{
		Header:     header(0),
		ID:         lease.ID,
		TTL:        lease.TTL,
		GrantedTTL: lease.TTL,
	}, nil
}

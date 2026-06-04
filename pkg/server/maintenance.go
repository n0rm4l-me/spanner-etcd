package server

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.uber.org/zap"

	"github.com/n0rm4l-me/spanner-etcd/pkg/store"
)

// MaintenanceServer implements etcdserverpb.MaintenanceServer.
type MaintenanceServer struct {
	etcdserverpb.UnimplementedMaintenanceServer
	store     *store.Store
	memberID  uint64
	clusterID uint64
	version   string
	log       *zap.Logger
}

func newMaintenanceServer(s *store.Store, memberID, clusterID uint64, version string, log *zap.Logger) *MaintenanceServer {
	return &MaintenanceServer{
		store:     s,
		memberID:  memberID,
		clusterID: clusterID,
		version:   version,
		log:       log,
	}
}

// Status returns server status. Kubernetes uses this as a health check.
func (m *MaintenanceServer) Status(ctx context.Context, r *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
	rev, err := m.store.CurrentRevision(ctx)
	if err != nil {
		rev = 0
	}
	return &etcdserverpb.StatusResponse{
		Header: &etcdserverpb.ResponseHeader{
			ClusterId: m.clusterID,
			MemberId:  m.memberID,
			Revision:  rev,
			RaftTerm:  1,
		},
		Version:  m.version,
		Leader:   m.memberID,
		RaftTerm: 1,
	}, nil
}

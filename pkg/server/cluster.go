package server

import (
	"context"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.uber.org/zap"
)

// ClusterServer implements etcdserverpb.ClusterServer.
// Kubernetes uses MemberList to discover cluster topology.
type ClusterServer struct {
	etcdserverpb.UnimplementedClusterServer
	memberID  uint64
	clusterID uint64
	peerURLs  []string
	log       *zap.Logger
}

func newClusterServer(memberID, clusterID uint64, peerURLs []string, log *zap.Logger) *ClusterServer {
	return &ClusterServer{
		memberID:  memberID,
		clusterID: clusterID,
		peerURLs:  peerURLs,
		log:       log,
	}
}

// MemberList returns the list of cluster members. In a stateless deployment,
// each spanner-etcd replica reports itself as a member. The actual member list
// should be populated from a Kubernetes endpoint or service discovery.
func (c *ClusterServer) MemberList(ctx context.Context, r *etcdserverpb.MemberListRequest) (*etcdserverpb.MemberListResponse, error) {
	return &etcdserverpb.MemberListResponse{
		Header: &etcdserverpb.ResponseHeader{
			ClusterId: c.clusterID,
			MemberId:  c.memberID,
			RaftTerm:  1,
		},
		Members: []*etcdserverpb.Member{
			{
				ID:       c.memberID,
				Name:     "spanner-etcd",
				PeerURLs: c.peerURLs,
			},
		},
	}, nil
}

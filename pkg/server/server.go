// Package server assembles all gRPC services and starts the listener.
package server

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/reflection"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/n0rm4l-me/spanner-etcd/pkg/store"
)

const (
	defaultVersion   = "3.5.13"
	defaultClusterID = uint64(0x1234567890abcdef)
	defaultMemberID  = uint64(0xfedcba9876543210)
)

// Config holds server configuration.
type Config struct {
	ListenAddr  string
	TLSCert     string // server certificate file
	TLSKey      string // server private key file
	TLSCAFile   string // CA cert for verifying client certs (enables mTLS when set)
	MetricsAddr string // HTTP address for /metrics; empty = disabled
	AuthUsers    string        // "user1:pass1,user2:pass2" — empty = auth disabled
	AuthTokenTTL time.Duration // token lifetime; 0 = DefaultTokenTTL (5 min)
	PeerURLs    []string
	Version     string
	MemberID    uint64
	ClusterID   uint64
}

// Server wraps the gRPC server and all etcd service implementations.
type Server struct {
	grpc     *grpc.Server
	store    *store.Store
	config   Config
	log      *zap.Logger
	listener net.Listener // set during Serve, used by Addr()
}

// Addr returns the address the server is listening on.
// Only valid after Serve has been called.
func (s *Server) Addr() string {
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.config.ListenAddr
}

// New creates a Server. Call Serve to start accepting connections.
func New(ctx context.Context, s *store.Store, cfg Config, log *zap.Logger) (*Server, error) {
	if cfg.Version == "" {
		cfg.Version = defaultVersion
	}
	if cfg.MemberID == 0 {
		cfg.MemberID = defaultMemberID
	}
	if cfg.ClusterID == 0 {
		cfg.ClusterID = defaultClusterID
	}

	// Auth — parse users and build interceptors.
	auth := newAuthServer(parseUsers(cfg.AuthUsers), cfg.AuthTokenTTL, log)
	if auth.enabled {
		log.Info("auth enabled", zap.Int("users", len(auth.users)))
	}

	var opts []grpc.ServerOption

	// TLS / mTLS.
	if cfg.TLSCert != "" && cfg.TLSKey != "" {
		creds, err := buildServerCreds(cfg.TLSCert, cfg.TLSKey, cfg.TLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("load TLS creds: %w", err)
		}
		opts = append(opts, grpc.Creds(creds))
		log.Info("TLS enabled", zap.Bool("mtls", cfg.TLSCAFile != ""))
	}

	opts = append(opts,
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionIdle: 0,
		}),
		grpc.MaxRecvMsgSize(8*1024*1024),
		grpc.MaxSendMsgSize(8*1024*1024),
		grpc.ChainUnaryInterceptor(loggingUnary(log), authUnaryInterceptor(auth)),
		grpc.ChainStreamInterceptor(loggingStream(log), authStreamInterceptor(auth)),
	)

	grpcServer := grpc.NewServer(opts...)

	// Register etcd services.
	etcdserverpb.RegisterKVServer(grpcServer, newKVServer(s, log))
	etcdserverpb.RegisterWatchServer(grpcServer, newWatchServer(s, log))
	etcdserverpb.RegisterLeaseServer(grpcServer, newLeaseServer(s.Leases(), log))
	etcdserverpb.RegisterClusterServer(grpcServer, newClusterServer(cfg.MemberID, cfg.ClusterID, cfg.PeerURLs, log))
	etcdserverpb.RegisterMaintenanceServer(grpcServer, newMaintenanceServer(s, cfg.MemberID, cfg.ClusterID, cfg.Version, log))
	etcdserverpb.RegisterAuthServer(grpcServer, auth)

	// Standard gRPC health check — used by Kubernetes liveness probes.
	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthSrv)

	// Reflection for grpcurl / debugging.
	reflection.Register(grpcServer)

	return &Server{grpc: grpcServer, store: s, config: cfg, log: log}, nil
}

// buildServerCreds builds gRPC server TLS credentials.
// If caFile is set, mTLS is enabled: clients must present a cert signed by that CA.
func buildServerCreds(certFile, keyFile, caFile string) (credentials.TransportCredentials, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	if caFile != "" {
		caBytes, err := os.ReadFile(caFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caBytes) {
			return nil, fmt.Errorf("failed to parse CA cert")
		}
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = pool
	}
	return credentials.NewTLS(cfg), nil
}

// Serve starts the gRPC listener and, if configured, the metrics HTTP server.
// Blocks until ctx is cancelled.
func (s *Server) Serve(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.config.ListenAddr, err)
	}
	s.listener = lis
	s.log.Info("spanner-etcd listening", zap.String("addr", lis.Addr().String()))

	// Start metrics server if configured.
	if s.config.MetricsAddr != "" {
		go s.serveMetrics(ctx)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- s.grpc.Serve(lis) }()

	select {
	case <-ctx.Done():
		s.grpc.GracefulStop()
		return nil
	case err := <-errCh:
		return err
	}
}

// serveMetrics runs a minimal HTTP server exposing /metrics and /healthz.
func (s *Server) serveMetrics(ctx context.Context) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok")) //nolint:errcheck
	})
	srv := &http.Server{Addr: s.config.MetricsAddr, Handler: mux}

	s.log.Info("metrics server listening", zap.String("addr", s.config.MetricsAddr))

	go func() {
		<-ctx.Done()
		srv.Close() //nolint:errcheck
	}()

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		s.log.Warn("metrics server error", zap.Error(err))
	}
}

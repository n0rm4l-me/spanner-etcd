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
	"sync"
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
	auth     *AuthServer
	config   Config
	log      *zap.Logger

	// listener is set by Serve and read by Addr, possibly from another goroutine
	// (tests start Serve in the background and ask for the address). mu orders the
	// two; without it the race detector rightly flags every such read.
	mu        sync.RWMutex
	listener  net.Listener
	ready     chan struct{} // closed once Serve has bound its listener
	readyOnce sync.Once
}

// Addr returns the address the server is listening on. Before Serve has bound
// its listener it returns the configured address, which for ":0" is not the real
// one; wait for Ready first when that matters.
func (s *Server) Addr() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.listener != nil {
		return s.listener.Addr().String()
	}
	return s.config.ListenAddr
}

// Ready returns a channel that is closed once Serve has bound its listener, after
// which Addr reports the real address. It is never closed if Serve fails to
// listen, so wait on it together with a timeout or Serve's error.
func (s *Server) Ready() <-chan struct{} { return s.ready }

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

	return &Server{grpc: grpcServer, store: s, auth: auth, config: cfg, log: log, ready: make(chan struct{})}, nil
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
	s.mu.Lock()
	s.listener = lis
	s.mu.Unlock()
	s.readyOnce.Do(func() { close(s.ready) })
	s.log.Info("spanner-etcd listening", zap.String("addr", lis.Addr().String()))

	// Start metrics server if configured.
	if s.config.MetricsAddr != "" {
		go s.serveMetrics(ctx)
	}

	errCh := make(chan error, 1)
	go func() { errCh <- s.grpc.Serve(lis) }()

	defer s.auth.close()

	select {
	case <-ctx.Done():
		s.log.Info("shutting down — draining connections")
		// GracefulStop waits for active RPCs to finish.
		// We give it 10 seconds; Watch streams will be terminated by context
		// cancellation, so in-flight KV/Lease RPCs are the only ones that need time.
		graceDone := make(chan struct{})
		go func() {
			s.grpc.GracefulStop()
			close(graceDone)
		}()
		select {
		case <-graceDone:
			s.log.Info("graceful shutdown complete")
		case <-time.After(10 * time.Second):
			s.log.Warn("graceful shutdown timed out — forcing stop")
			s.grpc.Stop()
		}
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

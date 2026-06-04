// spanner-etcd: a Kubernetes-native etcd v3 API server backed by Google Cloud Spanner.
//
// Usage:
//
//	spanner-etcd \
//	  --listen-address=0.0.0.0:2379 \
//	  --spanner-database=projects/my-project/instances/my-instance/databases/k8s \
//	  --log-level=info
//
// Environment variables:
//
//	SPANNER_EMULATOR_HOST=localhost:9010   use the local Spanner emulator
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"cloud.google.com/go/spanner"
	spanneradmin "cloud.google.com/go/spanner/admin/database/apiv1"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/n0rm4l-me/spanner-etcd/pkg/schema"
	"github.com/n0rm4l-me/spanner-etcd/pkg/server"
	"github.com/n0rm4l-me/spanner-etcd/pkg/store"
)

func main() {
	cfg := parseFlags()

	log, err := buildLogger(cfg.logLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to build logger: %v\n", err)
		os.Exit(1)
	}
	defer log.Sync() //nolint:errcheck

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, log); err != nil {
		log.Fatal("fatal error", zap.Error(err))
	}
	log.Info("shutdown complete")
}

// run is the main entry point, separated for testability.
func run(ctx context.Context, cfg appConfig, log *zap.Logger) error {
	log.Info("starting spanner-etcd",
		zap.String("database", cfg.spannerDatabase),
		zap.String("listen", cfg.listenAddr),
	)

	// ── Schema setup ──────────────────────────────────────────────────────────
	adminClient, err := spanneradmin.NewDatabaseAdminClient(ctx)
	if err != nil {
		return fmt.Errorf("create spanner admin client: %w", err)
	}
	defer adminClient.Close()

	if err := schema.Ensure(ctx, adminClient, cfg.spannerDatabase, log); err != nil {
		return fmt.Errorf("schema: %w", err)
	}

	// ── Spanner client ────────────────────────────────────────────────────────
	spannerClient, err := spanner.NewClientWithConfig(ctx, cfg.spannerDatabase,
		spanner.ClientConfig{
			NumChannels: cfg.spannerChannels,
			SessionPoolConfig: spanner.SessionPoolConfig{
				MinOpened:          cfg.spannerMinSessions,
				MaxOpened:          cfg.spannerMaxSessions,
				WriteSessions:      0.5,
				HealthCheckWorkers: 10,
			},
			// DisableNativeMetrics suppresses the "monitoring.timeSeries.create
			// denied" noise from Spanner's built-in client-side metrics.
			// Enable with --spanner-native-metrics if roles/monitoring.metricWriter
			// is granted (useful for Spanner client dashboards in Cloud Monitoring).
			DisableNativeMetrics: !cfg.spannerNativeMetrics,
		},
	)
	if err != nil {
		return fmt.Errorf("create spanner client: %w", err)
	}
	defer spannerClient.Close()

	if err := schema.SeedRevCounter(ctx, spannerClient); err != nil {
		return fmt.Errorf("seed revision counter: %w", err)
	}

	// ── Store ─────────────────────────────────────────────────────────────────
	kvStore, err := store.New(ctx, spannerClient, log)
	if err != nil {
		return fmt.Errorf("create store: %w", err)
	}
	defer kvStore.Close()

	// ── gRPC server ───────────────────────────────────────────────────────────
	srv, err := server.New(ctx, kvStore, server.Config{
		ListenAddr:  cfg.listenAddr,
		MetricsAddr: cfg.metricsAddr,
		TLSCert:     cfg.tlsCert,
		TLSKey:      cfg.tlsKey,
		TLSCAFile:   cfg.tlsCA,
		PeerURLs:    cfg.peerURLs,
	}, log)
	if err != nil {
		return fmt.Errorf("create server: %w", err)
	}

	return srv.Serve(ctx)
}

// ─── Config ───────────────────────────────────────────────────────────────────

type appConfig struct {
	listenAddr           string
	metricsAddr          string
	spannerDatabase      string
	spannerChannels      int
	spannerMinSessions   uint64
	spannerMaxSessions   uint64
	spannerNativeMetrics bool // enable Spanner built-in client-side metrics
	tlsCert              string
	tlsKey               string
	tlsCA                string
	peerURLs             []string
	logLevel             string
}

func parseFlags() appConfig {
	cfg := appConfig{
		listenAddr:         envOr("LISTEN_ADDR", "0.0.0.0:2379"),
		metricsAddr:        envOr("METRICS_ADDR", "0.0.0.0:2381"),
		spannerDatabase:    envOr("SPANNER_DATABASE", ""),
		spannerChannels:    4,
		spannerMinSessions: 10,
		spannerMaxSessions: 100,
		tlsCert:            envOr("TLS_CERT", ""),
		tlsKey:             envOr("TLS_KEY", ""),
		tlsCA:              envOr("TLS_CA", ""),
		peerURLs:           splitCSV(envOr("PEER_URLS", "")),
		logLevel:           envOr("LOG_LEVEL", "info"),
	}

	// Basic flag parsing (no external flag lib dependency).
	for i, arg := range os.Args[1:] {
		switch {
		case strings.HasPrefix(arg, "--listen-address="):
			cfg.listenAddr = strings.TrimPrefix(arg, "--listen-address=")
		case strings.HasPrefix(arg, "--metrics-addr="):
			cfg.metricsAddr = strings.TrimPrefix(arg, "--metrics-addr=")
		case arg == "--spanner-native-metrics":
			cfg.spannerNativeMetrics = true
		case strings.HasPrefix(arg, "--spanner-database="):
			cfg.spannerDatabase = strings.TrimPrefix(arg, "--spanner-database=")
		case strings.HasPrefix(arg, "--tls-cert="):
			cfg.tlsCert = strings.TrimPrefix(arg, "--tls-cert=")
		case strings.HasPrefix(arg, "--tls-key="):
			cfg.tlsKey = strings.TrimPrefix(arg, "--tls-key=")
		case strings.HasPrefix(arg, "--tls-ca="):
			cfg.tlsCA = strings.TrimPrefix(arg, "--tls-ca=")
		case strings.HasPrefix(arg, "--log-level="):
			cfg.logLevel = strings.TrimPrefix(arg, "--log-level=")
		case strings.HasPrefix(arg, "--peer-urls="):
			cfg.peerURLs = splitCSV(strings.TrimPrefix(arg, "--peer-urls="))
		}
		_ = i
	}

	if cfg.spannerDatabase == "" {
		fmt.Fprintln(os.Stderr, "error: --spanner-database is required (e.g. projects/P/instances/I/databases/D)")
		os.Exit(1)
	}
	return cfg
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, ",")
}

// buildLogger creates a production JSON logger (Kubernetes-native).
func buildLogger(level string) (*zap.Logger, error) {
	var l zapcore.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = zapcore.InfoLevel
	}
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(l)
	cfg.EncoderConfig.TimeKey = "ts"
	cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	cfg.EncoderConfig.MessageKey = "msg"
	cfg.EncoderConfig.LevelKey = "level"
	cfg.EncoderConfig.CallerKey = "caller"
	return cfg.Build(zap.AddCallerSkip(0))
}

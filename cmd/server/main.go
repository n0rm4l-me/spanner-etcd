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
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	spanneradmin "cloud.google.com/go/spanner/admin/database/apiv1"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/n0rm4l-me/spanner-etcd/pkg/schema"
	"github.com/n0rm4l-me/spanner-etcd/pkg/server"
	"github.com/n0rm4l-me/spanner-etcd/pkg/store"
)

func main() {
	cfg, err := parseConfig(os.Args[1:], os.Getenv, os.Stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(2)
	}

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
	spannerCfg := spanner.ClientConfig{
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
	}
	if cfg.spannerReadLocation != "" {
		spannerCfg.DirectedReadOptions = &sppb.DirectedReadOptions{
			Replicas: &sppb.DirectedReadOptions_IncludeReplicas_{
				IncludeReplicas: &sppb.DirectedReadOptions_IncludeReplicas{
					ReplicaSelections: []*sppb.DirectedReadOptions_ReplicaSelection{
						{Location: cfg.spannerReadLocation},
					},
				},
			},
		}
		log.Info("directed reads enabled", zap.String("location", cfg.spannerReadLocation))
	}
	spannerClient, err := spanner.NewClientWithConfig(ctx, cfg.spannerDatabase, spannerCfg)
	if err != nil {
		return fmt.Errorf("create spanner client: %w", err)
	}
	defer spannerClient.Close()

	if err := schema.SeedRevCounter(ctx, spannerClient); err != nil {
		return fmt.Errorf("seed revision counter: %w", err)
	}

	// ── Store ─────────────────────────────────────────────────────────────────
	kvStore, err := store.NewWithConfig(ctx, spannerClient, log, store.StoreConfig{
		AutoCompactInterval: cfg.autoCompactInterval,
		AutoCompactAge:      cfg.autoCompactAge,
	})
	if err != nil {
		return fmt.Errorf("create store: %w", err)
	}
	defer kvStore.Close()

	// ── gRPC server ───────────────────────────────────────────────────────────
	srv, err := server.New(ctx, kvStore, server.Config{
		ListenAddr:   cfg.listenAddr,
		MetricsAddr:  cfg.metricsAddr,
		TLSCert:      cfg.tlsCert,
		TLSKey:       cfg.tlsKey,
		TLSCAFile:    cfg.tlsCA,
		AuthUsers:    cfg.authUsers,
		AuthTokenTTL: cfg.authTokenTTL,
		PeerURLs:     cfg.peerURLs,
	}, log)
	if err != nil {
		return fmt.Errorf("create server: %w", err)
	}

	return srv.Serve(ctx)
}

// buildLogger creates a production JSON logger compatible with Google Cloud Logging.
//
// GCP structured logging expects:
//   - "severity"  for log level  (not "level")
//   - "message"   for the text   (not "msg")
//   - "time"      for timestamp  (ISO8601)
//
// This makes logs automatically parsed by Cloud Logging / GKE without
// a custom parser, and enables severity-based filtering in Cloud Console.
func buildLogger(level string) (*zap.Logger, error) {
	var l zapcore.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		l = zapcore.InfoLevel
	}
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(l)

	// Google Cloud Logging field names.
	cfg.EncoderConfig.TimeKey = "time"
	cfg.EncoderConfig.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	cfg.EncoderConfig.MessageKey = "message"
	cfg.EncoderConfig.LevelKey = "severity"
	cfg.EncoderConfig.CallerKey = "caller"

	// Map zap levels to GCP severity strings.
	// GCP accepts: DEBUG, INFO, NOTICE, WARNING, ERROR, CRITICAL, ALERT, EMERGENCY
	cfg.EncoderConfig.EncodeLevel = gcpLevelEncoder

	return cfg.Build(zap.AddCallerSkip(0))
}

// gcpLevelEncoder maps zap levels to Google Cloud Logging severity strings.
func gcpLevelEncoder(l zapcore.Level, enc zapcore.PrimitiveArrayEncoder) {
	switch l {
	case zapcore.DebugLevel:
		enc.AppendString("DEBUG")
	case zapcore.InfoLevel:
		enc.AppendString("INFO")
	case zapcore.WarnLevel:
		enc.AppendString("WARNING")
	case zapcore.ErrorLevel:
		enc.AppendString("ERROR")
	case zapcore.DPanicLevel, zapcore.PanicLevel:
		enc.AppendString("CRITICAL")
	case zapcore.FatalLevel:
		enc.AppendString("CRITICAL")
	default:
		enc.AppendString("DEFAULT")
	}
}

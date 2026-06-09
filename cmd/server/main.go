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
	"time"

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

// ─── Config ───────────────────────────────────────────────────────────────────

type appConfig struct {
	listenAddr           string
	metricsAddr          string
	spannerDatabase      string
	spannerChannels      int
	spannerMinSessions   uint64
	spannerMaxSessions   uint64
	spannerNativeMetrics bool
	spannerReadLocation  string // e.g. "us-east1" for directed reads
	tlsCert              string
	tlsKey               string
	tlsCA                string
	authUsers            string        // "user1:pass1,user2:pass2" — from env ETCD_AUTH_USERS
	authTokenTTL         time.Duration // 0 = DefaultTokenTTL (5m)
	peerURLs             []string
	logLevel             string
	autoCompactInterval  time.Duration // 0 = unset (use default 5m); any negative = disabled
	autoCompactAge       time.Duration // 0 = DefaultAutoCompactAge (5m)
}

func parseFlags() appConfig {
	cfg := appConfig{
		listenAddr:         envOr("LISTEN_ADDR", "0.0.0.0:2379"),
		metricsAddr:        envOr("METRICS_ADDR", "0.0.0.0:2381"),
		authUsers:          envOr("ETCD_AUTH_USERS", ""),
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
		case strings.HasPrefix(arg, "--spanner-read-location="):
			val := strings.TrimSpace(strings.TrimPrefix(arg, "--spanner-read-location="))
			if val == "" {
				fmt.Fprintln(os.Stderr, "error: --spanner-read-location requires a non-empty GCP region (e.g. us-east1)")
				os.Exit(1)
			}
			cfg.spannerReadLocation = val
		case strings.HasPrefix(arg, "--spanner-database="):
			cfg.spannerDatabase = strings.TrimPrefix(arg, "--spanner-database=")
		case strings.HasPrefix(arg, "--tls-cert="):
			cfg.tlsCert = strings.TrimPrefix(arg, "--tls-cert=")
		case strings.HasPrefix(arg, "--tls-key="):
			cfg.tlsKey = strings.TrimPrefix(arg, "--tls-key=")
		case strings.HasPrefix(arg, "--tls-ca="):
			cfg.tlsCA = strings.TrimPrefix(arg, "--tls-ca=")
		case strings.HasPrefix(arg, "--auth-users="):
			cfg.authUsers = strings.TrimPrefix(arg, "--auth-users=")
		case strings.HasPrefix(arg, "--auth-token-ttl="):
			d, err := time.ParseDuration(strings.TrimPrefix(arg, "--auth-token-ttl="))
			if err == nil {
				cfg.authTokenTTL = d
			}
		case strings.HasPrefix(arg, "--log-level="):
			cfg.logLevel = strings.TrimPrefix(arg, "--log-level=")
		case strings.HasPrefix(arg, "--peer-urls="):
			cfg.peerURLs = splitCSV(strings.TrimPrefix(arg, "--peer-urls="))
		case strings.HasPrefix(arg, "--auto-compact-interval="):
			val := strings.TrimPrefix(arg, "--auto-compact-interval=")
			if val == "0" || val == "off" || val == "disable" {
				cfg.autoCompactInterval = -1 // sentinel: disabled
			} else if d, err := time.ParseDuration(val); err != nil {
				fmt.Fprintf(os.Stderr, "error: invalid --auto-compact-interval=%q: %v\n", val, err)
				os.Exit(1)
			} else if d == 0 {
				cfg.autoCompactInterval = -1 // "0s" also disables
			} else {
				cfg.autoCompactInterval = d
			}
		case strings.HasPrefix(arg, "--auto-compact-age="):
			val := strings.TrimPrefix(arg, "--auto-compact-age=")
			d, err := time.ParseDuration(val)
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: invalid --auto-compact-age=%q: %v\n", val, err)
				os.Exit(1)
			}
			cfg.autoCompactAge = d
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

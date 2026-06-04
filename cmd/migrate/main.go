// migrate copies all keys from a source etcd to a spanner-etcd destination
// and verifies data integrity by comparing every key-value pair.
//
// Usage:
//
//	migrate \
//	  --src=http://localhost:2379 \
//	  --src-user=root \
//	  --src-password=secret \
//	  --dst=https://spanner-etcd:2379 \
//	  --dst-cacert=ca.crt \
//	  --dst-cert=client.crt \
//	  --dst-key=client.key \
//	  [--batch=500] \
//	  [--verify] \
//	  [--dry-run]
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type config struct {
	src         string
	srcUser     string
	srcPassword string
	dst         string
	dstCACert   string
	dstCert     string
	dstKey      string
	batchSize   int
	verify      bool
	dryRun      bool
	logLevel    string
}

func main() {
	cfg := parseFlags()

	log, _ := buildLogger(cfg.logLevel)
	defer log.Sync() //nolint

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, cfg, log); err != nil {
		log.Fatal("migration failed", zap.Error(err))
	}
}

func run(ctx context.Context, cfg config, log *zap.Logger) error {
	// ── Connect to source ──────────────────────────────────────────────────────
	log.Info("connecting to source etcd", zap.String("endpoint", cfg.src))
	src, err := newClient(cfg.src, cfg.srcUser, cfg.srcPassword, "", "", "")
	if err != nil {
		return fmt.Errorf("source client: %w", err)
	}
	defer src.Close()

	// ── Connect to destination ─────────────────────────────────────────────────
	log.Info("connecting to destination spanner-etcd", zap.String("endpoint", cfg.dst))
	dst, err := newClient(cfg.dst, "", "", cfg.dstCACert, cfg.dstCert, cfg.dstKey)
	if err != nil {
		return fmt.Errorf("destination client: %w", err)
	}
	defer dst.Close()

	// ── Stream keys from source in pages ──────────────────────────────────────
	log.Info("starting migration (streaming pages)...")

	var (
		migrated int
		failed   int
		start    = time.Now()
		lastKey  = ""
		pageSize = int64(cfg.batchSize)
	)

	for {
		var opts []clientv3.OpOption
		opts = append(opts, clientv3.WithFromKey(), clientv3.WithLimit(pageSize))
		if lastKey == "" {
			opts[0] = clientv3.WithFromKey()
		}

		fromKey := lastKey
		if fromKey == "" {
			fromKey = "\x00"
		}
		resp, err := src.Get(ctx, fromKey, clientv3.WithFromKey(), clientv3.WithLimit(pageSize))
		if err != nil {
			return fmt.Errorf("read source page: %w", err)
		}
		if len(resp.Kvs) == 0 {
			break
		}

		if cfg.dryRun && migrated == 0 {
			log.Info("dry-run mode — showing first 10 keys")
			n := 10
			if len(resp.Kvs) < n {
				n = len(resp.Kvs)
			}
			printSample(resp.Kvs[:n], n, log)
			return nil
		}

		for _, kv := range resp.Kvs {
			key := string(kv.Key)
			_, err := dst.Put(ctx, key, string(kv.Value))
			if err != nil {
				log.Warn("put failed", zap.String("key", key), zap.Error(err))
				failed++
			} else {
				migrated++
			}
		}

		elapsed := time.Since(start)
		rate := float64(migrated) / elapsed.Seconds()
		log.Info("progress",
			zap.Int("migrated", migrated),
			zap.Int("failed", failed),
			zap.Float64("rate_per_sec", rate),
		)

		// If we got fewer than pageSize, we're done.
		if int64(len(resp.Kvs)) < pageSize {
			break
		}
		// Next page starts after the last key.
		lastKey = string(resp.Kvs[len(resp.Kvs)-1].Key) + "\x00"
	}
	skipped := 0

	log.Info("migration complete",
		zap.Int("migrated", migrated),
		zap.Int("failed", failed),
		zap.Int("skipped", skipped), //nolint
		zap.Duration("elapsed", time.Since(start)),
	)

	if failed > 0 {
		return fmt.Errorf("%d keys failed to migrate", failed)
	}

	// ── Verify ─────────────────────────────────────────────────────────────────
	if cfg.verify {
		return verify(ctx, src, dst, log)
	}
	return nil
}

// verify reads every key from source and compares with destination.
func verify(ctx context.Context, src, dst *clientv3.Client, log *zap.Logger) error {
	log.Info("verifying data integrity...")

	srcResp, err := src.Get(ctx, "", clientv3.WithFromKey(), clientv3.WithLimit(0))
	if err != nil {
		return fmt.Errorf("verify read source: %w", err)
	}

	var (
		ok      int
		missing int
		wrong   int
	)

	for _, srcKV := range srcResp.Kvs {
		key := string(srcKV.Key)
		dstResp, err := dst.Get(ctx, key)
		if err != nil {
			log.Warn("verify get failed", zap.String("key", key), zap.Error(err))
			missing++
			continue
		}
		if len(dstResp.Kvs) == 0 {
			log.Warn("key missing in destination", zap.String("key", key))
			missing++
			continue
		}
		if string(dstResp.Kvs[0].Value) != string(srcKV.Value) {
			log.Warn("value mismatch",
				zap.String("key", key),
				zap.Int("src_len", len(srcKV.Value)),
				zap.Int("dst_len", len(dstResp.Kvs[0].Value)),
			)
			wrong++
			continue
		}
		ok++
	}

	log.Info("verification complete",
		zap.Int("ok", ok),
		zap.Int("missing", missing),
		zap.Int("wrong_value", wrong),
		zap.Int("total", len(srcResp.Kvs)),
	)

	if missing > 0 || wrong > 0 {
		return fmt.Errorf("verification failed: %d missing, %d wrong value", missing, wrong)
	}

	log.Info("✓ all keys match")
	return nil
}

// ── helpers ───────────────────────────────────────────────────────────────────

func newClient(endpoint, user, password, cacert, cert, key string) (*clientv3.Client, error) {
	var dialOpts []grpc.DialOption

	if cacert != "" || cert != "" {
		tlsCfg := &tls.Config{MinVersion: tls.VersionTLS12}
		if cacert != "" {
			caBytes, err := os.ReadFile(cacert)
			if err != nil {
				return nil, fmt.Errorf("read ca: %w", err)
			}
			pool := x509.NewCertPool()
			pool.AppendCertsFromPEM(caBytes)
			tlsCfg.RootCAs = pool
		}
		if cert != "" && key != "" {
			pair, err := tls.LoadX509KeyPair(cert, key)
			if err != nil {
				return nil, fmt.Errorf("load keypair: %w", err)
			}
			tlsCfg.Certificates = []tls.Certificate{pair}
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	cfg := clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 10 * time.Second,
		DialOptions: dialOpts,
	}
	if user != "" {
		cfg.Username = user
		cfg.Password = password
	}

	return clientv3.New(cfg)
}

func printSample(kvs []*mvccpb.KeyValue, n int, log *zap.Logger) {
	if len(kvs) < n {
		n = len(kvs)
	}
	log.Info("sample keys (first N)", zap.Int("n", n))
	for _, kv := range kvs[:n] {
		valPreview := string(kv.Value)
		if len(valPreview) > 80 {
			valPreview = valPreview[:80] + "..."
		}
		log.Info("key",
			zap.String("key", string(kv.Key)),
			zap.Int("value_bytes", len(kv.Value)),
			zap.String("value_preview", valPreview),
		)
	}
}

func buildLogger(level string) (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	cfg.EncoderConfig.TimeKey = "ts"
	return cfg.Build()
}

// ── flag parsing ──────────────────────────────────────────────────────────────

func parseFlags() config {
	cfg := config{
		src:       envOr("SRC_ENDPOINT", "http://localhost:2379"),
		dst:       envOr("DST_ENDPOINT", "http://localhost:12379"),
		batchSize: 500,
		verify:    true,
		logLevel:  "info",
	}

	for _, arg := range os.Args[1:] {
		switch {
		case strings.HasPrefix(arg, "--src="):
			cfg.src = strings.TrimPrefix(arg, "--src=")
		case strings.HasPrefix(arg, "--src-user="):
			cfg.srcUser = strings.TrimPrefix(arg, "--src-user=")
		case strings.HasPrefix(arg, "--src-password="):
			cfg.srcPassword = strings.TrimPrefix(arg, "--src-password=")
		case strings.HasPrefix(arg, "--dst="):
			cfg.dst = strings.TrimPrefix(arg, "--dst=")
		case strings.HasPrefix(arg, "--dst-cacert="):
			cfg.dstCACert = strings.TrimPrefix(arg, "--dst-cacert=")
		case strings.HasPrefix(arg, "--dst-cert="):
			cfg.dstCert = strings.TrimPrefix(arg, "--dst-cert=")
		case strings.HasPrefix(arg, "--dst-key="):
			cfg.dstKey = strings.TrimPrefix(arg, "--dst-key=")
		case arg == "--verify":
			cfg.verify = true
		case arg == "--no-verify":
			cfg.verify = false
		case arg == "--dry-run":
			cfg.dryRun = true
		}
	}
	return cfg
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

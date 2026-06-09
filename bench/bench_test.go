// Package bench contains benchmarks for spanner-etcd.
//
// Run against the Spanner emulator:
//
//	SPANNER_EMULATOR_HOST=localhost:9010 \
//	go test ./bench/... -bench=. -benchtime=10s -benchmem
//
// Run against a real Spanner instance (set SPANNER_DATABASE):
//
//	SPANNER_DATABASE=projects/P/instances/I/databases/D \
//	go test ./bench/... -bench=. -benchtime=30s -benchmem
package bench

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	sppb "cloud.google.com/go/spanner/apiv1/spannerpb"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"go.uber.org/zap"

	"github.com/n0rm4l-me/spanner-etcd/pkg/schema"
	"github.com/n0rm4l-me/spanner-etcd/pkg/store"
)

// ── Setup ─────────────────────────────────────────────────────────────────────

var (
	benchStore *store.Store
	benchOnce  sync.Once
)

func getStore(b *testing.B) *store.Store {
	b.Helper()
	benchOnce.Do(func() {
		s, err := setupStore(b)
		if err != nil {
			b.Fatalf("setup store: %v", err)
		}
		benchStore = s
	})
	return benchStore
}

func setupStore(b *testing.B) (*store.Store, error) {
	b.Helper()
	ctx := context.Background()

	dbPath := os.Getenv("SPANNER_DATABASE")
	if dbPath == "" {
		os.Setenv("SPANNER_EMULATOR_HOST", "localhost:9010")
		dbPath = setupEmulatorDB(b, ctx)
	}

	spannerCfg := spanner.ClientConfig{}
	if loc := strings.TrimSpace(os.Getenv("SPANNER_READ_LOCATION")); loc != "" {
		spannerCfg.DirectedReadOptions = &sppb.DirectedReadOptions{
			Replicas: &sppb.DirectedReadOptions_IncludeReplicas_{
				IncludeReplicas: &sppb.DirectedReadOptions_IncludeReplicas{
					ReplicaSelections: []*sppb.DirectedReadOptions_ReplicaSelection{
						{Location: loc},
					},
				},
			},
		}
		b.Logf("Directed reads enabled: %s", loc)
	}
	client, err := spanner.NewClientWithConfig(ctx, dbPath, spannerCfg)
	if err != nil {
		return nil, fmt.Errorf("spanner client: %w", err)
	}

	// Retry SeedRevCounter — first request to production Spanner may timeout
	// on cold start or after a long idle period.
	for i := 0; i < 5; i++ {
		if err := schema.SeedRevCounter(ctx, client); err == nil {
			break
		} else if i == 4 {
			client.Close()
			return nil, fmt.Errorf("seed rev: %w", err)
		}
		time.Sleep(time.Duration(i+1) * 2 * time.Second)
	}

	log := zap.NewNop()
	s, err := store.New(ctx, client, log)
	if err != nil {
		client.Close()
		return nil, err
	}

	// Warmup: fire concurrent reads to pre-populate the Spanner session pool
	// before benchmarks start. Without this, the first few ops are slow as
	// sessions are lazily created.
	b.Log("Warming up Spanner session pool...")
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.CurrentRevision(ctx) //nolint:errcheck
		}()
	}
	wg.Wait()
	time.Sleep(2 * time.Second)
	b.Log("Warmup done")
	return s, nil
}

func setupEmulatorDB(b *testing.B, ctx context.Context) string {
	b.Helper()

	proj, inst, dbName := "bench-project", "bench-instance", "bench-db"

	// Ensure instance.
	ic, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		b.Fatalf("instance admin: %v", err)
	}
	defer ic.Close()

	ipath := fmt.Sprintf("projects/%s/instances/%s", proj, inst)
	if _, err := ic.GetInstance(ctx, &instancepb.GetInstanceRequest{Name: ipath}); err != nil {
		op, err := ic.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
			Parent:     fmt.Sprintf("projects/%s", proj),
			InstanceId: inst,
			Instance: &instancepb.Instance{
				Config:      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", proj),
				DisplayName: "bench",
				NodeCount:   1,
			},
		})
		if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
			b.Fatalf("create instance: %v", err)
		}
		if op != nil {
			op.Wait(ctx) //nolint
		}
	}

	// Create/recreate database for clean state.
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", proj, inst, dbName)
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		b.Fatalf("db admin: %v", err)
	}
	defer adminClient.Close()

	adminClient.DropDatabase(ctx, &databasepb.DropDatabaseRequest{Database: dbPath}) //nolint
	op, err := adminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", proj, inst),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", dbName),
	})
	if err != nil && !strings.Contains(err.Error(), "AlreadyExists") {
		b.Fatalf("create database: %v", err)
	}
	if op != nil {
		op.Wait(ctx) //nolint
	}

	log := zap.NewNop()
	if err := schema.Ensure(ctx, adminClient, dbPath, log); err != nil {
		b.Fatalf("schema: %v", err)
	}

	return dbPath
}

// ── Write benchmarks ──────────────────────────────────────────────────────────

// BenchmarkCreate measures single-goroutine create throughput.
func BenchmarkCreate(b *testing.B) {
	s := getStore(b)
	ctx := context.Background()
	// Use nanosecond-unique prefix so re-runs don't collide.
	prefix := fmt.Sprintf("/bench/create/%d/", time.Now().UnixNano())
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("%s%d", prefix, i)
		if _, err := s.Create(ctx, key, []byte("value"), 0); err != nil {
			b.Fatalf("create: %v", err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkCreate_Parallel measures concurrent write throughput.
// This stress-tests the kv_rev counter serialization.
func BenchmarkCreate_Parallel(b *testing.B) {
	s := getStore(b)
	ctx := context.Background()
	prefix := fmt.Sprintf("/bench/parallel/%d/", time.Now().UnixNano())
	var counter atomic.Int64
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			n := counter.Add(1)
			key := fmt.Sprintf("%s%d", prefix, n)
			if _, err := s.Create(ctx, key, []byte("value"), 0); err != nil {
				b.Errorf("create: %v", err)
			}
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkUpdate measures single-key update throughput (CAS pattern).
func BenchmarkUpdate(b *testing.B) {
	s := getStore(b)
	ctx := context.Background()

	rev, _ := s.Create(ctx, "/bench/update/key", []byte("initial"), 0)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		newRev, _, _, err := s.Update(ctx, "/bench/update/key", []byte("v"), rev, 0)
		if err != nil {
			b.Fatalf("update i=%d: %v", i, err)
		}
		rev = newRev
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// ── Read benchmarks ───────────────────────────────────────────────────────────

// BenchmarkGet measures single-key read latency.
func BenchmarkGet(b *testing.B) {
	s := getStore(b)
	ctx := context.Background()

	s.Create(ctx, "/bench/get/key", []byte("value"), 0)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, _, err := s.Get(ctx, "/bench/get/key", 0); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkGet_Parallel measures concurrent read throughput.
// Reads are served by Spanner strong reads — fully parallel, no coordination.
func BenchmarkGet_Parallel(b *testing.B) {
	s := getStore(b)
	ctx := context.Background()

	s.Create(ctx, "/bench/getpar/key", []byte("value"), 0)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, _, err := s.Get(ctx, "/bench/getpar/key", 0); err != nil {
				b.Errorf("get: %v", err)
			}
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkList_100 measures prefix scan of 100 keys.
func BenchmarkList_100(b *testing.B) {
	s := getStore(b)
	ctx := context.Background()

	// Pre-populate 100 keys.
	for i := 0; i < 100; i++ {
		s.Create(ctx, fmt.Sprintf("/bench/list/%04d", i), []byte("v"), 0)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, _, kvs, err := s.List(ctx, "/bench/list/", "/bench/list/", 0, 0)
		if err != nil {
			b.Fatalf("list: %v", err)
		}
		if len(kvs) != 100 {
			b.Fatalf("want 100 keys, got %d", len(kvs))
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// ── Mixed workload ─────────────────────────────────────────────────────────────

// BenchmarkMixed simulates a Kubernetes-like workload:
// 70% reads, 20% creates, 10% updates — matches API server traffic patterns.
func BenchmarkMixed(b *testing.B) {
	s := getStore(b)
	ctx := context.Background()

	// Pre-populate some keys.
	for i := 0; i < 50; i++ {
		s.Create(ctx, fmt.Sprintf("/bench/mixed/%04d", i), []byte("v"), 0)
	}

	var (
		reads   atomic.Int64
		writes  atomic.Int64
		updates atomic.Int64
		errors  atomic.Int64
		counter atomic.Int64
	)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		n := counter.Add(1)
		i := int(n)
		for pb.Next() {
			i++
			op := i % 10
			switch {
			case op < 7: // 70% reads
				_, _, err := s.Get(ctx, fmt.Sprintf("/bench/mixed/%04d", i%50), 0)
				if err != nil {
					errors.Add(1)
				} else {
					reads.Add(1)
				}
			case op < 9: // 20% creates
				_, err := s.Create(ctx, fmt.Sprintf("/bench/mixed/new/%d", i), []byte("v"), 0)
				if err != nil && err != store.ErrKeyExists {
					errors.Add(1)
				} else {
					writes.Add(1)
				}
			default: // 10% updates — get then update
				_, kv, err := s.Get(ctx, fmt.Sprintf("/bench/mixed/%04d", i%50), 0)
				if err != nil || kv == nil {
					continue
				}
				s.Update(ctx, kv.Key, []byte("updated"), kv.Rev, 0) //nolint
				updates.Add(1)
			}
		}
	})

	elapsed := b.Elapsed()
	total := reads.Load() + writes.Load() + updates.Load()
	b.ReportMetric(float64(total)/elapsed.Seconds(), "ops/sec")
	b.ReportMetric(float64(reads.Load()*100)/float64(total), "read%")
	b.ReportMetric(float64(errors.Load()), "errors")
}

// ── Watch latency ─────────────────────────────────────────────────────────────

// BenchmarkWatch_Latency measures end-to-end Watch delivery latency
// (time from Write commit to event received by subscriber).
func BenchmarkWatch_Latency(b *testing.B) {
	s := getStore(b)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	curRev, _ := s.CurrentRevision(ctx)
	prefix := fmt.Sprintf("/bench/watch/%d/", time.Now().UnixNano())
	ch := s.Watch(ctx, prefix, curRev)
	// Wait for Change Stream to initialize — cold start can take 3–5s on production Spanner.
	time.Sleep(5 * time.Second)

	var totalLatency time.Duration
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		t0 := time.Now()
		key := fmt.Sprintf("%s%d", prefix, i)
		s.Create(ctx, key, []byte("v"), 0)

		select {
		case events := <-ch:
			if len(events) > 0 {
				totalLatency += time.Since(t0)
			}
		case <-time.After(5 * time.Second):
			b.Fatalf("watch timeout at i=%d", i)
		}
	}

	if b.N > 0 {
		avgLatency := totalLatency / time.Duration(b.N)
		b.ReportMetric(float64(avgLatency.Milliseconds()), "ms/event")
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "events/sec")
	}
}

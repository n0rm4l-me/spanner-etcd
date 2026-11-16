package store_test

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"go.uber.org/zap"

	"github.com/n0rm4l-me/spanner-etcd/pkg/schema"
	"github.com/n0rm4l-me/spanner-etcd/pkg/store"
)

const (
	emulatorHost = "localhost:9010"
	testProject  = "test-project"
	testInstance = "test-instance"
)

// realInstance returns the path of a real Spanner instance to run the tests
// against (projects/P/instances/I), taken from SPANNER_ETCD_TEST_INSTANCE.
// Empty means: use the emulator. Real mode uses application default
// credentials and creates one database per test, so run only the tests you
// need (-run) and expect each database to take a minute or two to set up.
func realInstance() string { return os.Getenv("SPANNER_ETCD_TEST_INSTANCE") }

func init() {
	if realInstance() != "" {
		os.Unsetenv("SPANNER_EMULATOR_HOST") // make sure we do not talk to an emulator
		return
	}
	os.Setenv("SPANNER_EMULATOR_HOST", emulatorHost)
}

// newTestStore creates a fresh Spanner database on the emulator and returns
// a ready-to-use Store. The database is dropped when t.Cleanup runs.
func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	return openStore(t, newTestDB(t), store.StoreConfig{})
}

// newTestStoreWithConfig creates a Store with explicit StoreConfig for tests that
// need to tune auto-compaction or other store-level settings.
func newTestStoreWithConfig(t *testing.T, ctx context.Context, cfg store.StoreConfig) *store.Store {
	t.Helper()
	_ = ctx
	return openStore(t, newTestDB(t), cfg)
}

// newTestDB creates a fresh database with the full schema on the emulator and
// returns its path. The database is dropped when t.Cleanup runs, after every
// store opened on it via openStore has been closed (cleanups run in LIFO order).
//
// Use it together with openStore to simulate several replicas, or a replica
// restart, against one database.
func newTestDB(t *testing.T) string {
	t.Helper()

	ctx := context.Background()
	parent := fmt.Sprintf("projects/%s/instances/%s", testProject, testInstance)
	if inst := realInstance(); inst != "" {
		parent = inst
	} else {
		if !emulatorRunning() {
			t.Skip("Spanner emulator not running: start it on localhost:9010 (see docs/development.md)")
		}
		ensureInstance(t, ctx)
	}
	// Unique across tests AND across concurrently running test processes: the
	// clock has only microsecond resolution on macOS, so a timestamp alone can
	// collide. Spanner database IDs are limited to 30 characters.
	dbName := fmt.Sprintf("test-%d-%04x", time.Now().UnixMilli(), rand.Intn(1<<16))
	dbPath := fmt.Sprintf("%s/databases/%s", parent, dbName)

	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	t.Cleanup(func() { adminClient.Close() })

	// Create the database first, then apply DDL.
	createOp, err := adminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          parent,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", dbName),
	})
	if err != nil {
		t.Fatalf("create database: %v", err)
	}
	if _, err := createOp.Wait(ctx); err != nil {
		t.Fatalf("wait create database: %v", err)
	}

	if err := schema.Ensure(ctx, adminClient, dbPath, zap.NewNop()); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	t.Cleanup(func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		if err := adminClient.DropDatabase(dropCtx, &databasepb.DropDatabaseRequest{Database: dbPath}); err != nil {
			t.Logf("DropDatabase %s: %v", dbPath, err)
		}
	})
	return dbPath
}

// openStore opens an independent Store (its own Spanner client, watcher and
// lease manager) on an existing database, as a separate replica would. It is
// closed on t.Cleanup; tests may also Close it earlier to simulate a replica
// going away (Store.Close is idempotent).
func openStore(t *testing.T, dbPath string, cfg store.StoreConfig) *store.Store {
	t.Helper()
	ctx := context.Background()

	spannerClient, err := spanner.NewClient(ctx, dbPath)
	if err != nil {
		t.Fatalf("spanner client: %v", err)
	}

	if err := schema.SeedRevCounter(ctx, spannerClient); err != nil {
		t.Fatalf("seed rev counter: %v", err)
	}

	s, err := store.NewWithConfig(ctx, spannerClient, zap.NewNop(), cfg)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	t.Cleanup(func() {
		s.Close()
		spannerClient.Close()
	})
	return s
}

func ensureInstance(t *testing.T, ctx context.Context) {
	t.Helper()
	ic, err := instance.NewInstanceAdminClient(ctx)
	if err != nil {
		t.Fatalf("instance admin client: %v", err)
	}
	defer ic.Close()

	instPath := fmt.Sprintf("projects/%s/instances/%s", testProject, testInstance)
	if _, err := ic.GetInstance(ctx, &instancepb.GetInstanceRequest{Name: instPath}); err == nil {
		return
	}

	op, err := ic.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", testProject),
		InstanceId: testInstance,
		Instance: &instancepb.Instance{
			Config:      fmt.Sprintf("projects/%s/instanceConfigs/emulator-config", testProject),
			DisplayName: "test",
			NodeCount:   1,
		},
	})
	if err != nil {
		if strings.Contains(err.Error(), "AlreadyExists") {
			return
		}
		t.Fatalf("create instance: %v", err)
	}
	if _, err := op.Wait(ctx); err != nil {
		t.Fatalf("wait create instance: %v", err)
	}
}

func emulatorRunning() bool {
	resp, err := http.Get(fmt.Sprintf("http://%s", strings.Replace(emulatorHost, "9010", "9020", 1)))
	if err != nil {
		return false
	}
	resp.Body.Close()
	return true
}

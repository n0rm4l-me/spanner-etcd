package store_test

import (
	"context"
	"fmt"
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

func init() {
	os.Setenv("SPANNER_EMULATOR_HOST", emulatorHost)
}

// newTestStore creates a fresh Spanner database on the emulator and returns
// a ready-to-use Store. The database is dropped when t.Cleanup runs.
func newTestStore(t *testing.T) *store.Store {
	t.Helper()

	if !emulatorRunning() {
		t.Skip("Spanner emulator not running — set SPANNER_EMULATOR_HOST or start it")
	}

	ctx := context.Background()
	dbName := fmt.Sprintf("test-%d", time.Now().UnixNano())
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s",
		testProject, testInstance, dbName)

	// Ensure instance exists.
	ensureInstance(t, ctx)

	// Create database.
	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	t.Cleanup(func() { adminClient.Close() })

	// Create the database first, then apply DDL.
	createOp, err := adminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", testProject, testInstance),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", dbName),
	})
	if err != nil {
		t.Fatalf("create database: %v", err)
	}
	if _, err := createOp.Wait(ctx); err != nil {
		t.Fatalf("wait create database: %v", err)
	}

	log := zap.NewNop()
	if err := schema.Ensure(ctx, adminClient, dbPath, log); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	// Spanner client.
	spannerClient, err := spanner.NewClient(ctx, dbPath)
	if err != nil {
		t.Fatalf("spanner client: %v", err)
	}

	if err := schema.SeedRevCounter(ctx, spannerClient); err != nil {
		t.Fatalf("seed rev counter: %v", err)
	}

	s, err := store.New(ctx, spannerClient, log)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	t.Cleanup(func() {
		s.Close()
		spannerClient.Close()
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		if err := adminClient.DropDatabase(dropCtx, &databasepb.DropDatabaseRequest{Database: dbPath}); err != nil {
			t.Logf("DropDatabase %s: %v", dbPath, err)
		}
	})
	return s
}

// newTestStoreWithConfig creates a Store with explicit StoreConfig for tests that
// need to tune auto-compaction or other store-level settings.
func newTestStoreWithConfig(t *testing.T, ctx context.Context, cfg store.StoreConfig) *store.Store {
	t.Helper()

	if !emulatorRunning() {
		t.Skip("Spanner emulator not running — set SPANNER_EMULATOR_HOST or start it")
	}

	dbName := fmt.Sprintf("test-%d", time.Now().UnixNano())
	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s",
		testProject, testInstance, dbName)

	ensureInstance(t, ctx)

	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	t.Cleanup(func() { adminClient.Close() })

	createOp, err := adminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          fmt.Sprintf("projects/%s/instances/%s", testProject, testInstance),
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", dbName),
	})
	if err != nil {
		t.Fatalf("create database: %v", err)
	}
	if _, err := createOp.Wait(ctx); err != nil {
		t.Fatalf("wait create database: %v", err)
	}

	log := zap.NewNop()
	if err := schema.Ensure(ctx, adminClient, dbPath, log); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	spannerClient, err := spanner.NewClient(ctx, dbPath)
	if err != nil {
		t.Fatalf("spanner client: %v", err)
	}

	if err := schema.SeedRevCounter(ctx, spannerClient); err != nil {
		t.Fatalf("seed rev counter: %v", err)
	}

	s, err := store.NewWithConfig(ctx, spannerClient, log, cfg)
	if err != nil {
		t.Fatalf("new store with config: %v", err)
	}

	t.Cleanup(func() {
		s.Close()
		spannerClient.Close()
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer dropCancel()
		if err := adminClient.DropDatabase(dropCtx, &databasepb.DropDatabaseRequest{Database: dbPath}); err != nil {
			t.Logf("DropDatabase %s: %v", dbPath, err)
		}
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

package server_test

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
// Empty means: use the emulator. Real mode uses application default credentials
// and creates one database per test, so run only the tests you need (-run) and
// expect each database to take a minute or two to set up. See docs/development.md.
func realInstance() string { return os.Getenv("SPANNER_ETCD_TEST_INSTANCE") }

func init() {
	if realInstance() != "" {
		os.Unsetenv("SPANNER_EMULATOR_HOST") // make sure we do not talk to an emulator
		return
	}
	os.Setenv("SPANNER_EMULATOR_HOST", emulatorHost)
}

func newTestStore(t *testing.T) *store.Store {
	t.Helper()

	ctx := context.Background()
	parent := fmt.Sprintf("projects/%s/instances/%s", testProject, testInstance)
	if inst := realInstance(); inst != "" {
		parent = inst
	} else {
		if !emulatorRunning() {
			t.Skip("Spanner emulator not running")
		}
		ensureInstance(t, ctx)
	}
	// Unique across tests and across concurrently running test processes (the
	// clock has only microsecond resolution on macOS). Spanner database IDs are
	// limited to 30 characters.
	dbName := fmt.Sprintf("srv-%d-%04x", time.Now().UnixMilli(), rand.Intn(1<<16))
	dbPath := fmt.Sprintf("%s/databases/%s", parent, dbName)

	adminClient, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	t.Cleanup(func() { adminClient.Close() })

	log := zap.NewNop()
	createOp, err := adminClient.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          parent,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", dbName),
	})
	if err != nil {
		t.Fatalf("create database: %v", err)
	}
	// Register the drop before waiting: Spanner keeps creating the database even
	// if our wait fails (for example DeadlineExceeded on a busy instance), and a
	// cleanup registered only after a successful wait would leave it behind.
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := adminClient.DropDatabase(dropCtx, &databasepb.DropDatabaseRequest{Database: dbPath}); err != nil {
			t.Logf("DropDatabase %s: %v", dbPath, err)
		}
	})
	if _, err := createOp.Wait(ctx); err != nil {
		t.Fatalf("wait create database: %v", err)
	}

	if err := schema.Ensure(ctx, adminClient, dbPath, log); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}

	spannerClient, err := spanner.NewClient(ctx, dbPath)
	if err != nil {
		t.Fatalf("spanner client: %v", err)
	}

	if err := schema.SeedRevCounter(ctx, spannerClient); err != nil {
		t.Fatalf("seed rev: %v", err)
	}

	s, err := store.New(ctx, spannerClient, log)
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
		t.Fatalf("instance admin: %v", err)
	}
	defer ic.Close()

	path := fmt.Sprintf("projects/%s/instances/%s", testProject, testInstance)
	if _, err := ic.GetInstance(ctx, &instancepb.GetInstanceRequest{Name: path}); err == nil {
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
	op.Wait(ctx) //nolint
}

func newTestLogger(t *testing.T) *zap.Logger {
	t.Helper()
	return zap.NewNop()
}

func emulatorRunning() bool {
	resp, err := http.Get(fmt.Sprintf("http://%s", strings.Replace(emulatorHost, "9010", "9020", 1)))
	if err != nil {
		return false
	}
	resp.Body.Close()
	return true
}

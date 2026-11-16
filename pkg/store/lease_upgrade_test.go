package store_test

import (
	"context"
	"testing"
	"time"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"go.uber.org/zap"

	"github.com/n0rm4l-me/spanner-etcd/pkg/schema"
	"github.com/n0rm4l-me/spanner-etcd/pkg/store"
)

// insertLegacyLease writes a kv_lease row the way a version without
// expires_at did: the column is left NULL.
func insertLegacyLease(t *testing.T, db string, id, ttl int64) {
	t.Helper()
	ctx := context.Background()
	client, err := spanner.NewClient(ctx, db)
	if err != nil {
		t.Fatalf("spanner client: %v", err)
	}
	defer client.Close()
	if _, err := client.Apply(ctx, []*spanner.Mutation{
		spanner.Insert("kv_lease",
			[]string{"lease_id", "ttl_sec", "granted_at"},
			[]interface{}{id, ttl, spanner.CommitTimestamp}),
	}); err != nil {
		t.Fatalf("insert legacy lease: %v", err)
	}
}

// A lease row with NULL expires_at (written by an older version) must be
// treated as granted_at + ttl: it expires on schedule ...
func TestLease_LegacyRowExpiresAtGrantPlusTTL(t *testing.T) {
	db := newTestDB(t)
	s := openStore(t, db, store.StoreConfig{})
	ctx := context.Background()

	const leaseID = int64(7001)
	insertLegacyLease(t, db, leaseID, 2)
	if _, err := s.Create(ctx, "/legacy/short", []byte("v"), leaseID); err != nil {
		t.Fatalf("create: %v", err)
	}

	if !waitKeyGone(t, s, "/legacy/short", 10*time.Second) {
		t.Fatal("legacy lease (NULL expires_at, ttl 2s) did not expire")
	}
}

// ... and can be kept alive, which stores a real expires_at from then on.
func TestLease_LegacyRowCanBeKeptAlive(t *testing.T) {
	db := newTestDB(t)
	s := openStore(t, db, store.StoreConfig{})
	ctx := context.Background()

	const leaseID = int64(7002)
	insertLegacyLease(t, db, leaseID, 60)

	snap, ok, err := s.Leases().GetTTL(ctx, leaseID)
	if err != nil || !ok {
		t.Fatalf("GetTTL: ok=%v err=%v", ok, err)
	}
	if snap.TTL != 60 || snap.Remaining <= 0 || snap.Remaining > 60 {
		t.Fatalf("legacy lease snapshot out of range: %+v", snap)
	}

	ttl, err := s.Leases().Keepalive(ctx, leaseID)
	if err != nil || ttl != 60 {
		t.Fatalf("keepalive: ttl=%d err=%v", ttl, err)
	}
	snap, _, _ = s.Leases().GetTTL(ctx, leaseID)
	if snap.Remaining < 58 {
		t.Fatalf("after keepalive want ~60s remaining, got %d", snap.Remaining)
	}
}

// schema.Ensure on a database created by an older version (kv_lease without
// expires_at) must add the column and keep existing rows.
func TestSchemaEnsure_AddsExpiresAtToExistingTable(t *testing.T) {
	db := newTestDB(t)
	ctx := context.Background()

	admin, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	defer admin.Close()

	// Recreate the old shape: drop the new column, then put a row in.
	op, err := admin.UpdateDatabaseDdl(ctx, &databasepb.UpdateDatabaseDdlRequest{
		Database:   db,
		Statements: []string{`ALTER TABLE kv_lease DROP COLUMN expires_at`},
	})
	if err != nil {
		t.Fatalf("drop column: %v", err)
	}
	if err := op.Wait(ctx); err != nil {
		t.Fatalf("wait drop column: %v", err)
	}
	insertLegacyLease(t, db, 7003, 30)

	// The upgrade path: what a new replica runs at startup.
	if err := schema.Ensure(ctx, admin, db, zap.NewNop()); err != nil {
		t.Fatalf("schema.Ensure on old-shape database: %v", err)
	}

	// The column exists now (the manager's queries reference it) and the old row survived.
	s := openStore(t, db, store.StoreConfig{})
	snap, ok, err := s.Leases().GetTTL(ctx, 7003)
	if err != nil || !ok || snap.TTL != 30 {
		t.Fatalf("existing lease after upgrade: snap=%+v ok=%v err=%v", snap, ok, err)
	}
	if _, err := s.Leases().Grant(ctx, 30); err != nil {
		t.Fatalf("grant after upgrade: %v", err)
	}

	// Running Ensure again must stay a no-op.
	if err := schema.Ensure(ctx, admin, db, zap.NewNop()); err != nil {
		t.Fatalf("second schema.Ensure: %v", err)
	}
}

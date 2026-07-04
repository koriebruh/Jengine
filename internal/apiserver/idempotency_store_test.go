package apiserver_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/apiserver"
	"github.com/koriebruh/Jengine/internal/testutil"
)

func TestPostgresIdempotencyStore_SaveAndGet(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'Acme', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		t.Fatalf("seed tenant failed: %v", err)
	}

	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	defer appPool.Close()
	store := apiserver.NewPostgresIdempotencyStore(appPool)

	_, err := store.Get(ctx, tenantID, "missing-key")
	if err != apiserver.ErrIdempotencyKeyNotFound {
		t.Fatalf("expected ErrIdempotencyKeyNotFound for a missing key, got: %v", err)
	}

	saved := apiserver.StoredResponse{RequestHash: "hash-1", ResponseBody: []byte("cached-bytes")}
	if err := store.Save(ctx, tenantID, "key-1", saved); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	got, err := store.Get(ctx, tenantID, "key-1")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got.RequestHash != saved.RequestHash || string(got.ResponseBody) != string(saved.ResponseBody) {
		t.Errorf("expected %+v, got %+v", saved, got)
	}

	// A second tenant must not see the first tenant's key (RLS).
	otherTenantID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'Other', 'STANDARD', 'us-east', 'ACTIVE')`,
		otherTenantID,
	); err != nil {
		t.Fatalf("seed other tenant failed: %v", err)
	}
	_, err = store.Get(ctx, otherTenantID, "key-1")
	if err != apiserver.ErrIdempotencyKeyNotFound {
		t.Errorf("expected a different tenant to NOT see the first tenant's idempotency key (RLS), got: %v", err)
	}
}

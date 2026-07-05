package tenancy_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/koriebruh/Jengine/internal/tenancy"
)

const localSuperuserDSNForProvisioning = "postgres://jengine:jengine_dev@localhost:5432/jengine?sslmode=disable"

func requireMigrateCLI(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("migrate"); err == nil {
		return
	}
	if gopath, err := exec.Command("go", "env", "GOPATH").Output(); err == nil {
		candidate := filepath.Join(strings.TrimSpace(string(gopath)), "bin", "migrate")
		if _, err := os.Stat(candidate); err == nil {
			return
		}
	}
	t.Skip("migrate CLI not on PATH or at $(go env GOPATH)/bin/migrate (see scripts/migrate.sh's own install instructions)")
}

// TestProvisionIsolatedSchema_CreateAndTeardown is plans/task/core/24's
// DoD schema-provisioning test: create a tenant schema, run the full
// base migration set against it, confirm the tables actually exist
// there (not just "the command exited 0"), then tear it down and
// confirm it's gone.
func TestProvisionIsolatedSchema_CreateAndTeardown(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}
	requireMigrateCLI(t)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, localSuperuserDSNForProvisioning)
	if err != nil {
		t.Fatalf("connect to local dev Postgres: %v", err)
	}
	defer pool.Close()

	tenantID := uuid.New()
	schemaName := tenancy.SchemaNameFor(tenantID)
	t.Cleanup(func() {
		_ = tenancy.DeprovisionIsolatedSchema(context.Background(), pool, tenantID)
	})

	if err := tenancy.ProvisionIsolatedSchema(ctx, pool, localSuperuserDSNForProvisioning, tenantID, "../../migrations"); err != nil {
		t.Fatalf("ProvisionIsolatedSchema failed: %v", err)
	}

	var tableCount int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = 'transactions'`,
		schemaName,
	).Scan(&tableCount); err != nil {
		t.Fatalf("query schema tables failed: %v", err)
	}
	if tableCount != 1 {
		t.Errorf("expected the 'transactions' table to exist in schema %q after provisioning, found %d", schemaName, tableCount)
	}

	if err := tenancy.DeprovisionIsolatedSchema(ctx, pool, tenantID); err != nil {
		t.Fatalf("DeprovisionIsolatedSchema failed: %v", err)
	}

	var schemaCount int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM information_schema.schemata WHERE schema_name = $1`, schemaName,
	).Scan(&schemaCount); err != nil {
		t.Fatalf("query schema existence failed: %v", err)
	}
	if schemaCount != 0 {
		t.Errorf("expected schema %q to be gone after deprovisioning, it still exists", schemaName)
	}
}

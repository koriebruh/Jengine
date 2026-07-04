package audit_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/audit"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

func TestVerifyChain_CleanChainReportsNoBreak(t *testing.T) {
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

	w := audit.NewPostgresWriter()
	for i := 0; i < 3; i++ {
		err := postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID}), appPool, tenantID, func(ctx context.Context) error {
			return w.Write(ctx, audit.AuditEvent{
				TenantID: tenantID, ActorID: "user-1", ActorType: "USER",
				EventType: "test.event", EntityType: "Break", EntityID: "break-1",
			})
		})
		if err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
	}

	store := audit.NewPostgresStore(db.Pool)
	report, err := audit.VerifyChain(ctx, store, tenantID)
	if err != nil {
		t.Fatalf("VerifyChain failed: %v", err)
	}
	if report.EventsChecked != 3 {
		t.Errorf("expected 3 events checked, got %d", report.EventsChecked)
	}
	if report.FirstBreakAt != nil {
		t.Errorf("expected a clean chain (nil FirstBreakAt), got break at %q", *report.FirstBreakAt)
	}
}

func TestVerifyChain_DetectsTamperedMiddleEvent(t *testing.T) {
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

	w := audit.NewPostgresWriter()
	for i := 0; i < 4; i++ {
		err := postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID}), appPool, tenantID, func(ctx context.Context) error {
			return w.Write(ctx, audit.AuditEvent{
				TenantID: tenantID, ActorID: "user-1", ActorType: "USER",
				EventType: "test.event", EntityType: "Break", EntityID: "break-1",
			})
		})
		if err != nil {
			t.Fatalf("write %d failed: %v", i, err)
		}
	}

	var ids []string
	rows, err := db.Pool.Query(ctx, `SELECT id FROM audit_events WHERE tenant_id = $1 ORDER BY id`, tenantID)
	if err != nil {
		t.Fatalf("list ids failed: %v", err)
	}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan id failed: %v", err)
		}
		ids = append(ids, id)
	}
	rows.Close()
	if len(ids) != 4 {
		t.Fatalf("expected 4 events, got %d", len(ids))
	}

	// Tamper the SECOND event's entity_id directly via the superuser
	// connection - bypasses the app role's UPDATE restriction entirely,
	// simulating a real tamper attempt. Tampering a middle (not the
	// last) event is the meaningful, catchable case: the NEXT event's
	// hash_chain_prev was fixed at write time against the ORIGINAL
	// (pre-tamper) hash of this row, so it will mismatch the recomputed
	// (post-tamper) hash.
	tamperedID := ids[1]
	if _, err := db.Pool.Exec(ctx, `UPDATE audit_events SET entity_id = 'TAMPERED' WHERE id = $1`, tamperedID); err != nil {
		t.Fatalf("tamper update (as superuser) failed: %v", err)
	}

	store := audit.NewPostgresStore(db.Pool)
	report, err := audit.VerifyChain(ctx, store, tenantID)
	if err != nil {
		t.Fatalf("VerifyChain failed: %v", err)
	}
	if report.FirstBreakAt == nil {
		t.Fatal("expected VerifyChain to detect the tampered row, got a clean report")
	}
	if *report.FirstBreakAt != ids[2] {
		t.Errorf("expected the break to be reported at event %q (the one after the tampered row), got %q", ids[2], *report.FirstBreakAt)
	}
}

func TestAppRole_CannotUpdateOrDeleteAuditEvents(t *testing.T) {
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

	w := audit.NewPostgresWriter()
	var eventID string
	err := postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID}), appPool, tenantID, func(ctx context.Context) error {
		return w.Write(ctx, audit.AuditEvent{
			TenantID: tenantID, ActorID: "user-1", ActorType: "USER",
			EventType: "test.event", EntityType: "Break", EntityID: "break-1",
		})
	})
	if err != nil {
		t.Fatalf("seed write failed: %v", err)
	}
	if err := db.Pool.QueryRow(ctx, `SELECT id FROM audit_events WHERE tenant_id = $1`, tenantID).Scan(&eventID); err != nil {
		t.Fatalf("query event id failed: %v", err)
	}

	// The app role itself (not a superuser connection) must be REJECTED
	// BY POSTGRES for UPDATE/DELETE - a permission-denied error, not an
	// application-level check (plans/task/core/14 Common Pitfalls: this
	// is the actual guarantee the design relies on).
	_, err = appPool.Exec(ctx, `UPDATE audit_events SET entity_id = 'HACKED' WHERE id = $1`, eventID)
	if err == nil {
		t.Fatal("expected the app role's UPDATE on audit_events to be rejected by Postgres, but it succeeded")
	}

	_, err = appPool.Exec(ctx, `DELETE FROM audit_events WHERE id = $1`, eventID)
	if err == nil {
		t.Fatal("expected the app role's DELETE on audit_events to be rejected by Postgres, but it succeeded")
	}
}

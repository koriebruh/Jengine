package audit_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/audit"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

func TestPostgresWriter_WriteRequiresTransaction(t *testing.T) {
	w := audit.NewPostgresWriter()
	err := w.Write(context.Background(), audit.AuditEvent{TenantID: uuid.New()})
	if err == nil {
		t.Fatal("expected Write to fail without an ambient transaction in context")
	}
}

func TestPostgresWriter_WriteBuildsValidChain(t *testing.T) {
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
	writeEvent := func(eventType string) error {
		return postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID}), appPool, tenantID, func(ctx context.Context) error {
			return w.Write(ctx, audit.AuditEvent{
				TenantID: tenantID, ActorID: "user-1", ActorType: "USER",
				EventType: eventType, EntityType: "Break", EntityID: "break-1",
				BeforeState: json.RawMessage(`{"status":"OPEN"}`), AfterState: json.RawMessage(`{"status":"ASSIGNED"}`),
			})
		})
	}

	if err := writeEvent("break.opened"); err != nil {
		t.Fatalf("first write failed: %v", err)
	}
	if err := writeEvent("break.assigned"); err != nil {
		t.Fatalf("second write failed: %v", err)
	}

	rows, err := db.Pool.Query(ctx, `SELECT id, hash_chain_prev FROM audit_events WHERE tenant_id = $1 ORDER BY id`, tenantID)
	if err != nil {
		t.Fatalf("query audit_events failed: %v", err)
	}
	defer rows.Close()

	type row struct{ id, prev string }
	var got []row
	for rows.Next() {
		var r row
		if err := rows.Scan(&r.id, &r.prev); err != nil {
			t.Fatalf("scan failed: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 audit_events rows, got %d", len(got))
	}
	if got[0].prev != "" {
		t.Errorf("expected the first event's hash_chain_prev to be empty (chain start), got %q", got[0].prev)
	}
	if got[1].prev == "" {
		t.Error("expected the second event's hash_chain_prev to be non-empty (chained to the first)")
	}

	var tailEventID, tailHash string
	if err := db.Pool.QueryRow(ctx, `SELECT last_event_id, last_hash FROM audit_chain_tail WHERE tenant_id = $1`, tenantID).Scan(&tailEventID, &tailHash); err != nil {
		t.Fatalf("query audit_chain_tail failed: %v", err)
	}
	if tailEventID != got[1].id {
		t.Errorf("expected chain tail to point at the last event %q, got %q", got[1].id, tailEventID)
	}
}

func TestPostgresWriter_ConcurrentWritesProduceValidChain(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
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
	const n = 20
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID}), appPool, tenantID, func(ctx context.Context) error {
				return w.Write(ctx, audit.AuditEvent{
					TenantID: tenantID, ActorID: "user-1", ActorType: "USER",
					EventType: "concurrent.test", EntityType: "Break", EntityID: "break-1",
				})
			})
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent write %d failed: %v", i, err)
		}
	}

	var count int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM audit_events WHERE tenant_id = $1`, tenantID).Scan(&count); err != nil {
		t.Fatalf("count audit_events failed: %v", err)
	}
	if count != n {
		t.Fatalf("expected %d audit_events rows (no lost writes), got %d", n, count)
	}

	// No two rows may share the same hash_chain_prev - that would mean
	// two events both claimed the same chain position (a lost-update /
	// race condition the FOR-UPDATE-equivalent locking is meant to
	// prevent).
	rows, err := db.Pool.Query(ctx, `SELECT hash_chain_prev, count(*) FROM audit_events WHERE tenant_id = $1 GROUP BY hash_chain_prev HAVING count(*) > 1`, tenantID)
	if err != nil {
		t.Fatalf("duplicate hash_chain_prev query failed: %v", err)
	}
	defer rows.Close()
	if rows.Next() {
		t.Fatal("found two audit_events rows sharing the same hash_chain_prev - the chain is corrupted")
	}
}

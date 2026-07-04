// Command audit-verify runs plans/task/core/14's chain-verification job:
// walks every tenant's audit_events chain and reports the first tampered
// event, if any. Runnable on demand (for ad-hoc compliance checks) or on
// a schedule (cron/k8s CronJob) - this binary itself does not loop or
// schedule; invoke it however the deployment environment prefers. Pass
// -seed to instead seed a demo tenant with a few real audit events (a
// manual-verification harness per plans/task/core/14's own DoD: "run the
// verification job against the local dev stack's seeded data... manually
// tamper a row... confirm the next run detects it").
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/koriebruh/Jengine/internal/audit"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
)

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	seed := flag.Bool("seed", false, "seed a demo tenant with a few real audit events, then exit (manual-verification harness)")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	superuserDSN := envOrDefault("DATABASE_URL", "postgres://jengine:jengine_dev@localhost:5432/jengine?sslmode=disable")
	pool, err := pgxpool.New(ctx, superuserDSN)
	if err != nil {
		log.Fatalf("audit-verify: connect: %v", err)
	}
	defer pool.Close()

	if *seed {
		runSeed(ctx, pool)
		return
	}
	runVerify(ctx, pool)
}

func runSeed(ctx context.Context, superuserPool *pgxpool.Pool) {
	appDSN := envOrDefault("APP_DATABASE_URL", "postgres://jengine_app:jengine_app_dev@localhost:5432/jengine?sslmode=disable")
	appPool, err := pgxpool.New(ctx, appDSN)
	if err != nil {
		log.Fatalf("audit-verify: connect as jengine_app: %v", err)
	}
	defer appPool.Close()

	tenantID := uuid.New()
	if _, err := superuserPool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'audit-verify-demo', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		log.Fatalf("audit-verify: seed tenant: %v", err)
	}

	w := audit.NewPostgresWriter()
	for i := 0; i < 3; i++ {
		err := postgres.WithTx(tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID}), appPool, tenantID, func(ctx context.Context) error {
			return w.Write(ctx, audit.AuditEvent{
				TenantID: tenantID, ActorID: "audit-verify-demo", ActorType: "SYSTEM",
				EventType: "manual.verification", EntityType: "Test", EntityID: fmt.Sprintf("test-%d", i),
			})
		})
		if err != nil {
			log.Fatalf("audit-verify: write event %d: %v", i, err)
		}
	}
	log.Printf("audit-verify: seeded tenant=%s with 3 audit events - run without -seed to verify, then try:\n  UPDATE audit_events SET entity_id='TAMPERED' WHERE id = (SELECT id FROM audit_events WHERE tenant_id='%s' ORDER BY id LIMIT 1 OFFSET 1);\nas the superuser and run again to see it detected", tenantID, tenantID)
}

func runVerify(ctx context.Context, pool *pgxpool.Pool) {
	rows, err := pool.Query(ctx, `SELECT id FROM tenants`)
	if err != nil {
		log.Fatalf("audit-verify: list tenants: %v", err)
	}
	var tenantIDs []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			log.Fatalf("audit-verify: scan tenant id: %v", err)
		}
		tenantIDs = append(tenantIDs, id)
	}
	rows.Close()

	store := audit.NewPostgresStore(pool)
	exitCode := 0
	for _, tenantID := range tenantIDs {
		report, err := audit.VerifyChain(ctx, store, tenantID)
		if err != nil {
			log.Printf("audit-verify: tenant %s: verification error: %v", tenantID, err)
			exitCode = 1
			continue
		}
		if report.FirstBreakAt != nil {
			fmt.Printf("TAMPERED  tenant=%s events_checked=%d first_break_at=%s\n", tenantID, report.EventsChecked, *report.FirstBreakAt)
			exitCode = 1
			continue
		}
		fmt.Printf("clean     tenant=%s events_checked=%d\n", tenantID, report.EventsChecked)
	}
	os.Exit(exitCode)
}

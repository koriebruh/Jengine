package batch_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/matching/batch"
	"github.com/koriebruh/Jengine/internal/testutil"
)

func TestEnumeratePartitions_PairsAccountsWithinSameTenantAndDay(t *testing.T) {
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

	accountA, accountB, accountC := uuid.New(), uuid.New(), uuid.New()
	for _, id := range []uuid.UUID{accountA, accountB, accountC} {
		if _, err := db.Pool.Exec(ctx,
			`INSERT INTO accounts (id, tenant_id, external_account_ref, account_type, currency, name) VALUES ($1, $2, $3, 'BANK', 'USD', 'Account')`,
			id, tenantID, id.String(),
		); err != nil {
			t.Fatalf("seed account failed: %v", err)
		}
	}

	day1 := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	day2 := time.Date(2024, 6, 5, 0, 0, 0, 0, time.UTC)

	insertTx := func(accountID uuid.UUID, valueDate time.Time, status string) {
		t.Helper()
		if _, err := db.Pool.Exec(ctx,
			`INSERT INTO transactions (tenant_id, account_id, external_ref, amount, currency, base_amount, value_date, booking_date, side, source_mode, ingestion_idempotency_key, status)
			 VALUES ($1, $2, $3, 100.00, 'USD', 100.00, $4, $4, 'DEBIT', 'BATCH', $3, $5)`,
			tenantID, accountID, uuid.NewString(), valueDate, status,
		); err != nil {
			t.Fatalf("seed transaction failed: %v", err)
		}
	}

	// Day 1: accountA and accountB both have UNMATCHED transactions ->
	// should pair into 1 partition.
	insertTx(accountA, day1, "UNMATCHED")
	insertTx(accountB, day1, "UNMATCHED")
	// Day 2: only accountC has an UNMATCHED transaction -> no pair (needs
	// at least 2 accounts to form a partition).
	insertTx(accountC, day2, "UNMATCHED")
	// Day 1: accountA also has a MATCHED transaction - must not affect
	// pairing (only UNMATCHED counts).
	insertTx(accountA, day1, "MATCHED")

	partitions, err := batch.EnumeratePartitions(ctx, db.Pool, time.Time{})
	if err != nil {
		t.Fatalf("EnumeratePartitions failed: %v", err)
	}

	if len(partitions) != 1 {
		t.Fatalf("expected exactly 1 partition (accountA+accountB on day1), got %d: %+v", len(partitions), partitions)
	}
	p := partitions[0]
	if p.TenantID != tenantID {
		t.Errorf("unexpected tenant: %s", p.TenantID)
	}
	gotPair := map[uuid.UUID]bool{p.SourceAccountID: true, p.TargetAccountID: true}
	if !gotPair[accountA] || !gotPair[accountB] {
		t.Errorf("expected the pair to be (accountA, accountB), got (%s, %s)", p.SourceAccountID, p.TargetAccountID)
	}
	if !p.ValueDateBucket.Equal(day1) {
		t.Errorf("expected bucket %v, got %v", day1, p.ValueDateBucket)
	}
}

func TestEnumeratePartitions_WatermarkExcludesOldTransactions(t *testing.T) {
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
	accountA, accountB := uuid.New(), uuid.New()
	for _, id := range []uuid.UUID{accountA, accountB} {
		if _, err := db.Pool.Exec(ctx,
			`INSERT INTO accounts (id, tenant_id, external_account_ref, account_type, currency, name) VALUES ($1, $2, $3, 'BANK', 'USD', 'Account')`,
			id, tenantID, id.String(),
		); err != nil {
			t.Fatalf("seed account failed: %v", err)
		}
	}

	day := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	for _, id := range []uuid.UUID{accountA, accountB} {
		if _, err := db.Pool.Exec(ctx,
			`INSERT INTO transactions (tenant_id, account_id, external_ref, amount, currency, base_amount, value_date, booking_date, side, source_mode, ingestion_idempotency_key, status)
			 VALUES ($1, $2, $3, 100.00, 'USD', 100.00, $4, $4, 'DEBIT', 'BATCH', $3, 'UNMATCHED')`,
			tenantID, id, uuid.NewString(), day,
		); err != nil {
			t.Fatalf("seed transaction failed: %v", err)
		}
	}

	// A future watermark should exclude everything (transactions were
	// just inserted "now," before the watermark).
	future := time.Now().Add(1 * time.Hour)
	partitions, err := batch.EnumeratePartitions(ctx, db.Pool, future)
	if err != nil {
		t.Fatalf("EnumeratePartitions failed: %v", err)
	}
	if len(partitions) != 0 {
		t.Errorf("expected 0 partitions for a future watermark, got %d", len(partitions))
	}
}

func TestPartitionKey_LoadWindow(t *testing.T) {
	bucket := time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC)
	k := batch.PartitionKey{ValueDateBucket: bucket}

	start, end := k.LoadWindow(2)
	wantStart := time.Date(2024, 6, 13, 0, 0, 0, 0, time.UTC)
	wantEnd := time.Date(2024, 6, 18, 0, 0, 0, 0, time.UTC)
	if !start.Equal(wantStart) {
		t.Errorf("expected start %v, got %v", wantStart, start)
	}
	if !end.Equal(wantEnd) {
		t.Errorf("expected end %v, got %v", wantEnd, end)
	}

	start0, end0 := k.LoadWindow(0)
	if !start0.Equal(bucket) || !end0.Equal(bucket.AddDate(0, 0, 1)) {
		t.Errorf("expected zero-margin window to be exactly [bucket, bucket+1day), got [%v, %v)", start0, end0)
	}
}

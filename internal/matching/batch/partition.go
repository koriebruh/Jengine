package batch

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PartitionKey identifies one independent unit of matching work -
// matches never cross accounts or far-apart dates
// (plans/docs/04-matching-engine.md §5.2), so a day's volume splits into
// thousands of independent, fully parallel partitions.
type PartitionKey struct {
	TenantID        uuid.UUID
	SourceAccountID uuid.UUID
	TargetAccountID uuid.UUID
	ValueDateBucket time.Time // truncated to day
}

// EnumeratePartitions finds every (tenant, account pair, day) combination
// with UNMATCHED transactions updated since the last watermark.
//
// Cross-task gap found during this task (documented in QA_REPORT.md):
// plans/docs/04-matching-engine.md §5.1's rule scope
// (scope.source/target.account_group) implies an account-grouping
// taxonomy that was never given a schema column (plans/task/core/03) or
// domain type (plans/task/core/05) - "account pairing from each
// CompiledRule's scope" as this task's own spec describes isn't
// resolvable as written. This MVP implementation instead pairs every
// distinct account with UNMATCHED transactions against every other
// distinct account in the SAME tenant and day with UNMATCHED
// transactions - correct and bounded (a tenant's account count is not
// partition-scale volume), just not pre-filtered by a scope taxonomy
// that doesn't exist yet.
//
// Partitions are unordered account pairs (source/target are an arbitrary
// assignment within the pair, not a directional bank-vs-GL distinction -
// that distinction is exactly the account_group gap above) - avoids
// enumerating both (A,B) and (B,A) as separate partitions.
func EnumeratePartitions(ctx context.Context, pool *pgxpool.Pool, since time.Time) ([]PartitionKey, error) {
	rows, err := pool.Query(ctx, `
		SELECT DISTINCT tenant_id, account_id, date_trunc('day', value_date) AS bucket
		FROM transactions
		WHERE status = 'UNMATCHED' AND updated_at >= $1
	`, since)
	if err != nil {
		return nil, fmt.Errorf("batch: enumerate partitions: %w", err)
	}
	defer rows.Close()

	type tenantDayKey struct {
		tenantID uuid.UUID
		bucket   time.Time
	}
	grouped := make(map[tenantDayKey][]uuid.UUID)

	for rows.Next() {
		var tenantID, accountID uuid.UUID
		var bucket time.Time
		if err := rows.Scan(&tenantID, &accountID, &bucket); err != nil {
			return nil, fmt.Errorf("batch: enumerate partitions: scan: %w", err)
		}
		k := tenantDayKey{tenantID: tenantID, bucket: bucket}
		grouped[k] = append(grouped[k], accountID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("batch: enumerate partitions: %w", err)
	}

	var partitions []PartitionKey
	for k, accounts := range grouped {
		for i := 0; i < len(accounts); i++ {
			for j := i + 1; j < len(accounts); j++ {
				partitions = append(partitions, PartitionKey{
					TenantID:        k.tenantID,
					SourceAccountID: accounts[i],
					TargetAccountID: accounts[j],
					ValueDateBucket: k.bucket,
				})
			}
		}
	}
	return partitions, nil
}

// LoadWindow returns [start, end) date-times for querying a partition's
// transactions - widened beyond the partition's exact ValueDateBucket day
// by marginDays on each side, so a rule's date_window tolerance can still
// find cross-day matches near the bucket boundary (a strict single-day
// load would silently defeat any date_window tolerance wider than zero,
// since Match only ever sees what's loaded into its source/target
// slices - partition enumeration drives work-scheduling granularity, not
// the actual query range).
func (k PartitionKey) LoadWindow(marginDays int) (start, end time.Time) {
	start = k.ValueDateBucket.AddDate(0, 0, -marginDays)
	end = k.ValueDateBucket.AddDate(0, 0, 1+marginDays)
	return start, end
}

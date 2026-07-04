package batch

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

// EnsureRiverSchema applies River's own job-queue-backing-table
// migrations (plans/task/core/12 Scope: "River manages its own schema
// via its migration helper — invoke that, don't hand-roll the table").
// Idempotent - safe to call on every startup, matching golang-migrate's
// behavior elsewhere in this codebase (plans/task/core/03).
func EnsureRiverSchema(ctx context.Context, pool *pgxpool.Pool) error {
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	if err != nil {
		return fmt.Errorf("batch: new river migrator: %w", err)
	}

	_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
	if err != nil {
		return fmt.Errorf("batch: river migrate up: %w", err)
	}
	return nil
}

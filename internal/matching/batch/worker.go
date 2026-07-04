package batch

import (
	"context"
	"fmt"
	"runtime"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
)

// DefaultWorkerFactor sizes the bounded worker pool at
// runtime.GOMAXPROCS(0) * factor (plans/task/core/12 Implementation
// Notes: "factor configurable, default small e.g. 2-4") - never an
// unbounded pool.
const DefaultWorkerFactor = 4

// NewRiverClient builds a River client with worker registered and a
// bounded worker pool - running N processes of this client against the
// same Postgres-backed job queue is safe (River's job claiming is
// itself the concurrency-safety mechanism), which is what makes adding
// KEDA-style autoscaling later an infra change, not a redesign
// (plans/task/core/12 Goal).
//
// worker takes the river.Worker[PartitionJobArgs] interface, not the
// concrete *PartitionWorker type, so plans/task/core/16's observability
// wiring (cmd/matching-batch/main.go) can register an instrumented
// wrapper around the real PartitionWorker without this package needing
// to import internal/platform/observability itself.
func NewRiverClient(pool *pgxpool.Pool, worker river.Worker[PartitionJobArgs], factor int) (*river.Client[pgx.Tx], error) {
	if factor <= 0 {
		factor = DefaultWorkerFactor
	}
	maxWorkers := runtime.GOMAXPROCS(0) * factor

	workers := river.NewWorkers()
	if err := river.AddWorkerSafely(workers, worker); err != nil {
		return nil, fmt.Errorf("batch: register partition worker: %w", err)
	}

	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: maxWorkers},
		},
		Workers: workers,
	})
	if err != nil {
		return nil, fmt.Errorf("batch: new river client: %w", err)
	}
	return client, nil
}

// EnqueuePartitions inserts one PartitionJobArgs job per partition -
// River deduplicates/handles concurrent claims itself, so calling this
// from multiple processes (or re-enqueuing an already-queued partition)
// is safe.
func EnqueuePartitions(ctx context.Context, client *river.Client[pgx.Tx], partitions []PartitionKey) error {
	for _, p := range partitions {
		_, err := client.Insert(ctx, PartitionJobArgs(p), nil)
		if err != nil {
			return fmt.Errorf("batch: enqueue partition %+v: %w", p, err)
		}
	}
	return nil
}

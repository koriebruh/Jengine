package ingestion

import (
	"context"
	"log/slog"
	"time"
)

// Producer is the minimal interface OutboxRelay needs to publish one
// event - satisfied by a thin franz-go adapter
// (internal/ingestion/kafka.Producer), kept as a local interface so this
// package doesn't depend on a specific Kafka client library directly.
type Producer interface {
	Produce(ctx context.Context, topic, key string, payload []byte) error
}

// OutboxRelay is the "deliberately simplified MVP version of the outbox
// pattern" (plans/task/core/06 Implementation Notes): polls
// OutboxReader.ListUnsent, publishes each event via Producer, and marks
// it sent. Full Debezium-based CDC relay is plans/task/core/18/22 - this
// is a synchronous best-effort poller, not a production-grade CDC
// pipeline, and is scoped accordingly.
type OutboxRelay struct {
	Reader    OutboxReader
	Producer  Producer
	BatchSize int // default 100 if <= 0
}

// RunOnce sweeps up to BatchSize unsent events once, returning how many
// were successfully published and marked sent. A single event's publish
// failure does not stop the sweep - it stays unsent and is retried on
// the next call, consistent with plans/docs/15-end-to-end-flows.md
// §15.5's "one bad record never halts the pipeline."
func (r *OutboxRelay) RunOnce(ctx context.Context) (int, error) {
	batchSize := r.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}

	events, err := r.Reader.ListUnsent(ctx, batchSize)
	if err != nil {
		return 0, err
	}

	sent := 0
	for _, e := range events {
		if err := r.Producer.Produce(ctx, e.Topic, e.Key, e.Payload); err != nil {
			slog.ErrorContext(ctx, "outbox relay: publish failed", "event_id", e.ID, "topic", e.Topic, "error", err)
			continue
		}
		if err := r.Reader.MarkSent(ctx, e.ID); err != nil {
			slog.ErrorContext(ctx, "outbox relay: mark-sent failed", "event_id", e.ID, "error", err)
			continue
		}
		sent++
	}
	return sent, nil
}

// Run polls RunOnce every interval until ctx is cancelled.
func (r *OutboxRelay) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.RunOnce(ctx); err != nil {
				slog.ErrorContext(ctx, "outbox relay: sweep failed", "error", err)
			}
		}
	}
}

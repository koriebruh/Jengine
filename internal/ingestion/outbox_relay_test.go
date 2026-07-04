package ingestion_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/ingestion"
)

type fakeOutboxReader struct {
	events    []ingestion.OutboxEvent
	sentIDs   map[uuid.UUID]bool
	markCalls int
}

func (r *fakeOutboxReader) ListUnsent(ctx context.Context, limit int) ([]ingestion.OutboxEvent, error) {
	var out []ingestion.OutboxEvent
	for _, e := range r.events {
		if !r.sentIDs[e.ID] {
			out = append(out, e)
		}
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (r *fakeOutboxReader) MarkSent(ctx context.Context, id uuid.UUID) error {
	if r.sentIDs == nil {
		r.sentIDs = make(map[uuid.UUID]bool)
	}
	r.sentIDs[id] = true
	r.markCalls++
	return nil
}

type fakeProducer struct {
	published  []string // topics published to
	failTopics map[string]bool
}

func (p *fakeProducer) Produce(ctx context.Context, topic, key string, payload []byte) error {
	if p.failTopics[topic] {
		return errors.New("simulated publish failure")
	}
	p.published = append(p.published, topic)
	return nil
}

func TestOutboxRelay_RunOnce_PublishesAndMarksSent(t *testing.T) {
	reader := &fakeOutboxReader{events: []ingestion.OutboxEvent{
		{ID: uuid.New(), Topic: "ingestion.raw.t1", Key: "k1", Payload: []byte("a")},
		{ID: uuid.New(), Topic: "ingestion.raw.t1", Key: "k2", Payload: []byte("b")},
	}}
	producer := &fakeProducer{}
	relay := &ingestion.OutboxRelay{Reader: reader, Producer: producer}

	sent, err := relay.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}
	if sent != 2 {
		t.Fatalf("expected 2 sent, got %d", sent)
	}
	if reader.markCalls != 2 {
		t.Fatalf("expected MarkSent called twice, got %d", reader.markCalls)
	}
}

func TestOutboxRelay_RunOnce_OneFailurePublishDoesNotBlockOthers(t *testing.T) {
	okID := uuid.New()
	failID := uuid.New()
	reader := &fakeOutboxReader{events: []ingestion.OutboxEvent{
		{ID: failID, Topic: "bad-topic", Key: "k1", Payload: []byte("a")},
		{ID: okID, Topic: "good-topic", Key: "k2", Payload: []byte("b")},
	}}
	producer := &fakeProducer{failTopics: map[string]bool{"bad-topic": true}}
	relay := &ingestion.OutboxRelay{Reader: reader, Producer: producer}

	sent, err := relay.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce failed: %v", err)
	}
	if sent != 1 {
		t.Fatalf("expected 1 sent (the other failed), got %d", sent)
	}
	if reader.sentIDs[failID] {
		t.Error("expected the failed publish to NOT be marked sent, so it's retried next sweep")
	}
	if !reader.sentIDs[okID] {
		t.Error("expected the successful publish to be marked sent")
	}
}

func TestOutboxRelay_Run_StopsOnContextCancel(t *testing.T) {
	reader := &fakeOutboxReader{}
	producer := &fakeProducer{}
	relay := &ingestion.OutboxRelay{Reader: reader, Producer: producer}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		relay.Run(ctx, 10*time.Millisecond)
		close(done)
	}()

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("expected Run to return promptly after context cancellation")
	}
}

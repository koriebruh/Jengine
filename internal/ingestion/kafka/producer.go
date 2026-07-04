// Package kafka is a thin franz-go adapter satisfying
// internal/ingestion.Producer, kept as its own package so
// internal/ingestion itself doesn't depend on a specific Kafka client
// library - see plans/docs/00-overview-and-architecture.md §1.3 (franz-go
// named as a compatible client for the Redpanda message bus).
package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Producer publishes to a Kafka-API-compatible broker (Redpanda locally,
// plans/task/core/02).
type Producer struct {
	client *kgo.Client
}

func NewProducer(brokers []string) (*Producer, error) {
	// AllowAutoTopicCreation: franz-go doesn't auto-create topics by
	// default (unlike older Kafka clients) - fine for local
	// dev/MVP where topics per plans/docs/06-streaming-architecture.md
	// §7.1 aren't pre-provisioned yet; V1 (task 18/22) should provision
	// topics explicitly with the documented partition counts instead of
	// relying on broker defaults from auto-creation.
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.AllowAutoTopicCreation())
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}
	return &Producer{client: client}, nil
}

func (p *Producer) Produce(ctx context.Context, topic, key string, payload []byte) error {
	record := &kgo.Record{Topic: topic, Key: []byte(key), Value: payload}
	res := p.client.ProduceSync(ctx, record)
	return res.FirstErr()
}

func (p *Producer) Close() {
	p.client.Close()
}

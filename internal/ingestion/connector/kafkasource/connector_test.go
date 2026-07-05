package kafkasource_test

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/koriebruh/Jengine/internal/ingestion/connector"
	"github.com/koriebruh/Jengine/internal/ingestion/connector/kafkasource"
)

const localRedpandaBroker = "localhost:9092"

// requireLocalRedpanda mirrors internal/ingestion's own helper of the
// same name - each package keeps its own local copy per this repo's
// established convention (see e.g. connector/sftp.SecretResolver's doc
// comment for the same "duplicate rather than share" rationale).
func requireLocalRedpanda(t *testing.T) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", localRedpandaBroker, 2*time.Second)
	if err != nil {
		t.Skipf("local Redpanda not reachable at %s (run `make dev-up`): %v", localRedpandaBroker, err)
	}
	_ = conn.Close()
}

type noopSecrets struct{}

func (noopSecrets) Resolve(ctx context.Context, ref string) (string, error) { return "", nil }

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

func produce(t *testing.T, topic string, values [][]byte) {
	t.Helper()
	client, err := kgo.NewClient(kgo.SeedBrokers(localRedpandaBroker), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatalf("producer client: %v", err)
	}
	defer client.Close()
	for _, v := range values {
		res := client.ProduceSync(context.Background(), &kgo.Record{Topic: topic, Value: v})
		if err := res.FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
}

func TestFetch_ConsumesProducedRecords(t *testing.T) {
	requireLocalRedpanda(t)

	topic := "kafkasource-test-" + uuid.NewString()[:8]
	group := "kafkasource-test-group-" + uuid.NewString()[:8]
	want := [][]byte{[]byte(`{"tx":"1"}`), []byte(`{"tx":"2"}`), []byte(`{"tx":"3"}`)}
	produce(t, topic, want)

	c := kafkasource.New(noopSecrets{})
	tenantID, connectorID := uuid.New(), uuid.New()
	cfg := connector.ConnectorConfig{
		TenantID: tenantID, ConnectorID: connectorID, Type: "KAFKA_SOURCE",
		Settings: mustJSON(t, kafkasource.Config{
			BootstrapServers: []string{localRedpandaBroker},
			Topic:            topic,
			ConsumerGroup:    group,
			AuthMode:         kafkasource.AuthModePlaintext,
			SchemaFormat:     "json",
			SourceFormat:     "TENANT_KAFKA_JSON",
		}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	records, err := c.Fetch(ctx, cfg)
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}

	got := make(map[string]bool)
	timeout := time.After(15 * time.Second)
	for len(got) < len(want) {
		select {
		case rec, ok := <-records:
			if !ok {
				t.Fatalf("channel closed early, got %d/%d records", len(got), len(want))
			}
			if rec.TenantID != tenantID || rec.ConnectorID != connectorID {
				t.Errorf("unexpected record identity: %+v", rec)
			}
			if rec.SourceFormat != "TENANT_KAFKA_JSON" {
				t.Errorf("unexpected SourceFormat: %q", rec.SourceFormat)
			}
			got[string(rec.Payload)] = true
		case <-timeout:
			t.Fatalf("timed out waiting for records, got %d/%d", len(got), len(want))
		}
	}
	for _, w := range want {
		if !got[string(w)] {
			t.Errorf("expected to receive record %q", w)
		}
	}
}

func TestFetch_CommittedOffsetsNotRedeliveredToFreshConsumer(t *testing.T) {
	requireLocalRedpanda(t)

	topic := "kafkasource-test-" + uuid.NewString()[:8]
	group := "kafkasource-test-group-" + uuid.NewString()[:8]
	produce(t, topic, [][]byte{[]byte(`{"tx":"first-batch"}`)})

	cfg := func() connector.ConnectorConfig {
		return connector.ConnectorConfig{
			TenantID: uuid.New(), ConnectorID: uuid.New(), Type: "KAFKA_SOURCE",
			Settings: mustJSON(t, kafkasource.Config{
				BootstrapServers: []string{localRedpandaBroker},
				Topic:            topic,
				ConsumerGroup:    group,
				AuthMode:         kafkasource.AuthModePlaintext,
				SchemaFormat:     "json",
				SourceFormat:     "TENANT_KAFKA_JSON",
			}),
		}
	}

	// First consumer: reads and commits the one record.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 15*time.Second)
	c1 := kafkasource.New(noopSecrets{})
	records1, err := c1.Fetch(ctx1, cfg())
	if err != nil {
		t.Fatalf("Fetch (first) failed: %v", err)
	}
	select {
	case rec := <-records1:
		if string(rec.Payload) != `{"tx":"first-batch"}` {
			t.Fatalf("unexpected payload: %s", rec.Payload)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for first consumer's record")
	}
	// Give the background commit loop a moment to actually commit
	// before tearing this consumer down.
	time.Sleep(2 * time.Second)
	cancel1()

	// Second consumer, same consumer group: must NOT redeliver the
	// already-committed record - proves "Checkpoint via committed
	// consumer-group offsets" (plans/task/core/18) actually works.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel2()
	c2 := kafkasource.New(noopSecrets{})
	records2, err := c2.Fetch(ctx2, cfg())
	if err != nil {
		t.Fatalf("Fetch (second) failed: %v", err)
	}
	select {
	case rec, ok := <-records2:
		if ok {
			t.Fatalf("expected no redelivery of committed record, got %+v", rec)
		}
	case <-time.After(5 * time.Second):
		// No record arrived within the wait window - expected: nothing
		// new to consume, offsets already committed past it.
	}
}
